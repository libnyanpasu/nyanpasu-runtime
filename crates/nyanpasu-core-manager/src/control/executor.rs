//! The control executor: one task that owns every mutating operation from
//! admission to a safe terminal state, isolated from caller cancellation
//! (design §10). Callers hold [`super::OperationHandle`]s; dropping one never
//! cancels the work it names.

use std::collections::{HashMap, VecDeque};

use camino::Utf8PathBuf;
use tokio::sync::{mpsc, watch};

use crate::{
    error::{CoreErrorKind, Error},
    manager::{ApplyOutcome, CoreManager},
    spec::InstanceSpec,
};

use super::{
    ConfigInput, CoreCommand, CoreError, OperationId, OperationOutput, OperationState,
    ReconcileRequest, payload_digest,
};

pub(super) struct ExecutorWork {
    pub(super) id: OperationId,
    pub(super) command: CoreCommand,
}

/// Bounded most-recent-operations registry: the idempotency table and the
/// query surface for lost-response recovery. Losing an entry never affects the
/// runtime — a client that misses falls back to reading `CoreStatus`, and the
/// revision CAS blocks double application (design §9.4).
pub(super) struct Registry {
    inner: parking_lot::Mutex<RegistryInner>,
    capacity: usize,
}

struct RegistryInner {
    operations: HashMap<OperationId, RegisteredOperation>,
    order: VecDeque<OperationId>,
}

struct RegisteredOperation {
    digest: String,
    state_tx: watch::Sender<OperationState>,
}

pub(super) enum Admission {
    /// New registration; the caller must enqueue the work.
    Registered(watch::Receiver<OperationState>),
    /// Same id + same digest: the original operation, wherever it is.
    Existing(watch::Receiver<OperationState>),
}

impl Registry {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            inner: parking_lot::Mutex::new(RegistryInner {
                operations: HashMap::new(),
                order: VecDeque::new(),
            }),
            capacity,
        }
    }

    pub(super) fn admit(&self, id: OperationId, digest: &str) -> Result<Admission, CoreError> {
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.operations.get(&id) {
            if existing.digest == digest {
                return Ok(Admission::Existing(existing.state_tx.subscribe()));
            }
            return Err(CoreError::new(
                CoreErrorKind::OperationConflict,
                format!("operation {id} was already submitted with a different payload"),
                false,
            )
            .with_operation(id));
        }
        // Evict the oldest *terminal* entries only. In-flight operations are
        // never evicted; their count is already bounded by the work queue.
        while inner.operations.len() >= self.capacity {
            let Some(position) = inner.order.iter().position(|id| {
                inner
                    .operations
                    .get(id)
                    .is_some_and(|operation| is_terminal(&operation.state_tx.borrow()))
            }) else {
                break;
            };
            let evicted = inner.order.remove(position).expect("position is in range");
            inner.operations.remove(&evicted);
        }
        let (state_tx, state_rx) = watch::channel(OperationState::Queued);
        inner.operations.insert(
            id,
            RegisteredOperation {
                digest: digest.to_owned(),
                state_tx,
            },
        );
        inner.order.push_back(id);
        Ok(Admission::Registered(state_rx))
    }

    /// Rolls back a registration whose enqueue failed.
    pub(super) fn remove(&self, id: OperationId) {
        let mut inner = self.inner.lock();
        inner.operations.remove(&id);
        inner.order.retain(|entry| *entry != id);
    }

    pub(super) fn set(&self, id: OperationId, state: OperationState) {
        let inner = self.inner.lock();
        if let Some(operation) = inner.operations.get(&id) {
            let _ = operation.state_tx.send(state);
        }
    }

    pub(super) fn get(&self, id: OperationId) -> Option<OperationState> {
        let inner = self.inner.lock();
        inner
            .operations
            .get(&id)
            .map(|operation| operation.state_tx.borrow().clone())
    }
}

fn is_terminal(state: &OperationState) -> bool {
    matches!(
        state,
        OperationState::Succeeded(_) | OperationState::Failed(_)
    )
}

pub(super) struct ExecutorContext {
    pub(super) manager: CoreManager,
    pub(super) registry: std::sync::Arc<Registry>,
    pub(super) source_dir: Utf8PathBuf,
    pub(super) working_dir: Utf8PathBuf,
}

pub(super) async fn run(mut rx: mpsc::Receiver<ExecutorWork>, context: ExecutorContext) {
    while let Some(work) = rx.recv().await {
        context.registry.set(work.id, OperationState::Running);
        let is_shutdown = matches!(work.command, CoreCommand::Shutdown);
        let state = match execute(&context, work.id, work.command).await {
            Ok(output) => OperationState::Succeeded(output),
            Err(error) => OperationState::Failed(error),
        };
        context.registry.set(work.id, state);
        if is_shutdown {
            break;
        }
    }
    // Everything still queued was admitted before the shutdown drained the
    // loop. A queued Shutdown *was* accomplished by the one that ran; anything
    // else was not.
    rx.close();
    while let Ok(work) = rx.try_recv() {
        let state = match work.command {
            CoreCommand::Shutdown => OperationState::Succeeded(OperationOutput::ShutDown),
            _ => OperationState::Failed(
                CoreError::new(
                    CoreErrorKind::ShuttingDown,
                    "the control plane shut down before this queued operation ran",
                    false,
                )
                .with_operation(work.id),
            ),
        };
        context.registry.set(work.id, state);
    }
}

async fn execute(
    context: &ExecutorContext,
    id: OperationId,
    command: CoreCommand,
) -> Result<OperationOutput, CoreError> {
    let domain = |error: &Error| CoreError::from_domain(error, Some(id));
    match command {
        CoreCommand::Reconcile(request) => reconcile(context, id, *request)
            .await
            .map(OperationOutput::Reconciled),
        CoreCommand::Stop => context
            .manager
            .stop()
            .await
            .map(|()| OperationOutput::Stopped)
            .map_err(|error| domain(&error)),
        CoreCommand::Recover => context
            .manager
            .recover_quarantine()
            .await
            .map(|()| OperationOutput::Recovered)
            .map_err(|error| domain(&error)),
        CoreCommand::Shutdown => context
            .manager
            .shutdown()
            .await
            .map(|()| OperationOutput::ShutDown)
            .map_err(|error| domain(&error)),
    }
}

async fn reconcile(
    context: &ExecutorContext,
    id: OperationId,
    request: ReconcileRequest,
) -> Result<ApplyOutcome, CoreError> {
    let ReconcileRequest {
        core,
        config,
        options,
        expected_applied,
    } = request;
    let config_path = materialize(
        &context.source_dir,
        // One fixed name: mutating operations are serialized, and after the
        // transaction the effective config lives in the runtime store, so the
        // source never accumulates.
        "reconcile-source.yaml",
        config,
        id,
    )
    .await?;
    let spec = InstanceSpec {
        core,
        config_path,
        working_dir: context.working_dir.clone(),
        pid_file: None,
        options,
    };
    context
        .manager
        .reconcile(spec, expected_applied)
        .await
        .map_err(|error| CoreError::from_domain(&error, Some(id)))
}

/// Writes portable config bytes to a host file for the path-based pipeline
/// below the control plane. The digest is verified before anything else
/// happens — a mismatch is a clean abort.
pub(super) async fn materialize(
    source_dir: &camino::Utf8Path,
    file_name: &str,
    config: ConfigInput,
    id: OperationId,
) -> Result<Utf8PathBuf, CoreError> {
    let ConfigInput::Inline {
        bytes,
        expected_digest,
    } = config;
    if let Some(declared) = expected_digest {
        let computed = payload_digest(&bytes);
        if declared != computed {
            return Err(CoreError::new(
                CoreErrorKind::InvalidConfig,
                format!("config digest mismatch: declared {declared}, computed {computed}"),
                false,
            )
            .with_operation(id));
        }
    }
    let io = |error: std::io::Error| CoreError::from_domain(&Error::Io(error), Some(id));
    tokio::fs::create_dir_all(source_dir).await.map_err(io)?;
    let path = source_dir.join(file_name);
    tokio::fs::write(&path, &bytes).await.map_err(io)?;
    Ok(path)
}
