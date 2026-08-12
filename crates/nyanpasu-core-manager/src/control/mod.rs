//! Portable control-plane command surface: operation identity, commands, and
//! operation results.
//!
//! Design: `docs/design/2026-08-08-core-manager-control-plane-runtime-backend-design.md`
//! (§9, §16, §25, with the 2026-08-12 amendments). Two deliberate deviations
//! from that draft, decided at implementation time:
//!
//! - Check is not a [`CoreCommand`] variant. Amendment A2 degrades check to an
//!   advisory, read-only call that never enters the mutating queue, so it is a
//!   separate control method rather than an impossible registry state.
//! - [`CoreError::kind`] is `Option`. The R0 protocol rule is that naming a
//!   kind is a statement of fact about the failure and a guessed kind is worse
//!   than an absent one; unclassified failures stay unclassified.

mod executor;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

use camino::Utf8PathBuf;
use tokio::sync::{Semaphore, broadcast, mpsc, watch};

use crate::{
    error::CoreErrorKind,
    log::LogFrame,
    manager::{ApplyOutcome, CoreManager},
    spec::{CoreSpec, InstanceOptions, InstanceSpec},
    state::{CoreStatus, RevisionId},
};

use executor::{Admission, ExecutorContext, ExecutorWork, Registry};

/// Correlation, idempotency, and event-tracing identity of one control
/// request. Not a lease, a session, or a lock: it never grants ownership and
/// never blocks another operation (design §9.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationId([u8; 16]);

impl OperationId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Convenience generator for local callers that do not carry their own id.
    ///
    /// Uniqueness comes from (unix nanos, pid, process-local counter). This is
    /// an identity for the bounded operation registry, not a cryptographic
    /// token; remote callers supply their own ids.
    pub fn generate() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&nanos.to_be_bytes());
        bytes[8..12].copy_from_slice(&std::process::id().to_be_bytes());
        bytes[12..16].copy_from_slice(&COUNTER.fetch_add(1, Ordering::Relaxed).to_be_bytes());
        Self(bytes)
    }
}

impl std::fmt::Display for OperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseOperationIdError;

impl std::fmt::Display for ParseOperationIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an operation id is exactly 32 lowercase hex characters")
    }
}

impl std::error::Error for ParseOperationIdError {}

impl std::str::FromStr for OperationId {
    type Err = ParseOperationIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32 || !value.is_ascii() {
            return Err(ParseOperationIdError);
        }
        let mut bytes = [0u8; 16];
        for (index, chunk) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let chunk = std::str::from_utf8(chunk).map_err(|_| ParseOperationIdError)?;
            bytes[index] = u8::from_str_radix(chunk, 16).map_err(|_| ParseOperationIdError)?;
        }
        Ok(Self(bytes))
    }
}

/// Change-identity digest over a raw payload: stable FNV-1a, hex-encoded. The
/// idempotency registry compares digests, never full payloads; this is not a
/// cryptographic integrity primitive.
pub fn payload_digest(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Portable configuration payload. The control plane never reads caller
/// filesystem paths; a host that has a path performs read → digest → `Inline`
/// at its own boundary (design §16.2). A `Resource` variant is deferred until
/// a config store consumer exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigInput {
    Inline {
        bytes: Vec<u8>,
        /// [`payload_digest`] of `bytes` as the caller computed it, verified
        /// on receipt when present.
        expected_digest: Option<String>,
    },
}

impl ConfigInput {
    pub fn inline(bytes: Vec<u8>) -> Self {
        Self::Inline {
            bytes,
            expected_digest: None,
        }
    }
}

/// The single mutating convergence command (amendment A2): make the runtime
/// match this desired core + config. Start, restart, apply, and switch are
/// classifications the orchestrator derives internally, not caller choices.
#[derive(Debug, Clone)]
pub struct ReconcileRequest {
    pub core: CoreSpec,
    pub config: ConfigInput,
    pub options: InstanceOptions,
    /// Compare-and-swap token: the revision the caller believes is applied.
    /// `None` skips the comparison (unconditional reconcile).
    pub expected_applied: Option<RevisionId>,
}

/// Advisory, read-only validation (amendment A2): concurrency-limited, never
/// queued, and never a precondition for any change.
#[derive(Debug, Clone)]
pub struct CheckRequest {
    pub core: CoreSpec,
    pub config: ConfigInput,
}

/// A mutating control command. Serialized by the control executor; every
/// variant runs as one transaction to a safe terminal state.
#[derive(Debug, Clone)]
pub enum CoreCommand {
    Reconcile(Box<ReconcileRequest>),
    Stop,
    Recover,
    Shutdown,
}

impl CoreCommand {
    /// The identity digest the idempotency registry compares: two envelopes
    /// with the same [`OperationId`] must describe the same work. Config
    /// bytes, the desired core, and the CAS token are identity; tuning fields
    /// ([`InstanceOptions`]) deliberately are not.
    pub fn payload_digest(&self) -> String {
        use std::fmt::Write;
        match self {
            Self::Reconcile(request) => {
                let mut identity = format!(
                    "reconcile\0{}\0{}\0{}\0",
                    request.core.kind,
                    request.core.binary_path,
                    request.core.version.as_deref().unwrap_or(""),
                );
                for feature in &request.core.features {
                    identity.push_str(feature);
                    identity.push('\0');
                }
                if let Some(expected) = &request.expected_applied {
                    let _ = write!(identity, "{expected}");
                }
                identity.push('\0');
                let mut payload = identity.into_bytes();
                let ConfigInput::Inline { bytes, .. } = &request.config;
                payload.extend_from_slice(bytes);
                payload_digest(&payload)
            }
            Self::Stop => payload_digest(b"stop"),
            Self::Recover => payload_digest(b"recover"),
            Self::Shutdown => payload_digest(b"shutdown"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoreCommandEnvelope {
    pub operation_id: OperationId,
    pub command: CoreCommand,
}

/// The successful terminal payload of one operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationOutput {
    Reconciled(ApplyOutcome),
    Stopped,
    Recovered,
    ShutDown,
}

/// Portable, cloneable error surface of the control plane (design §25). The
/// domain [`Error`](crate::Error) is converted at the executor boundary so the
/// registry can replay the same terminal result to idempotent re-submits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreError {
    /// See the module doc: `None` means unclassified, never "no error".
    pub kind: Option<CoreErrorKind>,
    /// Human-readable and unstable; never a branch condition.
    pub message: String,
    /// Whether resubmitting the same envelope can plausibly succeed. Set by
    /// the producer: the same kind can be retryable in one situation and not
    /// another (an `OperationConflict` from a handoff in progress is; one from
    /// an id reused with a different payload is not).
    pub retryable: bool,
    pub operation_id: Option<OperationId>,
}

impl CoreError {
    pub fn new(kind: CoreErrorKind, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            kind: Some(kind),
            message: message.into(),
            retryable,
            operation_id: None,
        }
    }

    pub fn from_domain(error: &crate::Error, operation_id: Option<OperationId>) -> Self {
        let kind = error.kind();
        Self {
            kind,
            message: error.to_string(),
            retryable: kind.is_some_and(default_retryable),
            operation_id,
        }
    }

    pub fn with_operation(mut self, operation_id: OperationId) -> Self {
        self.operation_id = Some(operation_id);
        self
    }
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            Some(kind) => write!(f, "{kind}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for CoreError {}

/// Kinds that are retryable regardless of the situation that produced them.
/// Everything else defaults to non-retryable and the producer overrides where
/// it knows better.
fn default_retryable(kind: CoreErrorKind) -> bool {
    matches!(
        kind,
        CoreErrorKind::QueueFull | CoreErrorKind::BackendUnavailable
    )
}

/// Registry-visible lifecycle of one operation (design §9.4).
#[derive(Debug, Clone)]
pub enum OperationState {
    Queued,
    Running,
    Succeeded(OperationOutput),
    Failed(CoreError),
}

/// Host-boundary configuration of one control plane.
#[derive(Debug, Clone)]
pub struct ControlOptions {
    /// Where portable config bytes are materialized for the path-based
    /// pipeline underneath. Host-owned; never a caller path.
    pub source_dir: Utf8PathBuf,
    /// Working directory for launched cores (data dir with geo assets).
    pub working_dir: Utf8PathBuf,
    /// Mutating-operation queue bound; a full queue answers `QueueFull`.
    pub queue_capacity: usize,
    /// Most-recent-operations registry bound (idempotency + query window).
    pub registry_capacity: usize,
    /// Concurrent advisory checks; further callers wait on the semaphore.
    pub check_concurrency: usize,
}

impl ControlOptions {
    pub fn new(source_dir: Utf8PathBuf, working_dir: Utf8PathBuf) -> Self {
        Self {
            source_dir,
            working_dir,
            queue_capacity: 16,
            registry_capacity: 64,
            check_concurrency: 2,
        }
    }
}

/// How the executor task ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorExit {
    /// A `Shutdown` operation completed and the executor drained.
    Clean,
    /// The task died without shutting down (panic). The host must treat the
    /// whole control plane as fatally broken.
    Died,
}

/// The portable control plane over one [`CoreManager`] (design §9): submit /
/// status / subscribe on the outside, one executor task owning every mutating
/// transaction on the inside. Cheap to clone; all clones share the executor.
#[derive(Clone)]
pub struct CoreControl {
    manager: CoreManager,
    registry: Arc<Registry>,
    work_tx: mpsc::Sender<ExecutorWork>,
    closing: Arc<AtomicBool>,
    check_semaphore: Arc<Semaphore>,
    source_dir: Utf8PathBuf,
    working_dir: Utf8PathBuf,
    done_rx: watch::Receiver<bool>,
}

impl CoreControl {
    /// Wraps a host-built manager and spawns the executor task. The manager
    /// handle stays usable directly, but every mutating call should go through
    /// `submit` from here on — the executor owns transaction serialization.
    pub fn spawn(manager: CoreManager, options: ControlOptions) -> Self {
        let ControlOptions {
            source_dir,
            working_dir,
            queue_capacity,
            registry_capacity,
            check_concurrency,
        } = options;
        let registry = Arc::new(Registry::new(registry_capacity));
        let (work_tx, work_rx) = mpsc::channel(queue_capacity);
        let (done_tx, done_rx) = watch::channel(false);
        let context = ExecutorContext {
            manager: manager.clone(),
            registry: registry.clone(),
            source_dir: source_dir.clone(),
            working_dir: working_dir.clone(),
        };
        tokio::spawn(async move {
            executor::run(work_rx, context).await;
            let _ = done_tx.send(true);
        });
        Self {
            manager,
            registry,
            work_tx,
            closing: Arc::new(AtomicBool::new(false)),
            check_semaphore: Arc::new(Semaphore::new(check_concurrency)),
            source_dir,
            working_dir,
            done_rx,
        }
    }

    /// Admission is synchronous: closing latch, idempotency, then the bounded
    /// queue. The returned handle names the operation; dropping it never
    /// cancels the work.
    pub fn submit(&self, envelope: CoreCommandEnvelope) -> Result<OperationHandle, CoreError> {
        let id = envelope.operation_id;
        if self.closing.load(Ordering::Acquire) {
            return Err(CoreError::new(
                CoreErrorKind::ShuttingDown,
                "the control plane is shutting down",
                false,
            )
            .with_operation(id));
        }
        let digest = envelope.command.payload_digest();
        let state_rx = match self.registry.admit(id, &digest)? {
            Admission::Existing(state_rx) => state_rx,
            Admission::Registered(state_rx) => {
                let is_shutdown = matches!(envelope.command, CoreCommand::Shutdown);
                match self.work_tx.try_send(ExecutorWork {
                    id,
                    command: envelope.command,
                }) {
                    Ok(()) => {
                        if is_shutdown {
                            self.closing.store(true, Ordering::Release);
                        }
                        state_rx
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.registry.remove(id);
                        return Err(CoreError::new(
                            CoreErrorKind::QueueFull,
                            "the operation queue is full",
                            true,
                        )
                        .with_operation(id));
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        self.registry.remove(id);
                        return Err(CoreError::new(
                            CoreErrorKind::Internal,
                            "the control executor is gone",
                            false,
                        )
                        .with_operation(id));
                    }
                }
            }
        };
        Ok(OperationHandle { id, state_rx })
    }

    /// Zero-mailbox snapshot read.
    pub fn status(&self) -> CoreStatus {
        self.manager.status()
    }

    pub fn subscribe(&self) -> watch::Receiver<CoreStatus> {
        self.manager.subscribe()
    }

    pub fn subscribe_logs(&self) -> broadcast::Receiver<Arc<LogFrame>> {
        self.manager.subscribe_logs()
    }

    /// The registry's answer for one operation id, or `None` when the entry
    /// was evicted or never existed. Losing an entry is recoverable by
    /// design: re-read status, and let the revision CAS block a double apply.
    pub fn operation(&self, id: OperationId) -> Option<OperationState> {
        self.registry.get(id)
    }

    /// Advisory, read-only config validation (amendment A2): bounded
    /// concurrency, never queued, and never a precondition for any change.
    pub async fn check(&self, request: CheckRequest) -> Result<(), CoreError> {
        let _permit = self.check_semaphore.acquire().await.map_err(|_| {
            CoreError::new(
                CoreErrorKind::Internal,
                "the check semaphore is closed",
                false,
            )
        })?;
        let ConfigInput::Inline { bytes, .. } = &request.config;
        let file_name = format!("check-source-{}.yaml", payload_digest(bytes));
        let id = OperationId::generate();
        let config_path =
            executor::materialize(&self.source_dir, &file_name, request.config, id).await?;
        let spec = InstanceSpec {
            core: request.core,
            config_path: config_path.clone(),
            working_dir: self.working_dir.clone(),
            pid_file: None,
            options: InstanceOptions::default(),
        };
        let result = self.manager.check_config(&spec).await;
        // Best-effort cleanup; a concurrent same-digest check rewrites the
        // same bytes, so a failed removal is harmless.
        let _ = tokio::fs::remove_file(&config_path).await;
        result.map_err(|error| CoreError::from_domain(&error, None))
    }

    /// Resolves when the executor task has exited: cleanly after a `Shutdown`
    /// operation, or [`ExecutorExit::Died`] when it panicked. The host must
    /// watch this and turn `Died` into a fatal service state.
    pub async fn until_closed(&self) -> ExecutorExit {
        let mut done_rx = self.done_rx.clone();
        loop {
            if *done_rx.borrow_and_update() {
                return ExecutorExit::Clean;
            }
            if done_rx.changed().await.is_err() {
                return ExecutorExit::Died;
            }
        }
    }
}

/// A submitted operation. `wait` consumes the handle and resolves with the
/// terminal result; `state` polls without consuming. Dropping the handle
/// leaves the operation running to its safe terminal state.
#[derive(Debug)]
pub struct OperationHandle {
    id: OperationId,
    state_rx: watch::Receiver<OperationState>,
}

impl OperationHandle {
    pub fn id(&self) -> OperationId {
        self.id
    }

    pub fn state(&self) -> OperationState {
        self.state_rx.borrow().clone()
    }

    pub async fn wait(mut self) -> Result<OperationOutput, CoreError> {
        loop {
            match self.state_rx.borrow_and_update().clone() {
                OperationState::Succeeded(output) => return Ok(output),
                OperationState::Failed(error) => return Err(error),
                OperationState::Queued | OperationState::Running => {}
            }
            if self.state_rx.changed().await.is_err() {
                return Err(CoreError::new(
                    CoreErrorKind::Internal,
                    "the control executor terminated before the operation completed",
                    false,
                )
                .with_operation(self.id));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn operation_ids_roundtrip_through_hex() {
        let id = OperationId::from_bytes([
            0x00, 0x01, 0x0a, 0x10, 0x7f, 0x80, 0xff, 0x42, 0x00, 0x01, 0x0a, 0x10, 0x7f, 0x80,
            0xff, 0x42,
        ]);
        let text = id.to_string();
        assert_eq!(text.len(), 32);
        assert_eq!(OperationId::from_str(&text).unwrap(), id);
    }

    #[test]
    fn malformed_operation_ids_are_rejected() {
        for bad in ["", "abc", &"a".repeat(31), &"g".repeat(32), &"a".repeat(33)] {
            assert_eq!(OperationId::from_str(bad), Err(ParseOperationIdError));
        }
    }

    #[test]
    fn generated_operation_ids_differ() {
        assert_ne!(OperationId::generate(), OperationId::generate());
    }

    #[test]
    fn the_payload_digest_is_stable_and_content_sensitive() {
        assert_eq!(payload_digest(b"abc"), payload_digest(b"abc"));
        assert_ne!(payload_digest(b"abc"), payload_digest(b"abd"));
        // Pinned so a silent algorithm change cannot slip past the idempotency
        // registry's stored digests.
        assert_eq!(payload_digest(b""), "cbf29ce484222325");
    }

    #[test]
    fn domain_errors_convert_with_their_kind_and_default_retryability() {
        let error = CoreError::from_domain(&crate::Error::AlreadyRunning, None);
        assert_eq!(error.kind, Some(CoreErrorKind::AlreadyRunning));
        assert!(!error.retryable);

        let unclassified = CoreError::from_domain(
            &crate::Error::Io(std::io::Error::other("boom")),
            Some(OperationId::from_bytes([7; 16])),
        );
        assert_eq!(unclassified.kind, None);
        assert!(!unclassified.retryable);
        assert_eq!(
            unclassified.operation_id,
            Some(OperationId::from_bytes([7; 16]))
        );
    }

    #[test]
    fn admission_kinds_default_to_retryable() {
        assert!(CoreError::new(CoreErrorKind::QueueFull, "full", true).retryable);
        assert!(default_retryable(CoreErrorKind::QueueFull));
        assert!(default_retryable(CoreErrorKind::BackendUnavailable));
        assert!(!default_retryable(CoreErrorKind::OperationConflict));
        assert!(!default_retryable(CoreErrorKind::ShuttingDown));
    }
}
