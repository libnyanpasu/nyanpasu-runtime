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

use std::sync::atomic::{AtomicU32, Ordering};

use crate::{
    error::CoreErrorKind,
    manager::ApplyOutcome,
    spec::{CoreSpec, InstanceOptions},
    state::RevisionId,
};

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
        for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
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
