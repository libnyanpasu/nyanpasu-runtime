//! Ordered notifications from the actual process supervisor, independent of status watches.

use crate::Epoch;

#[derive(Clone)]
pub enum InstanceLifecycleEvent {
    Started {
        instance_id: uuid::Uuid,
        epoch: Epoch,
        pid: u32,
        observed_at_ms: i64,
    },
    Exited {
        instance_id: uuid::Uuid,
        epoch: Epoch,
        observed_at_ms: i64,
    },
}

/// The host forwards these notifications to its domain owner without calling back
/// into the manager. A status watch cannot replace
/// this port: it can coalesce an entire short-lived process.
pub trait InstanceLifecycleSink: Send + Sync + 'static {
    fn publish(&self, event: InstanceLifecycleEvent);
}
