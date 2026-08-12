//! DNS override convergence at the fixed phases of the v2 transaction entry
//! points (`reconcile` / `stop` / `shutdown` / `recover_quarantine`).
//!
//! The legacy v1 methods (`start` / `apply_config` / `switch` / `restart`)
//! deliberately do *not* converge DNS: under v1 the app process owns DNS
//! behavior, and that stays untouched until the bridge-cleanup stage removes
//! v1 entirely.
//!
//! Failure policy: a DNS failure never fails the core lifecycle transaction.
//! It is logged, the record is kept (so a later restore can still undo it),
//! and the caller's outcome is unchanged. Read-back lives inside the
//! controller; an `Err` here never implies the side effect is absent.

use crate::{
    dns::{DnsController, DnsOverrideRecord, DnsOverrideState},
    runtime_store::RuntimeConfigStore,
};

use super::{CoreManager, Ctrl};

const RECORD_FILE: &str = "dns-override.json";

/// Startup orphan reconcile: a record left behind by a dead process is
/// restored (a no-op when its side effect never landed) and cleared. Runs
/// before the manager exists, so it takes the store directly.
pub(super) async fn reconcile_orphan_record(
    store: &RuntimeConfigStore,
    dns: Option<&dyn DnsController>,
) {
    let path = store.dir().join(RECORD_FILE);
    let Ok(bytes) = tokio::fs::read(&path).await else {
        return;
    };
    let record = match serde_json::from_slice::<DnsOverrideRecord>(&bytes) {
        Ok(record) => record,
        Err(error) => {
            tracing::warn!("unreadable orphan dns override record; keeping the file: {error}");
            return;
        }
    };
    let Some(dns) = dns else {
        tracing::warn!(
            "an orphan dns override record exists but no dns controller is injected; keeping it"
        );
        return;
    };
    match dns.restore(&record).await {
        Ok(()) => {
            if let Err(error) = tokio::fs::remove_file(&path).await {
                tracing::warn!("failed to clear the restored dns override record: {error}");
            }
        }
        Err(error) => {
            tracing::warn!("orphan dns override restore failed; keeping the record: {error}");
        }
    }
}

impl CoreManager {
    /// Converge tail: apply the override when a runtime is up and wants one,
    /// restore otherwise. Idempotent; called with the control lock held.
    pub(super) async fn dns_converge(&self, ctrl: &mut Ctrl) {
        let Some(dns) = self.inner.dns.clone() else {
            return;
        };
        let desired = ctrl.current.as_ref().and_then(|active| {
            let running = !active.instance.state().borrow().state.is_terminal();
            running
                .then(|| {
                    dns.desired(&active.effective_document)
                        .map(|intent| (intent, active.instance.epoch()))
                })
                .flatten()
        });
        let Some((intent, epoch)) = desired else {
            self.dns_restore(ctrl).await;
            return;
        };
        // Persist a pre-record before the side effect: a crash between the
        // two leaves an orphan whose restore is a read-back no-op.
        let pre_record = DnsOverrideRecord {
            interface: String::new(),
            previous: Vec::new(),
            applied: intent.servers.clone(),
            runtime_epoch: epoch,
            owner_generation: None,
            state: DnsOverrideState::Applied,
        };
        self.persist_dns_record(ctrl, Some(pre_record)).await;
        match dns.apply(&intent, epoch).await {
            Ok(record) => self.persist_dns_record(ctrl, Some(record)).await,
            Err(error) => {
                // Keep the pre-record: the side effect is uncertain, and a
                // later restore must still be able to undo it.
                tracing::warn!("dns override apply failed; keeping the pre-record: {error}");
            }
        }
    }

    /// Restore head/tail: undo the recorded override and clear the record.
    /// Called at the head of stop/shutdown (so resolution never points at a
    /// core being torn down) and from the converge tail when nothing runs.
    pub(super) async fn dns_restore(&self, ctrl: &mut Ctrl) {
        let Some(dns) = self.inner.dns.clone() else {
            return;
        };
        let Some(record) = ctrl.dns_record.clone() else {
            return;
        };
        let mut pending = record.clone();
        pending.state = DnsOverrideState::RestorePending;
        self.persist_dns_record(ctrl, Some(pending)).await;
        match dns.restore(&record).await {
            Ok(()) => self.persist_dns_record(ctrl, None).await,
            Err(error) => {
                tracing::warn!("dns override restore failed; keeping the record: {error}");
            }
        }
    }

    /// Record-before-side-effect persistence. Failures are logged, never
    /// propagated: losing the record loses crash recovery, not correctness of
    /// the running transaction.
    async fn persist_dns_record(&self, ctrl: &mut Ctrl, record: Option<DnsOverrideRecord>) {
        let path = self.inner.store.dir().join(RECORD_FILE);
        let result = match &record {
            Some(entry) => match serde_json::to_vec_pretty(entry) {
                Ok(bytes) => tokio::fs::write(&path, bytes).await,
                Err(error) => {
                    tracing::warn!("failed to encode the dns override record: {error}");
                    ctrl.dns_record = record;
                    return;
                }
            },
            None => match tokio::fs::remove_file(&path).await {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
                _ => Ok(()),
            },
        };
        if let Err(error) = result {
            tracing::warn!("failed to persist the dns override record: {error}");
        }
        ctrl.dns_record = record;
    }
}
