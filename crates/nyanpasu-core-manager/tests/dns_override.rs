//! DNS override wiring: fixed-phase converge/restore, the persisted record,
//! orphan reconcile at construction, and the never-fail-the-transaction
//! policy — all against a fake controller.

mod common;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use nyanpasu_core_manager::{
    CoreState, ManagerOptions,
    dns::{DnsController, DnsError, DnsIntent, DnsOverrideRecord, DnsOverrideState},
    manager::{ApplyOutcome, CoreManager},
    runtime::BoxFuture,
};

#[derive(Default)]
struct FakeDns {
    applied: Mutex<Vec<(DnsIntent, u64)>>,
    restored: Mutex<Vec<DnsOverrideRecord>>,
    fail_apply: AtomicBool,
}

impl DnsController for FakeDns {
    fn desired(&self, _effective: &serde_yaml_ng::Mapping) -> Option<DnsIntent> {
        Some(DnsIntent {
            servers: vec!["198.18.0.2".to_owned()],
        })
    }

    fn apply<'a>(
        &'a self,
        intent: &'a DnsIntent,
        runtime_epoch: u64,
    ) -> BoxFuture<'a, Result<DnsOverrideRecord, DnsError>> {
        Box::pin(async move {
            if self.fail_apply.load(Ordering::SeqCst) {
                return Err(DnsError::Command("injected apply failure".into()));
            }
            self.applied
                .lock()
                .unwrap()
                .push((intent.clone(), runtime_epoch));
            Ok(DnsOverrideRecord {
                interface: "Wi-Fi".to_owned(),
                previous: vec!["10.0.0.1".to_owned()],
                applied: intent.servers.clone(),
                runtime_epoch,
                owner_generation: None,
                state: DnsOverrideState::Applied,
            })
        })
    }

    fn restore<'a>(&'a self, record: &'a DnsOverrideRecord) -> BoxFuture<'a, Result<(), DnsError>> {
        Box::pin(async move {
            self.restored.lock().unwrap().push(record.clone());
            Ok(())
        })
    }
}

async fn manager_with_dns(dir: &camino::Utf8Path, dns: Arc<FakeDns>) -> CoreManager {
    CoreManager::builder(ManagerOptions {
        runtime_dir: Some(dir.join("runtime")),
        ..ManagerOptions::default()
    })
    .dns_controller(dns)
    .build()
    .await
    .expect("construct manager")
}

fn record_path(dir: &camino::Utf8Path) -> camino::Utf8PathBuf {
    dir.join("runtime").join("dns-override.json")
}

#[tokio::test]
async fn reconcile_applies_the_override_and_stop_restores_it_first() {
    let (_guard, dir) = common::utf8_tempdir();
    let port = common::free_port();
    let config = common::write_config(&dir, &format!("external-controller: 127.0.0.1:{port}\n"));
    let spec = common::mihomo_spec(&dir, config);
    let dns = Arc::new(FakeDns::default());
    let manager = manager_with_dns(&dir, dns.clone()).await;

    let outcome = manager.reconcile(spec.clone(), None).await.unwrap();
    assert!(matches!(outcome, ApplyOutcome::Started { .. }));
    {
        let applied = dns.applied.lock().unwrap();
        assert_eq!(applied.len(), 1, "start tail applies exactly once");
        assert_eq!(applied[0].0.servers, vec!["198.18.0.2".to_owned()]);
        assert_eq!(applied[0].1, 1, "the record names the running epoch");
    }
    // The record survived to disk with the controller's read-back data.
    let record: DnsOverrideRecord =
        serde_json::from_slice(&std::fs::read(record_path(&dir)).unwrap()).unwrap();
    assert_eq!(record.state, DnsOverrideState::Applied);
    assert_eq!(record.previous, vec!["10.0.0.1".to_owned()]);

    manager.stop().await.unwrap();
    {
        let restored = dns.restored.lock().unwrap();
        assert_eq!(restored.len(), 1, "stop head restores exactly once");
        assert_eq!(restored[0].applied, vec!["198.18.0.2".to_owned()]);
    }
    assert!(
        !record_path(&dir).exists(),
        "a restored override leaves no record behind"
    );

    manager.shutdown().await.unwrap();
    assert_eq!(
        dns.restored.lock().unwrap().len(),
        1,
        "shutdown after a clean stop has nothing left to restore"
    );
}

#[tokio::test]
async fn a_noop_reconcile_reapplies_idempotently() {
    let (_guard, dir) = common::utf8_tempdir();
    let port = common::free_port();
    let config = common::write_config(&dir, &format!("external-controller: 127.0.0.1:{port}\n"));
    let spec = common::mihomo_spec(&dir, config);
    let dns = Arc::new(FakeDns::default());
    let manager = manager_with_dns(&dir, dns.clone()).await;

    manager.reconcile(spec.clone(), None).await.unwrap();
    let outcome = manager.reconcile(spec, None).await.unwrap();
    assert!(matches!(outcome, ApplyOutcome::Noop { .. }));
    // The converge tail runs each transaction; idempotency is the
    // controller's contract, observed here as a second harmless apply.
    assert_eq!(dns.applied.lock().unwrap().len(), 2);

    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_orphan_record_is_restored_at_construction() {
    let (_guard, dir) = common::utf8_tempdir();
    let runtime_dir = dir.join("runtime");
    std::fs::create_dir_all(&runtime_dir).unwrap();
    let orphan = DnsOverrideRecord {
        interface: "Wi-Fi".to_owned(),
        previous: vec!["10.0.0.1".to_owned()],
        applied: vec!["198.18.0.2".to_owned()],
        runtime_epoch: 7,
        owner_generation: None,
        state: DnsOverrideState::Applied,
    };
    std::fs::write(
        runtime_dir.join("dns-override.json"),
        serde_json::to_vec(&orphan).unwrap(),
    )
    .unwrap();

    let dns = Arc::new(FakeDns::default());
    let manager = manager_with_dns(&dir, dns.clone()).await;
    {
        let restored = dns.restored.lock().unwrap();
        assert_eq!(restored.len(), 1, "construction reconciles the orphan");
        assert_eq!(restored[0], orphan);
    }
    assert!(!record_path(&dir).exists());

    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_dns_apply_failure_never_fails_the_transaction() {
    let (_guard, dir) = common::utf8_tempdir();
    let port = common::free_port();
    let config = common::write_config(&dir, &format!("external-controller: 127.0.0.1:{port}\n"));
    let spec = common::mihomo_spec(&dir, config);
    let dns = Arc::new(FakeDns::default());
    dns.fail_apply.store(true, Ordering::SeqCst);
    let manager = manager_with_dns(&dir, dns.clone()).await;

    let outcome = manager.reconcile(spec, None).await.unwrap();
    assert!(
        matches!(outcome, ApplyOutcome::Started { .. }),
        "the core transaction succeeds regardless of DNS"
    );
    assert!(matches!(manager.status().state, CoreState::Running { .. }));
    // The pre-record is kept: the side effect is uncertain and a later
    // restore must still be able to undo it.
    let record: DnsOverrideRecord =
        serde_json::from_slice(&std::fs::read(record_path(&dir)).unwrap()).unwrap();
    assert!(
        record.previous.is_empty(),
        "a pre-record has no read-back yet"
    );

    manager.shutdown().await.unwrap();
    assert_eq!(
        dns.restored.lock().unwrap().len(),
        1,
        "shutdown restores the uncertain pre-record"
    );
    assert!(!record_path(&dir).exists());
}
