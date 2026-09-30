//! Service composition adapter; the service owns collection while desktops are absent.
use nyanpasu_core_manager::{Host, InstanceLifecycleEvent, InstanceLifecycleSink};
use nyanpasu_traffic::*;
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;

pub async fn start(
    directory: PathBuf,
    cancellation: CancellationToken,
) -> TrafficResult<TrafficClient> {
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    }
    #[cfg(windows)]
    nyanpasu_utils::io::atomic_fs::harden_windows_directory_acl(&directory)
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let store =
        adapters::redb::RedbTrafficStore::open(directory.join("traffic.redb"), 32 * 1024 * 1024)
            .await?;
    TrafficClient::start(actor::TrafficActorArgs {
        host: HostId("service".into()),
        source: Arc::new(adapters::clash::ClashTrafficSource::default()),
        store: Arc::new(store),
        clock: Arc::new(SystemClock::default()),
        cancellation,
    })
    .await
}

pub struct LifecycleSink(pub TrafficClient);
impl InstanceLifecycleSink for LifecycleSink {
    fn publish(&self, event: InstanceLifecycleEvent) {
        let result = match event {
            InstanceLifecycleEvent::Started {
                instance_id,
                observed_at_ms,
                controller,
                ..
            } => {
                let time =
                    UInt(u64::try_from(observed_at_ms).expect("wall clock predates Unix epoch"));
                let instance_id = instance_id.to_string();
                let endpoint = match controller.host {
                    Host::Http(url) => SourceEndpoint::Http(url.to_string()),
                    Host::UnixSocket(path) => SourceEndpoint::UnixSocket(path),
                    Host::NamedPipe(path) => SourceEndpoint::NamedPipe(path),
                    _ => {
                        tracing::error!("unsupported traffic controller transport");
                        return;
                    }
                };
                self.0.notify_instance_started(
                    NewSession {
                        host: HostId("service".into()),
                        instance_id: instance_id.clone(),
                        process_started_at: None,
                        attached_at: time,
                        late_attach: false,
                    },
                    Some(SourceBinding {
                        instance_id,
                        endpoint,
                        secret: controller.secret,
                    }),
                )
            }
            InstanceLifecycleEvent::Exited {
                instance_id,
                observed_at_ms,
                ..
            } => self
                .0
                .notify_instance_exited(instance_id.to_string(), observed_at_ms),
        };
        if let Err(error) = result {
            tracing::error!(%error, "traffic lifecycle delivery unavailable");
        }
    }
}

/// The configuration watch only supplies the latest available context. It is
/// not a complete configuration timeline and never determines session identity.
pub async fn context_bridge(
    mut subscription: nyanpasu_core_manager::ConfigCommitSubscription,
    traffic: TrafficClient,
    cancellation: CancellationToken,
) {
    let mut initial = subscription.latest();
    loop {
        let snapshot = if let Some(snapshot) = initial.take() {
            snapshot
        } else {
            tokio::select! { _ = cancellation.cancelled() => break, snapshot = subscription.changed() => match snapshot {Some(snapshot)=>snapshot,None=>break} }
        };
        let raw_rules: Vec<String> = snapshot
            .config
            .get("rules")
            .and_then(|v| v.as_sequence())
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str())
            .map(str::to_owned)
            .collect();
        let rules = accounting::config_context_rules(&raw_rules);
        if let Err(error) = traffic
            .update_context(
                snapshot.instance_id.to_string(),
                Some(ConfigContext {
                    revision: snapshot.revision.effective_hash,
                    rules,
                }),
            )
            .await
        {
            tracing::warn!(%error, "traffic configuration context unavailable");
        }
    }
}

/// Selection is a latest-state slice, distinct from the ordered process events.
pub async fn selection_bridge(
    mut status: tokio::sync::watch::Receiver<nyanpasu_core_manager::CoreStatus>,
    traffic: TrafficClient,
    cancellation: CancellationToken,
) {
    loop {
        let id = status
            .borrow_and_update()
            .instance_id
            .map(|id| id.to_string());
        if let Err(error) = traffic.notify_current_instance(id) {
            tracing::warn!(%error,"traffic instance selection unavailable");
        }
        tokio::select! { _=cancellation.cancelled()=>break, result=status.changed()=>if result.is_err(){break} }
    }
}

#[cfg(test)]
mod smoke_tests {
    use super::*;
    use nyanpasu_core_manager::{CoreKind, CoreManager, CoreSpec, InstanceSpec, ManagerOptions};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    #[ignore = "requires NYANPASU_TRAFFIC_TEST_MIHOMO pointing to a real mihomo binary"]
    async fn real_mihomo_records_closed_connections_without_a_desktop() {
        run(false).await;
    }
    #[tokio::test]
    #[ignore = "requires NYANPASU_TRAFFIC_TEST_MIHOMO pointing to a real mihomo binary"]
    async fn root_cancel_seals_exact_instances_before_reopening_traffic() {
        run(true).await;
    }
    async fn run(root_cancel: bool) {
        let binary = std::env::var("NYANPASU_TRAFFIC_TEST_MIHOMO").expect("set test mihomo binary");
        let root = tempfile::tempdir().unwrap();
        let utf8 = |p: PathBuf| camino::Utf8PathBuf::from_path_buf(p).unwrap();
        let token = CancellationToken::new();
        let traffic = start(root.path().join("traffic"), token.clone())
            .await
            .unwrap();
        let manager = CoreManager::builder(ManagerOptions {
            runtime_dir: Some(utf8(root.path().join("runtime"))),
            cancel_token: token.clone(),
            ..ManagerOptions::default()
        })
        .lifecycle_sink(Arc::new(LifecycleSink(traffic.clone())))
        .build()
        .await
        .unwrap();
        let select = tokio::spawn(selection_bridge(
            manager.subscribe(),
            traffic.clone(),
            token.clone(),
        ));
        let controller = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let controller_port = controller.local_addr().unwrap().port();
        drop(controller);
        let proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_port = proxy.local_addr().unwrap().port();
        drop(proxy);
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let (release_sender, release) = tokio::sync::oneshot::channel();
        let origin_task = tokio::spawn(async move {
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut request = vec![0; 4096];
            socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            socket.write_all(&vec![b'x'; 1024]).await.unwrap();
            release.await.unwrap();
            socket.write_all(&vec![b'x'; 3072]).await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let config = root.path().join("config.yaml");
        tokio::fs::write(&config,format!("mixed-port: {proxy_port}\nexternal-controller: 127.0.0.1:{controller_port}\nsecret: smoke-secret\nallow-lan: false\nmode: rule\nlog-level: silent\nrules:\n  - MATCH,DIRECT\n")).await.unwrap();
        manager
            .start(InstanceSpec {
                core: CoreSpec {
                    kind: CoreKind::Mihomo,
                    binary_path: utf8(binary.into()),
                    version: None,
                    features: vec![],
                },
                config_path: utf8(config),
                working_dir: utf8(root.path().to_owned()),
                pid_file: None,
                options: Default::default(),
            })
            .await
            .unwrap();
        let mut summaries = traffic.subscribe_summary();
        let mut client = tokio::net::TcpStream::connect(("127.0.0.1", proxy_port))
            .await
            .unwrap();
        client.write_all(format!("GET http://{origin_address}/traffic-smoke HTTP/1.1\r\nHost: {origin_address}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        let session = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(summary) = summaries.borrow_and_update().clone() {
                    if summary.active_connections.0 > 0
                        && summary.session.attributed_bytes.download.0 > 0
                    {
                        break summary.session;
                    }
                }
                summaries.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        release_sender.send(()).unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        drop(client);
        origin_task.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if summaries
                    .borrow_and_update()
                    .as_ref()
                    .is_some_and(|s| s.active_connections.0 == 0)
                {
                    break;
                }
                summaries.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let page = traffic
            .query_connections(ConnectionsQuery {
                session_id: session.id.clone(),
                filter: ConnectionFilter::default(),
                limit: 100,
                cursor: None,
            })
            .await
            .unwrap();
        assert!(!page.connections.is_empty());
        assert!(page.connections.iter().all(|c| matches!(
            c.status,
            ConnectionStatus::Closed {
                final_counters_exact: false,
                ..
            }
        )));
        assert!(
            page.connections
                .iter()
                .any(|c| c.sample.chains.iter().any(|chain| chain == "DIRECT"))
        );
        let count = page.meta.session.attributed_bytes.clone();
        assert!(count.download.0 >= 1024);
        if root_cancel {
            token.cancel();
            manager.shutdown().await.unwrap();
            select.await.unwrap();
            traffic.shutdown().await.unwrap();
            drop(summaries);
            drop(traffic);
            drop(manager);
            let reopened = start(root.path().join("traffic"), CancellationToken::new())
                .await
                .unwrap();
            let ended = reopened.current_session().await.unwrap().unwrap();
            assert_eq!(ended.id, session.id);
            assert!(ended.ended_at.is_some());
            assert_eq!(ended.attributed_bytes, count);
            reopened.shutdown().await.unwrap();
            drop(reopened);
            return;
        }
        manager.stop().await.unwrap();
        let ended = traffic.session(session.id).await.unwrap();
        assert!(ended.ended_at.is_some());
        assert_eq!(ended.attributed_bytes, count);
        manager.shutdown().await.unwrap();
        token.cancel();
        select.await.unwrap();
        traffic.shutdown().await.unwrap();
        drop(summaries);
        drop(traffic);
        drop(manager);
    }
}
