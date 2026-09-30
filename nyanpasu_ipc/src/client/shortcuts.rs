use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, StreamExt};
use nyanpasu_utils::reqwest_ext::NamedPipeRequestExt;
use reqwest_websocket::Message;

use crate::api::{
    self,
    contract::{
        CoreCheck, CoreStart, CoreStop, CoreV2Operation, CoreV2Status, CoreV2Submit, LogsInspect,
        LogsRetrieve, NetworkSetDns, Status,
    },
    core::v2::{
        CORE_V2_OPERATION_ENDPOINT, CORE_V2_STATUS_ENDPOINT, CORE_V2_SUBMIT_ENDPOINT, OperationInfo,
    },
    log::{LOGS_INSPECT_ENDPOINT, LOGS_RETRIEVE_ENDPOINT},
    status::STATUS_ENDPOINT,
    ws::events::{EVENT_URI, Event},
};

use super::{ClientError, Result};

pub use super::Client;

impl Client {
    pub async fn log_files(
        &self,
    ) -> Result<nyanpasu_logging::LogResult<Vec<nyanpasu_logging::LogFileInfo>>> {
        self.call::<api::contract::LogFiles>(None)
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: api::log::LOG_FILES_ENDPOINT,
            })
    }
    pub async fn open_logs(
        &self,
        request: &api::log::OwnedLogRequest<nyanpasu_logging::OpenLogs>,
    ) -> Result<nyanpasu_logging::LogResult<nyanpasu_logging::LogSession>> {
        self.call::<api::contract::LogOpen>(Some(request))
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: api::log::LOG_OPEN_ENDPOINT,
            })
    }
    pub async fn query_logs(
        &self,
        request: &api::log::OwnedLogRequest<nyanpasu_logging::QueryLogs>,
    ) -> Result<nyanpasu_logging::LogResult<nyanpasu_logging::LogPage>> {
        self.call::<api::contract::LogQuery>(Some(request))
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: api::log::LOG_QUERY_ENDPOINT,
            })
    }
    pub async fn close_logs(
        &self,
        request: &api::log::OwnedLogRequest<String>,
    ) -> Result<nyanpasu_logging::LogResult<()>> {
        self.call::<api::contract::LogClose>(Some(request))
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: api::log::LOG_CLOSE_ENDPOINT,
            })
    }
    pub async fn status(&self) -> Result<api::status::StatusResBody<'static>> {
        self.call::<Status>(None)
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: STATUS_ENDPOINT,
            })
    }

    pub async fn start_core(&self, payload: &api::core::start::CoreStartReq<'_>) -> Result<()> {
        self.call::<CoreStart>(Some(payload)).await.map(|_| ())
    }

    pub async fn stop_core(&self) -> Result<()> {
        self.call::<CoreStop>(None).await.map(|_| ())
    }

    /// Dry-run a config against a core binary without touching the running one.
    pub async fn check_config(&self, payload: &api::core::check::CoreCheckReq<'_>) -> Result<()> {
        self.call::<CoreCheck>(Some(payload)).await.map(|_| ())
    }

    /// Submit one v2 control-plane operation. The reply is the operation's
    /// admission-time snapshot; poll [`Self::core_operation`] for the result.
    pub async fn submit_core(
        &self,
        payload: &api::core::v2::CoreSubmitReq<'_>,
    ) -> Result<OperationInfo> {
        self.call::<CoreV2Submit>(Some(payload))
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: CORE_V2_SUBMIT_ENDPOINT,
            })
    }

    /// Query one v2 operation, optionally long-polling for its terminal state.
    pub async fn core_operation(
        &self,
        payload: &api::core::v2::CoreOperationReq<'_>,
    ) -> Result<OperationInfo> {
        self.call::<CoreV2Operation>(Some(payload))
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: CORE_V2_OPERATION_ENDPOINT,
            })
    }

    /// Read the applied process's API credentials over the protected IPC socket.
    pub async fn core_api_connection(&self) -> Result<Option<api::core::v2::CoreApiConnection>> {
        Ok(self
            .call::<api::contract::CoreV2ApiConnection>(None)
            .await?
            .data
            .flatten())
    }

    /// The daemon's canonical core status projection.
    pub async fn core_status_v2(&self) -> Result<api::status::CoreInfos> {
        self.call::<CoreV2Status>(None)
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: CORE_V2_STATUS_ENDPOINT,
            })
    }

    pub async fn inspect_logs(&self) -> Result<api::log::LogsResBody<'static>> {
        self.call::<LogsInspect>(None)
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: LOGS_INSPECT_ENDPOINT,
            })
    }

    pub async fn retrieve_logs(&self) -> Result<api::log::LogsResBody<'static>> {
        self.call::<LogsRetrieve>(None)
            .await?
            .data
            .ok_or(ClientError::EmptyData {
                operation: LOGS_RETRIEVE_ENDPOINT,
            })
    }

    pub async fn set_dns(
        &self,
        payload: &api::network::set_dns::NetworkSetDnsReq<'_>,
    ) -> Result<()> {
        self.call::<NetworkSetDns>(Some(payload)).await.map(|_| ())
    }

    /// Subscribe to the events pushed by the service over `/ws/events`.
    ///
    /// Snapshot first: the service pushes one [`Event::CoreStatusChanged`] the
    /// moment the socket opens, one after every dropped-event recovery, and one
    /// per manager transition — including the `Starting`/`Restarting`
    /// transitions the two-valued [`Event::CoreStateChanged`] cannot express.
    /// There is nothing to negotiate and no version parameter to pass; the
    /// service ignores the query string.
    ///
    /// [`Event::CoreStateChanged`] keeps arriving alongside the snapshots, so a
    /// consumer of both sees each transition twice. The snapshot is idempotent,
    /// so the simplest correct handling is to let the last frame win.
    pub async fn events(&self) -> Result<EventStream> {
        let response = self
            .get(EVENT_URI)
            .upgrade_with_named_pipe_retry()
            .await
            .map_err(|source| ClientError::WebSocket {
                operation: EVENT_URI,
                source,
            })?;
        let websocket =
            response
                .into_websocket()
                .await
                .map_err(|source| ClientError::WebSocket {
                    operation: EVENT_URI,
                    source,
                })?;
        let stream = websocket.filter_map(|message| async move {
            let bytes = match message {
                Ok(Message::Binary(bytes)) => bytes,
                Ok(Message::Text(text)) => text.into(),
                // pings are answered internally, everything else is not an event
                Ok(_) => return None,
                Err(source) => {
                    return Some(Err(ClientError::WebSocket {
                        operation: EVENT_URI,
                        source,
                    }));
                }
            };
            Some(
                serde_json::from_slice(&bytes).map_err(|source| ClientError::Decode {
                    operation: EVENT_URI,
                    source,
                }),
            )
        });
        Ok(EventStream {
            inner: Box::pin(stream),
        })
    }
}

/// A stream of [`Event`]s pushed by the service.
pub struct EventStream {
    inner: Pin<Box<dyn Stream<Item = Result<Event>> + Send>>,
}

impl Stream for EventStream {
    type Item = Result<Event>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl std::fmt::Debug for EventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStream").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod api_connection_tests {
    use super::*;

    #[tokio::test]
    async fn applied_binding_and_unavailable_binding_decode_through_the_contract() {
        use api::{RBuilder, core::v2::CoreApiConnection, status::CoreControllerInfo};
        use axum::{Json, Router, routing::get};
        for connection in [
            None,
            Some(CoreApiConnection {
                instance_id: "process-id".into(),
                controller: CoreControllerInfo::Http("http://127.0.0.1:9090/".into()),
                secret: Some("controller-secret".into()),
            }),
        ] {
            let expected = connection.clone();
            let router = Router::new().route(
                api::core::v2::CORE_V2_API_CONNECTION_ENDPOINT,
                get(move || {
                    let connection = connection.clone();
                    async move { Json(RBuilder::success(connection)) }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            let client = Client {
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
                base_url: format!("http://{address}/").parse().unwrap(),
            };
            assert_eq!(client.core_api_connection().await.unwrap(), expected);
            server.abort();
        }
    }
}

impl Client {
    pub async fn ensure_traffic_supported(&self) -> Result<()> {
        if tokio::time::timeout(std::time::Duration::from_secs(30), self.status())
            .await
            .map_err(|_| ClientError::TrafficDeadline)??
            .traffic_query_version
            == Some(api::traffic::TRAFFIC_QUERY_VERSION)
        {
            Ok(())
        } else {
            Err(ClientError::UnsupportedTraffic)
        }
    }
    pub async fn traffic_session(
        &self,
        request: &nyanpasu_traffic::SessionId,
    ) -> Result<nyanpasu_traffic::TrafficResult<nyanpasu_traffic::SessionRecord>> {
        self.ensure_traffic_supported().await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.call::<api::contract::TrafficSession>(Some(request)),
        )
        .await
        .map_err(|_| ClientError::TrafficDeadline)??
        .data
        .ok_or(ClientError::EmptyData {
            operation: <api::contract::TrafficSession as api::contract::IpcOperation>::PATH,
        })
    }
    pub async fn query_traffic_connections(
        &self,
        request: &nyanpasu_traffic::ConnectionsQuery,
    ) -> Result<nyanpasu_traffic::TrafficResult<nyanpasu_traffic::ConnectionPage>> {
        self.ensure_traffic_supported().await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.call::<api::contract::TrafficConnections>(Some(request)),
        )
        .await
        .map_err(|_| ClientError::TrafficDeadline)??
        .data
        .ok_or(ClientError::EmptyData {
            operation: <api::contract::TrafficConnections as api::contract::IpcOperation>::PATH,
        })
    }
    pub async fn query_traffic_usage(
        &self,
        request: &nyanpasu_traffic::UsageQuery,
    ) -> Result<nyanpasu_traffic::TrafficResult<nyanpasu_traffic::UsageResult>> {
        self.ensure_traffic_supported().await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.call::<api::contract::TrafficUsage>(Some(request)),
        )
        .await
        .map_err(|_| ClientError::TrafficDeadline)??
        .data
        .ok_or(ClientError::EmptyData {
            operation: <api::contract::TrafficUsage as api::contract::IpcOperation>::PATH,
        })
    }
    pub async fn query_traffic_topology(
        &self,
        request: &nyanpasu_traffic::TopologyQuery,
    ) -> Result<nyanpasu_traffic::TrafficResult<nyanpasu_traffic::TopologyResult>> {
        self.ensure_traffic_supported().await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.call::<api::contract::TrafficTopology>(Some(request)),
        )
        .await
        .map_err(|_| ClientError::TrafficDeadline)??
        .data
        .ok_or(ClientError::EmptyData {
            operation: <api::contract::TrafficTopology as api::contract::IpcOperation>::PATH,
        })
    }
}

impl Client {
    pub async fn subscribe_traffic_summary(
        &self,
    ) -> Result<TrafficStream<nyanpasu_traffic::TrafficSummary>> {
        self.traffic_stream(api::traffic::TRAFFIC_SUMMARY_ENDPOINT)
            .await
    }
    pub async fn subscribe_traffic_details(
        &self,
    ) -> Result<TrafficStream<nyanpasu_traffic::TrafficDetails>> {
        self.traffic_stream(api::traffic::TRAFFIC_DETAILS_ENDPOINT)
            .await
    }
    async fn traffic_stream<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        endpoint: &'static str,
    ) -> Result<TrafficStream<T>> {
        self.ensure_traffic_supported().await?;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.get(endpoint).upgrade_with_named_pipe_retry(),
        )
        .await
        .map_err(|_| ClientError::TrafficDeadline)?
        .map_err(|source| ClientError::WebSocket {
            operation: endpoint,
            source,
        })?;
        let websocket =
            response
                .into_websocket()
                .await
                .map_err(|source| ClientError::WebSocket {
                    operation: endpoint,
                    source,
                })?;
        let stream = websocket.filter_map(move |message| async move {
            let bytes = match message {
                Ok(Message::Binary(bytes)) => bytes,
                Ok(Message::Text(text)) => text.into(),
                Ok(_) => return None,
                Err(source) => {
                    return Some(Err(ClientError::WebSocket {
                        operation: endpoint,
                        source,
                    }));
                }
            };
            Some(
                serde_json::from_slice(&bytes).map_err(|source| ClientError::Decode {
                    operation: endpoint,
                    source,
                }),
            )
        });
        Ok(TrafficStream {
            inner: Box::pin(stream),
        })
    }
}
/// Latest domain observations; this stream is not the durable accounting log.
pub struct TrafficStream<T> {
    inner: Pin<Box<dyn Stream<Item = Result<Option<T>>> + Send>>,
}
impl<T> Stream for TrafficStream<T> {
    type Item = Result<Option<T>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl Client {
    pub async fn current_traffic_session(
        &self,
    ) -> Result<nyanpasu_traffic::TrafficResult<Option<nyanpasu_traffic::SessionRecord>>> {
        self.ensure_traffic_supported().await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.call::<api::contract::TrafficCurrentSession>(None),
        )
        .await
        .map_err(|_| ClientError::TrafficDeadline)??
        .data
        .ok_or(ClientError::EmptyData {
            operation: api::traffic::TRAFFIC_CURRENT_SESSION_ENDPOINT,
        })
    }
    pub async fn traffic_status(&self) -> Result<nyanpasu_traffic::TrafficResult<()>> {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.call::<api::contract::TrafficStatus>(None),
        )
        .await
        .map_err(|_| ClientError::TrafficDeadline)??
        .data
        .ok_or(ClientError::EmptyData {
            operation: api::traffic::TRAFFIC_STATUS_ENDPOINT,
        })
    }
}

#[cfg(all(test, feature = "server"))]
mod traffic_tests {
    use super::*;
    #[tokio::test]
    async fn old_service_is_unsupported_without_requesting_a_collector() {
        use axum::{Json, Router, routing::get};
        let payload = serde_json::json!({"code":"Ok","msg":"ok","ts":1,"data":{
          "version":"old-service","core_infos":{"type":null,"state":{"Stopped":null},"state_changed_at":0,"config_path":null},
          "runtime_infos":{"service_data_dir":"data","service_config_dir":"config","nyanpasu_config_dir":"config","nyanpasu_data_dir":"data"}
        }});
        let router = Router::new().route(
            "/status",
            get(move || {
                let payload = payload.clone();
                async move { Json(payload) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let client = Client {
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            base_url: format!("http://{address}/").parse().unwrap(),
        };
        assert!(matches!(
            client
                .traffic_session(&nyanpasu_traffic::SessionId("unknown".into()))
                .await,
            Err(ClientError::UnsupportedTraffic)
        ));
        assert!(matches!(
            client.subscribe_traffic_summary().await,
            Err(ClientError::UnsupportedTraffic)
        ));
        server.abort();
    }
}
