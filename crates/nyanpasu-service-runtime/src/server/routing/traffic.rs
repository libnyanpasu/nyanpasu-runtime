use super::AppState;
use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nyanpasu_ipc::{
    api::{R, RBuilder, contract::*},
    server::RegisterOperation,
};
use nyanpasu_traffic::*;

pub fn queries() -> Router<AppState> {
    Router::new()
        .register(TrafficCurrentSession, current_session)
        .register(TrafficStatus, status)
        .register(TrafficSession, session)
        .register(TrafficConnections, connections)
        .register(TrafficUsage, usage)
        .register(TrafficTopology, topology)
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
}
async fn session(
    State(state): State<AppState>,
    Json(request): Json<SessionId>,
) -> Json<R<'static, TrafficResult<SessionRecord>>> {
    let result = match &state.traffic {
        Ok(client) => client.session(request).await,
        Err(error) => Err(error.clone()),
    };
    Json(RBuilder::success(result))
}
async fn connections(
    State(state): State<AppState>,
    Json(request): Json<ConnectionsQuery>,
) -> Json<R<'static, TrafficResult<ConnectionPage>>> {
    let result = match &state.traffic {
        Ok(client) => client.query_connections(request).await,
        Err(error) => Err(error.clone()),
    };
    Json(RBuilder::success(result))
}
async fn usage(
    State(state): State<AppState>,
    Json(request): Json<UsageQuery>,
) -> Json<R<'static, TrafficResult<UsageResult>>> {
    let result = match &state.traffic {
        Ok(client) => client.query_usage(request).await,
        Err(error) => Err(error.clone()),
    };
    Json(RBuilder::success(result))
}
async fn topology(
    State(state): State<AppState>,
    Json(request): Json<TopologyQuery>,
) -> Json<R<'static, TrafficResult<TopologyResult>>> {
    let result = match &state.traffic {
        Ok(client) => client.query_topology(request).await,
        Err(error) => Err(error.clone()),
    };
    Json(RBuilder::success(result))
}

pub fn subscriptions() -> Router<AppState> {
    Router::new()
        .route(
            nyanpasu_ipc::api::traffic::TRAFFIC_SUMMARY_ENDPOINT,
            get(summary),
        )
        .route(
            nyanpasu_ipc::api::traffic::TRAFFIC_DETAILS_ENDPOINT,
            get(details),
        )
}
async fn summary(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    match state.traffic {
        Ok(client) => {
            ws.on_upgrade(move |socket| stream_latest(socket, client.subscribe_summary()))
        }
        Err(error) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(error)).into_response(),
    }
}
async fn details(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    match state.traffic {
        Ok(client) => {
            ws.on_upgrade(move |socket| stream_latest(socket, client.subscribe_details()))
        }
        Err(error) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(error)).into_response(),
    }
}
/// Each subscriber holds just the latest committed domain frame. Disconnecting
/// drops the receiver, and never cancels the host's collector.
async fn stream_latest<T: serde::Serialize + Clone + Send + Sync + 'static>(
    socket: WebSocket,
    mut receiver: tokio::sync::watch::Receiver<Option<T>>,
) {
    let (mut sink, mut incoming) = socket.split();
    let sender = async {
        loop {
            let current = receiver.borrow_and_update().clone();
            let bytes = match serde_json::to_vec(&current) {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::error!(%error, "traffic serialization failed");
                    break;
                }
            };
            if sink.send(Message::Binary(bytes.into())).await.is_err() {
                break;
            }
            if receiver.changed().await.is_err() {
                break;
            }
        }
    };
    let reader = async { while let Some(Ok(_)) = incoming.next().await {} };
    tokio::select! { _ = sender => {}, _ = reader => {} }
}

async fn current_session(
    State(state): State<AppState>,
) -> Json<R<'static, TrafficResult<Option<SessionRecord>>>> {
    let result = match &state.traffic {
        Ok(client) => client.current_session().await,
        Err(error) => Err(error.clone()),
    };
    Json(RBuilder::success(result))
}
async fn status(State(state): State<AppState>) -> Json<R<'static, TrafficResult<()>>> {
    let result = match &state.traffic {
        Ok(client) => client
            .subscribe_status()
            .borrow()
            .clone()
            .map_or(Ok(()), Err),
        Err(error) => Err(error.clone()),
    };
    Json(RBuilder::success(result))
}
