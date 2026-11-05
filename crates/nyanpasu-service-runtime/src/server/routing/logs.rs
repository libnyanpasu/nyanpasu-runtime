use axum::{Json, Router, extract::State, http::StatusCode};
use nyanpasu_ipc::{
    api::{
        RBuilder,
        contract::{LogsInspect, LogsRetrieve},
        log::{LogsRes, LogsResBody},
    },
    server::RegisterOperation,
};

use super::AppState;

use nyanpasu_ipc::api::{
    R,
    contract::{
        CoreLogClose, CoreLogFiles, CoreLogOpen, CoreLogQuery, LogClose, LogFiles, LogOpen,
        LogQuery,
    },
    log::OwnedLogRequest,
};
use nyanpasu_logging::{
    LogError, LogFileInfo, LogPage, LogResult, LogSession, OpenLogs, QueryLogs,
};

pub fn sessions() -> Router<AppState> {
    Router::new()
        .register(LogFiles, list_files)
        .register(LogOpen, open_session)
        .register(LogQuery, query_session)
        .register(LogClose, close_session)
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
}
pub fn core_sessions() -> Router<AppState> {
    Router::new()
        .register(CoreLogFiles, core_list_files)
        .register(CoreLogOpen, core_open_session)
        .register(CoreLogQuery, core_query_session)
        .register(CoreLogClose, core_close_session)
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
}
async fn bounded<T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug>(
    future: impl std::future::Future<Output = LogResult<T>>,
) -> Json<R<'static, LogResult<T>>> {
    Json(RBuilder::success(
        tokio::time::timeout(std::time::Duration::from_secs(4), future)
            .await
            .unwrap_or(Err(LogError::Unavailable)),
    ))
}
async fn list_files(
    State(state): State<AppState>,
) -> Json<R<'static, LogResult<Vec<LogFileInfo>>>> {
    bounded(state.logs.catalog()).await
}
async fn open_session(
    State(state): State<AppState>,
    Json(req): Json<OwnedLogRequest<OpenLogs>>,
) -> Json<R<'static, LogResult<LogSession>>> {
    bounded(state.logs.open(req.owner, req.request)).await
}
async fn query_session(
    State(state): State<AppState>,
    Json(req): Json<OwnedLogRequest<QueryLogs>>,
) -> Json<R<'static, LogResult<LogPage>>> {
    bounded(state.logs.query(req.owner, req.request)).await
}
async fn close_session(
    State(state): State<AppState>,
    Json(req): Json<OwnedLogRequest<String>>,
) -> Json<R<'static, LogResult<()>>> {
    bounded(state.logs.close(req.owner, req.request)).await
}

async fn core_list_files(
    State(state): State<AppState>,
) -> Json<R<'static, LogResult<Vec<LogFileInfo>>>> {
    bounded(state.core_logs.catalog()).await
}
async fn core_open_session(
    State(state): State<AppState>,
    Json(req): Json<OwnedLogRequest<OpenLogs>>,
) -> Json<R<'static, LogResult<LogSession>>> {
    bounded(state.core_logs.open(req.owner, req.request)).await
}
async fn core_query_session(
    State(state): State<AppState>,
    Json(req): Json<OwnedLogRequest<QueryLogs>>,
) -> Json<R<'static, LogResult<LogPage>>> {
    bounded(state.core_logs.query(req.owner, req.request)).await
}
async fn core_close_session(
    State(state): State<AppState>,
    Json(req): Json<OwnedLogRequest<String>>,
) -> Json<R<'static, LogResult<()>>> {
    bounded(state.core_logs.close(req.owner, req.request)).await
}

pub fn setup() -> Router<AppState> {
    Router::new()
        .register(LogsRetrieve, retrieve_logs)
        .register(LogsInspect, inspect_logs)
}

pub async fn retrieve_logs(State(state): State<AppState>) -> (StatusCode, Json<LogsRes<'static>>) {
    let logs = state.logger.retrieve_logs();
    (
        StatusCode::OK,
        Json(RBuilder::success(LogsResBody { logs })),
    )
}

pub async fn inspect_logs(State(state): State<AppState>) -> (StatusCode, Json<LogsRes<'static>>) {
    let logs = state.logger.inspect_logs();
    (
        StatusCode::OK,
        Json(RBuilder::success(LogsResBody { logs })),
    )
}
