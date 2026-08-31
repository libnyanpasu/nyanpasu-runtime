use axum::Router;
use nyanpasu_ipc::{
    api::contract::{
        CoreCheck, CoreRestart, CoreStart, CoreStop, CoreV2Operation, CoreV2Status, CoreV2Submit,
    },
    server::RegisterOperation,
};

use super::AppState;

pub mod check;
pub mod restart;
pub mod start;
pub mod stop;
pub mod v2;

pub fn setup() -> Router<AppState> {
    Router::new()
        .register(CoreStart, start::start)
        .register(CoreStop, stop::stop)
        .register(CoreRestart, restart::restart)
        .register(CoreCheck, check::check)
        .register(CoreV2Submit, v2::submit)
        .register(CoreV2Operation, v2::operation)
        .register(CoreV2Status, v2::status)
}
