#![feature(error_generic_member_access)]

mod cmds;
pub mod consts;
mod logging;
mod server;
pub mod utils;

pub use cmds::Command;
use consts::ExitCode;
use nyanpasu_utils::process::ProcessActivity;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub async fn handler(
    command: Command,
    shutdown: CancellationToken,
    process_activity: Option<ProcessActivity>,
) -> ExitCode {
    crate::utils::deadlock_detection();
    let result = cmds::process(command, shutdown, process_activity).await;
    match result {
        Ok(_) => ExitCode::Normal,
        Err(cmds::CommandError::PermissionDenied) => {
            eprintln!("Permission denied, please run as administrator or root");
            ExitCode::PermissionDenied
        }
        Err(cmds::CommandError::ServiceNotInstalled) => {
            eprintln!("Service not installed");
            ExitCode::ServiceNotInstalled
        }
        Err(cmds::CommandError::ServiceAlreadyInstalled) => {
            eprintln!("Service already installed");
            ExitCode::ServiceAlreadyInstalled
        }
        Err(cmds::CommandError::ServiceAlreadyStopped) => {
            eprintln!("Service already stopped");
            ExitCode::ServiceAlreadyStopped
        }
        Err(cmds::CommandError::ServiceAlreadyRunning) => {
            eprintln!("Service already running");
            ExitCode::ServiceAlreadyRunning
        }
        Err(e) => {
            error!("Error: {:#?}", e);
            ExitCode::Other
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_module_paths_keep_the_legacy_log_target_root() {
        assert_eq!(module_path!().split("::").next(), Some("nyanpasu_service"));
    }
}
