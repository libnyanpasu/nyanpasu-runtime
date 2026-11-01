use std::{ffi::OsString, io::Result, time::Duration};

use nyanpasu_utils::runtime::block_on;
use windows_service::{
    define_windows_service,
    service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult, ServiceStatusHandle},
    service_dispatcher,
};

use crate::service_host::{Shutdown, join_worker, start_worker, wait_for_service};
use nyanpasu_service::{
    Command,
    consts::{ExitCode, SERVICE_LABEL},
};

const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;

pub fn run() -> Result<()> {
    service_dispatcher::start(SERVICE_LABEL, ffi_service_main).map_err(std::io::Error::other)
}

define_windows_service!(ffi_service_main, service_main);

fn service_main(args: Vec<OsString>) {
    let exit_code = match run_service(args) {
        Ok(exit_code) => exit_code,
        Err(error) => {
            tracing::error!("service failed: {error}");
            eprintln!("service failed: {error}");
            ExitCode::Other as u32
        }
    };
    if exit_code != 0 {
        std::process::exit(exit_code as i32);
    }
}

struct ServiceHandleGuard {
    handle: ServiceStatusHandle,
    exit_code: u32,
}

impl Drop for ServiceHandleGuard {
    fn drop(&mut self) {
        let _ = self.handle.set_service_status(ServiceStatus {
            current_state: ServiceState::Stopped,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code: if self.exit_code == 0 {
                ServiceExitCode::Win32(0)
            } else {
                ServiceExitCode::ServiceSpecific(self.exit_code)
            },
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
            service_type: SERVICE_TYPE,
        });
    }
}

fn report_stop_pending(
    status_handle: &ServiceStatusHandle,
    checkpoint: u32,
) -> windows_service::Result<()> {
    status_handle.set_service_status(ServiceStatus {
        service_type: SERVICE_TYPE,
        current_state: ServiceState::StopPending,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint,
        wait_hint: Duration::from_secs(10),
        process_id: Some(std::process::id()),
        controls_accepted: ServiceControlAccept::empty(),
    })
}

fn run_service(_arguments: Vec<OsString>) -> windows_service::Result<u32> {
    let shutdown = Shutdown::new();
    let callback_shutdown = shutdown.clone();
    let event_handler = move |control_event| -> ServiceControlHandlerResult {
        match control_event {
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            ServiceControl::Stop | ServiceControl::Preshutdown => {
                callback_shutdown.request();
                tracing::info!("Received service stop control");
                ServiceControlHandlerResult::NoError
            }
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };
    let status_handle = service_control_handler::register(SERVICE_LABEL, event_handler)?;
    let mut guard = ServiceHandleGuard {
        handle: status_handle,
        exit_code: ExitCode::Other as u32,
    };
    status_handle.set_service_status(ServiceStatus {
        service_type: SERVICE_TYPE,
        current_state: ServiceState::StartPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 1,
        wait_hint: Duration::from_secs(10),
        process_id: Some(std::process::id()),
    })?;

    let host_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(windows_service::Error::Winapi)?;
    let command = Command::parse();
    let worker = start_worker(
        &shutdown,
        nyanpasu_service::utils::windows_service::configure_preshutdown,
        || {
            let token = shutdown.token.clone();
            let (done, finished) = tokio::sync::oneshot::channel();
            let handle = std::thread::spawn(move || {
                let exit = block_on(nyanpasu_service::handler(command, token));
                let _ = done.send(());
                exit
            });
            let reported = status_handle.set_service_status(ServiceStatus {
                service_type: SERVICE_TYPE,
                current_state: ServiceState::Running,
                controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::PRESHUTDOWN,
                exit_code: ServiceExitCode::Win32(0),
                checkpoint: 0,
                wait_hint: Duration::default(),
                process_id: Some(std::process::id()),
            });
            (handle, finished, reported)
        },
    )?;
    let Some((handle, finished, reported)) = worker else {
        guard.exit_code = 0;
        return Ok(0);
    };
    let reporting_failed = if let Err(error) = reported {
        tracing::error!("failed to report Running: {error}");
        shutdown.request();
        true
    } else {
        false
    };
    let exit = host_runtime.block_on(wait_for_service(
        &shutdown,
        join_worker(handle, finished),
        |checkpoint| {
            if let Err(error) = report_stop_pending(&status_handle, checkpoint) {
                tracing::warn!("failed to refresh STOP_PENDING checkpoint: {error}");
            }
        },
    ));
    let exit = if reporting_failed {
        ExitCode::Other
    } else {
        exit
    };
    let exit_code = exit as u32;
    guard.exit_code = exit_code;
    Ok(exit_code)
}
