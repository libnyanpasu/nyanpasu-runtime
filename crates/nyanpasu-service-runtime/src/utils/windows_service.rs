use std::time::Duration;

use nyanpasu_ipc::types::ServiceStatus;
use windows::Win32::Foundation::ERROR_SERVICE_DOES_NOT_EXIST;
use windows_service::{
    service::{ServiceAccess, ServiceState},
    service_manager::{ServiceManager, ServiceManagerAccess},
};

use crate::consts::SERVICE_LABEL;

pub fn configure_preshutdown() -> windows_service::Result<()> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
    let service = manager.open_service(SERVICE_LABEL, ServiceAccess::CHANGE_CONFIG)?;
    service.set_preshutdown_timeout(Duration::from_secs(120))
}

pub fn query_status() -> windows_service::Result<ServiceStatus> {
    query_status_for(SERVICE_LABEL)
}

fn query_status_for(name: &str) -> windows_service::Result<ServiceStatus> {
    let result = (|| {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
        let service = manager.open_service(name, ServiceAccess::QUERY_STATUS)?;
        Ok(service.query_status()?.current_state)
    })();
    map_query_result(result)
}

fn map_query_result(
    result: windows_service::Result<ServiceState>,
) -> windows_service::Result<ServiceStatus> {
    match result {
        Ok(state) => Ok(match state {
            ServiceState::Stopped => ServiceStatus::Stopped,
            ServiceState::Running => ServiceStatus::Running,
            ServiceState::StartPending => ServiceStatus::StartPending,
            ServiceState::StopPending => ServiceStatus::StopPending,
            ServiceState::Paused | ServiceState::PausePending | ServiceState::ContinuePending => {
                ServiceStatus::Unknown
            }
        }),
        Err(windows_service::Error::Winapi(error))
            if error.raw_os_error() == Some(ERROR_SERVICE_DOES_NOT_EXIST.0 as i32) =>
        {
            Ok(ServiceStatus::NotInstalled)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows::Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_SERVICE_DOES_NOT_EXIST};

    #[test]
    fn scm_states_preserve_pending_and_only_stopped_maps_to_stopped() {
        for (state, expected) in [
            (ServiceState::Stopped, ServiceStatus::Stopped),
            (ServiceState::Running, ServiceStatus::Running),
            (ServiceState::StartPending, ServiceStatus::StartPending),
            (ServiceState::StopPending, ServiceStatus::StopPending),
            (ServiceState::Paused, ServiceStatus::Unknown),
            (ServiceState::PausePending, ServiceStatus::Unknown),
            (ServiceState::ContinuePending, ServiceStatus::Unknown),
        ] {
            assert_eq!(map_query_result(Ok(state)).unwrap(), expected);
        }
    }

    #[test]
    fn access_denied_is_not_not_installed() {
        let error = windows_service::Error::Winapi(std::io::Error::from_raw_os_error(
            ERROR_ACCESS_DENIED.0 as i32,
        ));
        let error = map_query_result(Err(error)).unwrap_err();
        assert!(matches!(error, windows_service::Error::Winapi(error)
            if error.raw_os_error() == Some(ERROR_ACCESS_DENIED.0 as i32)));
    }

    #[test]
    fn only_missing_service_maps_to_not_installed() {
        let error = windows_service::Error::Winapi(std::io::Error::from_raw_os_error(
            ERROR_SERVICE_DOES_NOT_EXIST.0 as i32,
        ));
        assert_eq!(
            map_query_result(Err(error)).unwrap(),
            ServiceStatus::NotInstalled
        );
        assert!(map_query_result(Err(windows_service::Error::ArgumentHasNulByte("name"))).is_err());
    }
    #[test]
    fn real_scm_reports_a_missing_service() {
        let directory = tempfile::tempdir().unwrap();
        let suffix = directory.path().file_name().unwrap().to_string_lossy();
        let name = format!("nyanpasu-status-test-{suffix}");
        assert_eq!(
            query_status_for(&name).unwrap(),
            ServiceStatus::NotInstalled
        );
    }
}
