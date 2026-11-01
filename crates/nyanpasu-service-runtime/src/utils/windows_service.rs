use std::time::Duration;

use windows_service::{
    service::ServiceAccess,
    service_manager::{ServiceManager, ServiceManagerAccess},
};

use crate::consts::SERVICE_LABEL;

pub fn configure_preshutdown() -> windows_service::Result<()> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
    let service = manager.open_service(SERVICE_LABEL, ServiceAccess::CHANGE_CONFIG)?;
    service.set_preshutdown_timeout(Duration::from_secs(120))
}
