use crate::api::status::StatusResBody;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "specta", derive(specta::Type))]
pub enum ServiceStatus {
    NotInstalled,
    Stopped,
    Running,
    StartPending,
    StopPending,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "specta", derive(specta::Type))]
pub struct StatusInfo<'n> {
    pub name: Cow<'n, str>,    // The client program name
    pub version: Cow<'n, str>, // The client program version
    pub status: ServiceStatus,
    pub server: Option<StatusResBody<'n>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detailed_json_roundtrips_pending_states() {
        for wire in ["start_pending", "stop_pending", "unknown"] {
            let json = serde_json::json!({
                "name": "nyanpasu-service", "version": "2.0.2", "status": wire, "server": null
            });
            let info: StatusInfo<'static> = serde_json::from_value(json.clone()).unwrap();
            assert_eq!(serde_json::to_value(info).unwrap(), json);
        }
    }
}
