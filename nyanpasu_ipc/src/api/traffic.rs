//! Versioned traffic domain operations. Credentials and storage paths never enter these models.
pub const TRAFFIC_QUERY_VERSION: u32 = 1;
pub const TRAFFIC_SESSION_ENDPOINT: &str = "/v1/traffic/session";
pub const TRAFFIC_CONNECTIONS_ENDPOINT: &str = "/v1/traffic/connections";
pub const TRAFFIC_USAGE_ENDPOINT: &str = "/v1/traffic/usage";
pub const TRAFFIC_TOPOLOGY_ENDPOINT: &str = "/v1/traffic/topology";
pub const TRAFFIC_SUMMARY_ENDPOINT: &str = "/v1/traffic/summary";
pub const TRAFFIC_DETAILS_ENDPOINT: &str = "/v1/traffic/details";
pub const TRAFFIC_CURRENT_SESSION_ENDPOINT: &str = "/v1/traffic/current-session";
pub const TRAFFIC_STATUS_ENDPOINT: &str = "/v1/traffic/status";
