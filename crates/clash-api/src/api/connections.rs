use std::{fmt, time::Duration};

use chrono::{DateTime, FixedOffset};
use reqwest::Method;
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use uuid::Uuid;

use crate::{Client, Error, Result, retry::RequestMetadata};

/// Snapshot returned by `GET /connections` and each WebSocket frame.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionsSnapshot {
    pub download_total: i64,
    pub upload_total: i64,
    /// Mihomo serializes this field as `null` when no connections exist.
    pub connections: Option<Vec<Connection>>,
    pub memory: Option<u64>,
}

/// Type-only description of an arbitrary JSON value, used to give the `extra`
/// maps below (see [`Connection::extra`], [`ConnectionMetadata::extra`]) a
/// named, exportable specta shape. Never constructed or serialized itself;
/// the actual runtime data stays `serde_json::Value`.
#[derive(serde::Serialize, specta::Type)]
#[serde(untagged)]
pub enum JsonValue {
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Option<JsonValue>>),
    Object(indexmap::IndexMap<String, Option<JsonValue>>),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, specta::Type)]
#[serde(rename_all = "camelCase")]
#[serde(from = "ConnectionWire")]
pub struct Connection {
    pub id: Uuid,
    pub metadata: Option<ConnectionMetadata>,
    pub upload: i64,
    pub download: i64,
    pub start: DateTime<FixedOffset>,
    pub chains: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_chains: Option<Vec<String>>,
    pub rule: String,
    pub rule_payload: String,
    /// Fields Mihomo returns that this type does not yet model. Deserializing
    /// (from Mihomo) collects unknown keys here regardless of direction;
    /// serializing (for our own IPC) emits them under this named key instead
    /// of flattening them, so a future Mihomo field literally named `extra`
    /// cannot collide with it.
    #[serde(rename = "_extra")]
    #[specta(type = indexmap::IndexMap<String, Option<JsonValue>>)]
    pub extra: indexmap::IndexMap<String, serde_json::Value>,
}

/// Deserialize-only mirror of [`Connection`], with unknown fields flattened
/// the way Mihomo sends them. [`Connection`]'s `Deserialize` impl delegates
/// here via `#[serde(from = "ConnectionWire")]` so serialization can use a
/// different, named shape for `extra` (see [`Connection::extra`]).
// Serialize registers the serde attributes that Specta needs for the wire schema;
// the application serializes Connection instead. Decoding below bypasses flatten.
#[derive(serde::Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
struct ConnectionWire {
    id: Uuid,
    metadata: Option<ConnectionMetadata>,
    upload: i64,
    download: i64,
    start: DateTime<FixedOffset>,
    chains: Vec<String>,
    #[serde(default)]
    provider_chains: Option<Vec<String>>,
    rule: String,
    rule_payload: String,
    #[serde(flatten)]
    #[specta(type = indexmap::IndexMap<String, Option<JsonValue>>)]
    extra: indexmap::IndexMap<String, serde_json::Value>,
}

impl<'de> Deserialize<'de> for ConnectionWire {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier)]
        enum Field {
            #[serde(rename = "id")]
            Id,
            #[serde(rename = "metadata")]
            Metadata,
            #[serde(rename = "upload")]
            Upload,
            #[serde(rename = "download")]
            Download,
            #[serde(rename = "start")]
            Start,
            #[serde(rename = "chains")]
            Chains,
            #[serde(rename = "providerChains")]
            ProviderChains,
            #[serde(rename = "rule")]
            Rule,
            #[serde(rename = "rulePayload")]
            RulePayload,
            // Only unknown keys need owned storage for the extra map.
            Other(String),
        }

        struct WireVisitor;

        impl<'de> Visitor<'de> for WireVisitor {
            type Value = ConnectionWire;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a connection object")
            }

            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut id = None;
                let mut metadata = None;
                let mut upload = None;
                let mut download = None;
                let mut start = None;
                let mut chains = None;
                let mut provider_chains = None;
                let mut rule = None;
                let mut rule_payload = None;
                let mut unknown_fields = indexmap::IndexMap::new();

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::Id => read_once(&mut id, &mut map, "id")?,
                        Field::Metadata => read_once(&mut metadata, &mut map, "metadata")?,
                        Field::Upload => read_once(&mut upload, &mut map, "upload")?,
                        Field::Download => read_once(&mut download, &mut map, "download")?,
                        Field::Start => read_once(&mut start, &mut map, "start")?,
                        Field::Chains => read_once(&mut chains, &mut map, "chains")?,
                        Field::ProviderChains => {
                            read_once(&mut provider_chains, &mut map, "providerChains")?
                        }
                        Field::Rule => read_once(&mut rule, &mut map, "rule")?,
                        Field::RulePayload => {
                            read_once(&mut rule_payload, &mut map, "rulePayload")?
                        }
                        Field::Other(key) => {
                            unknown_fields.insert(key, map.next_value()?);
                        }
                    }
                }

                Ok(ConnectionWire {
                    id: id.ok_or_else(|| de::Error::missing_field("id"))?,
                    metadata: metadata.unwrap_or_default(),
                    upload: upload.ok_or_else(|| de::Error::missing_field("upload"))?,
                    download: download.ok_or_else(|| de::Error::missing_field("download"))?,
                    start: start.ok_or_else(|| de::Error::missing_field("start"))?,
                    chains: chains.ok_or_else(|| de::Error::missing_field("chains"))?,
                    provider_chains: provider_chains.unwrap_or_default(),
                    rule: rule.ok_or_else(|| de::Error::missing_field("rule"))?,
                    rule_payload: rule_payload
                        .ok_or_else(|| de::Error::missing_field("rulePayload"))?,
                    extra: unknown_fields,
                })
            }
        }

        deserializer.deserialize_map(WireVisitor)
    }
}

impl From<ConnectionWire> for Connection {
    fn from(wire: ConnectionWire) -> Self {
        Self {
            id: wire.id,
            metadata: wire.metadata,
            upload: wire.upload,
            download: wire.download,
            start: wire.start,
            chains: wire.chains,
            provider_chains: wire.provider_chains,
            rule: wire.rule,
            rule_payload: wire.rule_payload,
            extra: wire.extra,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, specta::Type)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionNetwork {
    Tcp,
    Udp,
    All,
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, specta::Type)]
pub enum ConnectionType {
    #[serde(rename = "HTTP")]
    Http,
    #[serde(rename = "HTTPS")]
    Https,
    #[serde(rename = "Socks4")]
    Socks4,
    #[serde(rename = "Socks5")]
    Socks5,
    #[serde(rename = "ShadowSocks")]
    ShadowSocks,
    Snell,
    #[serde(rename = "Vmess")]
    Vmess,
    #[serde(rename = "Vless")]
    Vless,
    Redir,
    TProxy,
    Trojan,
    Tunnel,
    Tun,
    Tuic,
    Hysteria2,
    #[serde(rename = "AnyTLS")]
    AnyTls,
    Mieru,
    Sudoku,
    TrustTunnel,
    ShadowQuic,
    Inner,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, specta::Type)]
pub enum DnsMode {
    #[serde(rename = "normal")]
    Normal,
    #[serde(rename = "fake-ip")]
    FakeIp,
    #[serde(rename = "redir-host")]
    RedirHost,
    #[serde(rename = "hosts")]
    Hosts,
    Unknown,
}

/// Known `ConnectionMetadata` fields. Flattened into both [`ConnectionMetadata`]
/// (serialize) and [`ConnectionMetadataWire`] (deserialize) so they are only
/// defined once; see [`ConnectionMetadata::extra`] for the unknown-field split.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, specta::Type)]
pub struct ConnectionMetadataFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<crate::ConfigEnum<ConnectionNetwork>>,
    #[serde(rename = "type")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_type: Option<crate::ConfigEnum<ConnectionType>>,
    #[serde(rename = "sourceIP")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<String>,
    #[serde(rename = "destinationIP")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_ip: Option<String>,
    #[serde(rename = "sourceGeoIP")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_geo_ip: Option<Vec<String>>,
    #[serde(rename = "destinationGeoIP")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_geo_ip: Option<Vec<String>>,
    #[serde(rename = "sourceIPASN")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ip_asn: Option<String>,
    #[serde(rename = "destinationIPASN")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_ip_asn: Option<String>,
    #[serde(rename = "sourcePort", with = "optional_port")]
    #[specta(type = Option<String>)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_port: Option<u16>,
    #[serde(rename = "destinationPort", with = "optional_port")]
    #[specta(type = Option<String>)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_port: Option<u16>,
    #[serde(rename = "inboundIP")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_ip: Option<String>,
    #[serde(rename = "inboundPort", with = "optional_port")]
    #[specta(type = Option<String>)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_port: Option<u16>,
    #[serde(rename = "inboundName")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_name: Option<String>,
    #[serde(rename = "inboundUser")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_user: Option<String>,
    #[serde(rename = "rematchName")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rematch_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(rename = "dnsMode")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dns_mode: Option<crate::ConfigEnum<DnsMode>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<String>,
    #[serde(rename = "processPath")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_path: Option<String>,
    #[serde(rename = "specialProxy")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub special_proxy: Option<String>,
    #[serde(rename = "specialRules")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub special_rules: Option<String>,
    #[serde(rename = "remoteDestination")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_destination: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dscp: Option<u8>,
    #[serde(rename = "sniffHost")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sniff_host: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, specta::Type)]
#[serde(from = "ConnectionMetadataWire")]
pub struct ConnectionMetadata {
    #[serde(flatten)]
    pub known: ConnectionMetadataFields,
    /// See [`Connection::extra`].
    #[serde(rename = "_extra")]
    #[specta(type = indexmap::IndexMap<String, Option<JsonValue>>)]
    pub extra: indexmap::IndexMap<String, serde_json::Value>,
}

/// Deserialize-only mirror of [`ConnectionMetadata`]; see [`ConnectionWire`].
// Keep the flattened Specta wire schema while decoding directly into known fields.
#[derive(serde::Serialize, specta::Type)]
struct ConnectionMetadataWire {
    #[serde(flatten)]
    known: ConnectionMetadataFields,
    #[serde(flatten)]
    #[specta(type = indexmap::IndexMap<String, Option<JsonValue>>)]
    extra: indexmap::IndexMap<String, serde_json::Value>,
}

impl<'de> Deserialize<'de> for ConnectionMetadataWire {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier)]
        enum Field {
            #[serde(rename = "network")]
            Network,
            #[serde(rename = "type")]
            ConnectionType,
            #[serde(rename = "sourceIP")]
            SourceIp,
            #[serde(rename = "destinationIP")]
            DestinationIp,
            #[serde(rename = "sourceGeoIP")]
            SourceGeoIp,
            #[serde(rename = "destinationGeoIP")]
            DestinationGeoIp,
            #[serde(rename = "sourceIPASN")]
            SourceIpAsn,
            #[serde(rename = "destinationIPASN")]
            DestinationIpAsn,
            #[serde(rename = "sourcePort")]
            SourcePort,
            #[serde(rename = "destinationPort")]
            DestinationPort,
            #[serde(rename = "inboundIP")]
            InboundIp,
            #[serde(rename = "inboundPort")]
            InboundPort,
            #[serde(rename = "inboundName")]
            InboundName,
            #[serde(rename = "inboundUser")]
            InboundUser,
            #[serde(rename = "rematchName")]
            RematchName,
            #[serde(rename = "host")]
            Host,
            #[serde(rename = "dnsMode")]
            DnsMode,
            #[serde(rename = "uid")]
            Uid,
            #[serde(rename = "process")]
            Process,
            #[serde(rename = "processPath")]
            ProcessPath,
            #[serde(rename = "specialProxy")]
            SpecialProxy,
            #[serde(rename = "specialRules")]
            SpecialRules,
            #[serde(rename = "remoteDestination")]
            RemoteDestination,
            #[serde(rename = "dscp")]
            Dscp,
            #[serde(rename = "sniffHost")]
            SniffHost,
            // Only unknown keys need owned storage for the extra map.
            Other(String),
        }

        struct WireVisitor;

        impl<'de> Visitor<'de> for WireVisitor {
            type Value = ConnectionMetadataWire;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a connection metadata object")
            }

            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut network = None;
                let mut connection_type = None;
                let mut source_ip = None;
                let mut destination_ip = None;
                let mut source_geo_ip = None;
                let mut destination_geo_ip = None;
                let mut source_ip_asn = None;
                let mut destination_ip_asn = None;
                let mut source_port: Option<optional_port::Port> = None;
                let mut destination_port: Option<optional_port::Port> = None;
                let mut inbound_ip = None;
                let mut inbound_port: Option<optional_port::Port> = None;
                let mut inbound_name = None;
                let mut inbound_user = None;
                let mut rematch_name = None;
                let mut host = None;
                let mut dns_mode = None;
                let mut uid = None;
                let mut process = None;
                let mut process_path = None;
                let mut special_proxy = None;
                let mut special_rules = None;
                let mut remote_destination = None;
                let mut dscp = None;
                let mut sniff_host = None;
                let mut unknown_fields = indexmap::IndexMap::new();

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::Network => read_once(&mut network, &mut map, "network")?,
                        Field::ConnectionType => read_once(&mut connection_type, &mut map, "type")?,
                        Field::SourceIp => read_once(&mut source_ip, &mut map, "sourceIP")?,
                        Field::DestinationIp => {
                            read_once(&mut destination_ip, &mut map, "destinationIP")?
                        }
                        Field::SourceGeoIp => {
                            read_once(&mut source_geo_ip, &mut map, "sourceGeoIP")?
                        }
                        Field::DestinationGeoIp => {
                            read_once(&mut destination_geo_ip, &mut map, "destinationGeoIP")?
                        }
                        Field::SourceIpAsn => {
                            read_once(&mut source_ip_asn, &mut map, "sourceIPASN")?
                        }
                        Field::DestinationIpAsn => {
                            read_once(&mut destination_ip_asn, &mut map, "destinationIPASN")?
                        }
                        Field::SourcePort => read_once(&mut source_port, &mut map, "sourcePort")?,
                        Field::DestinationPort => {
                            read_once(&mut destination_port, &mut map, "destinationPort")?
                        }
                        Field::InboundIp => read_once(&mut inbound_ip, &mut map, "inboundIP")?,
                        Field::InboundPort => {
                            read_once(&mut inbound_port, &mut map, "inboundPort")?
                        }
                        Field::InboundName => {
                            read_once(&mut inbound_name, &mut map, "inboundName")?
                        }
                        Field::InboundUser => {
                            read_once(&mut inbound_user, &mut map, "inboundUser")?
                        }
                        Field::RematchName => {
                            read_once(&mut rematch_name, &mut map, "rematchName")?
                        }
                        Field::Host => read_once(&mut host, &mut map, "host")?,
                        Field::DnsMode => read_once(&mut dns_mode, &mut map, "dnsMode")?,
                        Field::Uid => read_once(&mut uid, &mut map, "uid")?,
                        Field::Process => read_once(&mut process, &mut map, "process")?,
                        Field::ProcessPath => {
                            read_once(&mut process_path, &mut map, "processPath")?
                        }
                        Field::SpecialProxy => {
                            read_once(&mut special_proxy, &mut map, "specialProxy")?
                        }
                        Field::SpecialRules => {
                            read_once(&mut special_rules, &mut map, "specialRules")?
                        }
                        Field::RemoteDestination => {
                            read_once(&mut remote_destination, &mut map, "remoteDestination")?
                        }
                        Field::Dscp => read_once(&mut dscp, &mut map, "dscp")?,
                        Field::SniffHost => read_once(&mut sniff_host, &mut map, "sniffHost")?,
                        Field::Other(key) => {
                            unknown_fields.insert(key, map.next_value()?);
                        }
                    }
                }

                Ok(ConnectionMetadataWire {
                    known: ConnectionMetadataFields {
                        network: network.unwrap_or_default(),
                        connection_type: connection_type.unwrap_or_default(),
                        source_ip: source_ip.unwrap_or_default(),
                        destination_ip: destination_ip.unwrap_or_default(),
                        source_geo_ip: source_geo_ip.unwrap_or_default(),
                        destination_geo_ip: destination_geo_ip.unwrap_or_default(),
                        source_ip_asn: source_ip_asn.unwrap_or_default(),
                        destination_ip_asn: destination_ip_asn.unwrap_or_default(),
                        source_port: source_port.and_then(|port| port.0),
                        destination_port: destination_port.and_then(|port| port.0),
                        inbound_ip: inbound_ip.unwrap_or_default(),
                        inbound_port: inbound_port.and_then(|port| port.0),
                        inbound_name: inbound_name.unwrap_or_default(),
                        inbound_user: inbound_user.unwrap_or_default(),
                        rematch_name: rematch_name.unwrap_or_default(),
                        host: host.unwrap_or_default(),
                        dns_mode: dns_mode.unwrap_or_default(),
                        uid: uid.unwrap_or_default(),
                        process: process.unwrap_or_default(),
                        process_path: process_path.unwrap_or_default(),
                        special_proxy: special_proxy.unwrap_or_default(),
                        special_rules: special_rules.unwrap_or_default(),
                        remote_destination: remote_destination.unwrap_or_default(),
                        dscp: dscp.unwrap_or_default(),
                        sniff_host: sniff_host.unwrap_or_default(),
                    },
                    extra: unknown_fields,
                })
            }
        }

        deserializer.deserialize_map(WireVisitor)
    }
}

// The outer Option records presence, including a field explicitly set to null.
fn read_once<'de, M: MapAccess<'de>, T: Deserialize<'de>>(
    field: &mut Option<T>,
    map: &mut M,
    name: &'static str,
) -> std::result::Result<(), M::Error> {
    if field.is_some() {
        return Err(de::Error::duplicate_field(name));
    }
    *field = Some(map.next_value()?);
    Ok(())
}

impl From<ConnectionMetadataWire> for ConnectionMetadata {
    fn from(wire: ConnectionMetadataWire) -> Self {
        Self {
            known: wire.known,
            extra: wire.extra,
        }
    }
}

/// WebSocket sampling interval. Mihomo interprets it as decimal milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, specta::Type)]
pub struct ConnectionStreamQuery {
    interval: Duration,
}

impl ConnectionStreamQuery {
    pub fn new(interval: Duration) -> Result<Self> {
        if interval.as_millis() == 0 {
            return Err(Error::InvalidArgument {
                argument: "interval",
                message: "must be at least one millisecond".to_owned(),
            });
        }
        if interval.as_millis() > isize::MAX as u128 {
            return Err(Error::InvalidArgument {
                argument: "interval",
                message: "does not fit Mihomo's millisecond query".to_owned(),
            });
        }
        Ok(Self { interval })
    }

    pub const fn interval(self) -> Duration {
        self.interval
    }

    fn milliseconds(self) -> u128 {
        self.interval.as_millis()
    }
}

impl Default for ConnectionStreamQuery {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
        }
    }
}

impl Client {
    /// Fetch the one-shot HTTP connections snapshot.
    pub async fn connections(&self) -> Result<ConnectionsSnapshot> {
        self.send_json(
            RequestMetadata::new("connections", Method::GET, true),
            || self.get("/connections"),
        )
        .await
    }

    /// Open the sampled, typed connections WebSocket.
    pub async fn connections_ws(
        &self,
        query: ConnectionStreamQuery,
    ) -> Result<crate::WebSocketStream<ConnectionsSnapshot>> {
        let interval = query.milliseconds().to_string();
        self.websocket(
            RequestMetadata::new("connections_ws", Method::GET, true),
            || Ok(self.get("/connections")?.query(&[("interval", &interval)])),
        )
        .await
        .map(|socket| crate::WebSocketStream::new(socket, "connections_ws"))
    }

    /// Close a connection. Mihomo also returns success when the id is absent.
    pub async fn close_connection(&self, id: Uuid) -> Result<()> {
        let url = self.endpoint_with_segments("/connections", [id.to_string()])?;
        self.send_empty(
            RequestMetadata::new("close_connection", Method::DELETE, true),
            || Ok(self.request_url(Method::DELETE, url.clone())),
        )
        .await
    }

    /// Close every current connection. This is intentionally not auto-retried.
    pub async fn close_all_connections(&self) -> Result<()> {
        self.send_empty(
            RequestMetadata::new("close_all_connections", Method::DELETE, false),
            || self.delete("/connections"),
        )
        .await
    }
}

mod optional_port {
    use std::fmt;

    use serde::{
        Deserialize, Deserializer, Serialize, Serializer,
        de::{self, Visitor},
    };

    // Allows MapAccess::next_value to use the same parser as the public fields type.
    #[derive(Deserialize)]
    #[serde(transparent)]
    pub(super) struct Port(#[serde(deserialize_with = "deserialize")] pub Option<u16>);

    pub fn serialize<S: Serializer>(value: &Option<u16>, serializer: S) -> Result<S::Ok, S::Error> {
        value.map(|port| port.to_string()).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u16>, D::Error> {
        struct PortVisitor;

        impl<'de> Visitor<'de> for PortVisitor {
            type Value = Option<u16>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("null, a u16, or a decimal port string")
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_some<D: Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Self::Value, D::Error> {
                deserializer.deserialize_any(self)
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                u16::try_from(value).map(Some).map_err(E::custom)
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                u16::try_from(value).map(Some).map_err(E::custom)
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse().map(Some).map_err(E::custom)
            }
        }

        deserializer.deserialize_option(PortVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_ports_follow_gos_string_encoding() {
        let metadata: ConnectionMetadata = serde_json::from_str(
            r#"{
                "network":"tcp","type":"HTTP",
                "sourceIP":"127.0.0.1","destinationIP":"1.1.1.1",
                "sourceGeoIP":null,"destinationGeoIP":[],
                "sourceIPASN":"","destinationIPASN":"AS13335",
                "sourcePort":"1234","destinationPort":"443",
                "inboundIP":"127.0.0.1","inboundPort":"7890",
                "inboundName":"mixed","inboundUser":"","rematchName":"",
                "host":"example.com","dnsMode":"normal","uid":0,
                "process":"","processPath":"","specialProxy":"",
                "specialRules":"","remoteDestination":"","dscp":0,"sniffHost":""
            }"#,
        )
        .unwrap();

        assert_eq!(metadata.known.source_port, Some(1234));
        assert_eq!(metadata.known.destination_port, Some(443));
        assert_eq!(metadata.known.source_geo_ip, None);
    }

    #[test]
    fn zero_connection_interval_is_rejected_before_the_go_ticker_can_panic() {
        assert!(matches!(
            ConnectionStreamQuery::new(Duration::ZERO),
            Err(Error::InvalidArgument {
                argument: "interval",
                ..
            })
        ));
    }

    #[test]
    fn unknown_fields_deserialize_flattened_and_serialize_under_named_extra() {
        let connection: Connection = serde_json::from_str(
            r#"{
                "id":"550e8400-e29b-41d4-a716-446655440000","upload":1,"download":2,
                "start":"2026-09-07T12:00:00Z","chains":["DIRECT"],"rule":"Match","rulePayload":"",
                "metadata":{"extension":{"nested":[1,"two"]}},
                "newConnectionField":true
            }"#,
        )
        .unwrap();
        assert_eq!(
            connection.extra.get("newConnectionField"),
            Some(&serde_json::json!(true))
        );
        assert_eq!(
            connection.metadata.as_ref().unwrap().extra.get("extension"),
            Some(&serde_json::json!({"nested": [1, "two"]}))
        );

        let serialized = serde_json::to_value(&connection).unwrap();
        assert_eq!(serialized["_extra"]["newConnectionField"], true);
        assert_eq!(
            serialized["metadata"]["_extra"]["extension"]["nested"][1],
            "two"
        );
        assert!(serialized.get("newConnectionField").is_none());
        assert!(serialized["metadata"].get("extension").is_none());
    }

    #[test]
    fn a_field_literally_named_extra_does_not_collide_with_the_named_wrapper() {
        let connection: Connection = serde_json::from_str(
            r#"{
                "id":"550e8400-e29b-41d4-a716-446655440000","upload":1,"download":2,
                "start":"2026-09-07T12:00:00Z","chains":["DIRECT"],"rule":"Match","rulePayload":"",
                "extra":"mihomo-added-this"
            }"#,
        )
        .unwrap();
        assert_eq!(
            connection.extra.get("extra"),
            Some(&serde_json::json!("mihomo-added-this"))
        );

        let serialized = serde_json::to_value(&connection).unwrap();
        assert_eq!(serialized["_extra"]["extra"], "mihomo-added-this");
    }
}
