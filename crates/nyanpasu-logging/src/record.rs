use crate::Level;
use serde_json::Value;
use std::borrow::Cow;

pub(crate) struct Record<'a> {
    pub timestamp: Option<i64>,
    pub level: Level,
    pub target: &'a str,
    pub message: Cow<'a, str>,
    pub unparsed: bool,
    pub truncated: bool,
}

/// Decode both tracing JSON and the manager's console archive envelope.
pub(crate) fn decode<'a>(value: Option<&'a Value>, raw: &'a str) -> Record<'a> {
    let object = value.filter(|v| v.is_object());
    let core = object.is_some_and(|v| matches!(v["t"].as_str(), Some("log" | "gap")));
    let gap = object.is_some_and(|v| v["t"] == "gap");
    Record {
        timestamp: object.and_then(|v| {
            if core {
                v["at"].as_i64()
            } else {
                v["timestamp"]
                    .as_str()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|t| t.timestamp_millis())
            }
        }),
        level: if gap {
            Level::Warn
        } else {
            object
                .and_then(|v| v["level"].as_str())
                .map(Level::parse)
                .unwrap_or_default()
        },
        target: if gap {
            "core_log_archive"
        } else {
            object.and_then(|v| v["target"].as_str()).unwrap_or("")
        },
        message: if gap {
            Cow::Owned(format!(
                "Core console capture dropped {} records",
                object.unwrap()["dropped"]
            ))
        } else {
            Cow::Borrowed(
                object
                    .and_then(|v| {
                        if core {
                            v["message"].as_str()
                        } else {
                            v["fields"]["message"].as_str()
                        }
                    })
                    .unwrap_or(raw),
            )
        },
        unparsed: object.is_none(),
        truncated: core && object.is_some_and(|v| v["truncated"].as_bool() == Some(true)),
    }
}
