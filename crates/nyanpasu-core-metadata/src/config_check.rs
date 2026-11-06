//! Locations reported by a core's failed configuration check.

/// The verified part of a kernel diagnostic, before application path mapping.
/// Proxy and rule indices are zero-based. Groups are identified by name because
/// kernels may reorder them while checking dependencies.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, specta::Type)]
#[non_exhaustive]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreConfigCheckDiagnostic {
    MissingGroupMember {
        group: String,
        member: String,
    },
    Proxy {
        index: u32,
    },
    Rule {
        index: u32,
    },
    /// Captured output for display only. Its text must never drive decisions.
    Unrecognized {
        raw: String,
    },
}

#[cfg(test)]
mod tests {
    use super::CoreConfigCheckDiagnostic;

    #[test]
    fn diagnostic_wire_shapes_roundtrip() {
        let cases = [
            (
                CoreConfigCheckDiagnostic::MissingGroupMember {
                    group: "自动优选".into(),
                    member: "X VA".into(),
                },
                serde_json::json!({"kind":"missing_group_member","group":"自动优选","member":"X VA"}),
            ),
            (
                CoreConfigCheckDiagnostic::Proxy { index: 2 },
                serde_json::json!({"kind":"proxy","index":2}),
            ),
            (
                CoreConfigCheckDiagnostic::Rule { index: 3 },
                serde_json::json!({"kind":"rule","index":3}),
            ),
            (
                CoreConfigCheckDiagnostic::Unrecognized {
                    raw: "unknown output".into(),
                },
                serde_json::json!({"kind":"unrecognized","raw":"unknown output"}),
            ),
        ];
        for (diagnostic, wire) in cases {
            assert_eq!(serde_json::to_value(&diagnostic).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<CoreConfigCheckDiagnostic>(wire).unwrap(),
                diagnostic
            );
        }
    }
}
