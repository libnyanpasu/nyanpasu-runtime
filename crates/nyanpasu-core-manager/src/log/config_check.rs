//! Recognizes only audited kernel check records, before log continuations join.

use camino::Utf8Path;
use nyanpasu_core_metadata::CoreConfigCheckDiagnostic;

use super::{CapturedOutput, LogLevel, LogParser, strip_ansi, summarize_output};
use crate::kind::CoreKind;

pub(crate) struct ConfigCheckFailure {
    pub message: String,
    pub diagnostic: CoreConfigCheckDiagnostic,
}

pub(crate) fn parse_config_check_output(
    kind: CoreKind,
    output: CapturedOutput<'_>,
    checked_config_path: &Utf8Path,
) -> ConfigCheckFailure {
    let diagnostic = recognize_output(kind, output, checked_config_path).unwrap_or_else(|| {
        // Capture keeps each stream's text, but not their interleaving.
        let raw = match (output.stdout.is_empty(), output.stderr.is_empty()) {
            (false, false) => format!("{}\n{}", output.stdout, output.stderr),
            _ => format!("{}{}", output.stdout, output.stderr),
        };
        CoreConfigCheckDiagnostic::Unrecognized { raw }
    });
    ConfigCheckFailure {
        message: summarize_output(kind, output),
        diagnostic,
    }
}

fn recognize_output(
    kind: CoreKind,
    output: CapturedOutput<'_>,
    checked_config_path: &Utf8Path,
) -> Option<CoreConfigCheckDiagnostic> {
    if !matches!(kind, CoreKind::Mihomo | CoreKind::ClashPremium) {
        return None;
    }
    let mut parser = LogParser::new(kind, 0);
    let observed_at = chrono::Local::now().fixed_offset();
    let trailer = format!("configuration file {checked_config_path} test failed");
    let path_suffix = format!(" path={checked_config_path}");
    let mut diagnostic = None;
    let mut saw_error = false;
    for line in output.stdout.lines().chain(output.stderr.lines()) {
        let line = strip_ansi(line.to_owned());
        if line.is_empty() {
            continue;
        }
        let Some(record) = parser.parse_header(&line, observed_at) else {
            if kind == CoreKind::Mihomo && line == trailer {
                continue;
            }
            return None;
        };
        if record.level < LogLevel::Error {
            continue;
        }
        if saw_error || record.level != LogLevel::Error {
            return None;
        }
        saw_error = true;
        let cause = match kind {
            CoreKind::Mihomo => record.message.as_str(),
            CoreKind::ClashPremium if record.target.as_deref() == Some("Config") => record
                .message
                .strip_prefix("configuration file test failed error=")?
                .strip_suffix(&path_suffix)?,
            _ => return None,
        };
        diagnostic = parse_go_cause(cause);
    }
    diagnostic
}

fn parse_go_cause(cause: &str) -> Option<CoreConfigCheckDiagnostic> {
    if let Some(rest) = cause.strip_prefix("proxy group[") {
        let (index, rest) = rest.split_once("]: ")?;
        parse_index(index)?;
        let rest = rest.strip_suffix("' not found")?;
        let mut splits = rest.match_indices(": '").filter_map(|(offset, separator)| {
            let group = &rest[..offset];
            let member = &rest[offset + separator.len()..];
            (!group.is_empty() && !member.is_empty()).then_some((group, member))
        });
        let (group, member) = splits.next()?;
        if splits.next().is_some() {
            return None;
        }
        return Some(CoreConfigCheckDiagnostic::MissingGroupMember {
            group: group.to_owned(),
            member: member.to_owned(),
        });
    }
    if let Some(rest) = cause.strip_prefix("proxy ") {
        let (index, detail) = rest.split_once(": ")?;
        if !detail.is_empty() {
            return Some(CoreConfigCheckDiagnostic::Proxy {
                index: parse_index(index)?,
            });
        }
    }
    if let Some(rest) = cause.strip_prefix("rules[") {
        let (index, rest) = rest.split_once("] [")?;
        let (rule, detail) = rest.rsplit_once("] error: ")?;
        if !rule.is_empty() && !detail.is_empty() {
            return Some(CoreConfigCheckDiagnostic::Rule {
                index: parse_index(index)?,
            });
        }
    }
    None
}

fn parse_index(index: &str) -> Option<u32> {
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    index.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(kind: &str, case: &str, stream: &str) -> String {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        std::fs::read_to_string(
            root.join("tests/fixtures/config-check")
                .join(kind)
                .join(format!("{case}.{stream}")),
        )
        .unwrap()
    }

    fn fixture_path(stdout: &str) -> &Utf8Path {
        let path = stdout
            .lines()
            .find_map(|line| {
                line.strip_prefix("configuration file ")
                    .and_then(|rest| rest.strip_suffix(" test failed"))
                    .or_else(|| line.rsplit_once(" path=").map(|(_, path)| path))
            })
            .expect("the Go capture includes its invocation path");
        Utf8Path::new(path)
    }

    fn mihomo_line(cause: &str) -> String {
        format!(
            "time=\"2026-11-05T21:38:41-08:00\" level=error msg={}\n",
            serde_json::to_string(cause).unwrap()
        )
    }

    fn diagnostic(kind: CoreKind, stdout: &str, stderr: &str) -> CoreConfigCheckDiagnostic {
        parse_config_check_output(
            kind,
            CapturedOutput { stdout, stderr },
            Utf8Path::new("/fixtures/check.yaml"),
        )
        .diagnostic
    }

    fn assert_unrecognized(kind: CoreKind, stdout: &str, stderr: &str) {
        let expected = if !stdout.is_empty() && !stderr.is_empty() {
            format!("{stdout}\n{stderr}")
        } else {
            format!("{stdout}{stderr}")
        };
        assert_eq!(
            diagnostic(kind, stdout, stderr),
            CoreConfigCheckDiagnostic::Unrecognized { raw: expected }
        );
    }

    #[test]
    fn audited_go_fixtures_have_typed_locations() {
        for (directory, kind) in [
            ("mihomo", CoreKind::Mihomo),
            ("mihomo-alpha", CoreKind::Mihomo),
            ("clash", CoreKind::ClashPremium),
        ] {
            for (case, expected) in [
                (
                    "a_missing_proxy",
                    CoreConfigCheckDiagnostic::MissingGroupMember {
                        group: "G".into(),
                        member: "MISSING".into(),
                    },
                ),
                (
                    "a_cjk_spaces",
                    CoreConfigCheckDiagnostic::MissingGroupMember {
                        group: "自动优选".into(),
                        member: "X VA".into(),
                    },
                ),
                (
                    "a_index_after_sort",
                    CoreConfigCheckDiagnostic::MissingGroupMember {
                        group: "Second".into(),
                        member: "MISSING".into(),
                    },
                ),
                (
                    "b_missing_provider",
                    CoreConfigCheckDiagnostic::MissingGroupMember {
                        group: "G".into(),
                        member: "MISSING_PROVIDER".into(),
                    },
                ),
                (
                    "e_unknown_proxy_type",
                    CoreConfigCheckDiagnostic::Proxy { index: 0 },
                ),
                (
                    "e_missing_cipher",
                    CoreConfigCheckDiagnostic::Proxy { index: 0 },
                ),
                (
                    "c_missing_rule_target",
                    CoreConfigCheckDiagnostic::Rule { index: 0 },
                ),
                (
                    "d_unknown_rule",
                    CoreConfigCheckDiagnostic::Rule { index: 0 },
                ),
                (
                    "d_malformed_rule",
                    CoreConfigCheckDiagnostic::Rule { index: 0 },
                ),
            ] {
                let stdout = fixture(directory, case, "stdout");
                let stderr = fixture(directory, case, "stderr");
                let failure = parse_config_check_output(
                    kind,
                    CapturedOutput {
                        stdout: &stdout,
                        stderr: &stderr,
                    },
                    fixture_path(&stdout),
                );
                assert_eq!(failure.diagnostic, expected, "{directory}/{case}");
                assert_eq!(
                    failure.message,
                    summarize_output(
                        kind,
                        CapturedOutput {
                            stdout: &stdout,
                            stderr: &stderr
                        }
                    )
                );
            }
        }
    }

    #[test]
    fn ambiguous_and_unsupported_audited_errors_fall_back() {
        for (directory, kind) in [
            ("mihomo", CoreKind::Mihomo),
            ("mihomo-alpha", CoreKind::Mihomo),
            ("clash", CoreKind::ClashPremium),
        ] {
            for case in [
                "a_delimiters",
                "h_bad_yaml",
                "f_duplicate_groups",
                "g_group_cycle",
            ] {
                let stdout = fixture(directory, case, "stdout");
                let stderr = fixture(directory, case, "stderr");
                let result = parse_config_check_output(
                    kind,
                    CapturedOutput {
                        stdout: &stdout,
                        stderr: &stderr,
                    },
                    fixture_path(&stdout),
                );
                assert!(
                    matches!(
                        result.diagnostic,
                        CoreConfigCheckDiagnostic::Unrecognized { .. }
                    ),
                    "{directory}/{case}"
                );
            }
        }
    }

    #[test]
    fn rust_families_always_fall_back() {
        for (directory, kind) in [
            ("clash-rs-official", CoreKind::ClashRust),
            ("clash-rs-alpha-official", CoreKind::ClashRust),
            ("meow", CoreKind::Meow),
            ("meow-alpha", CoreKind::Meow),
        ] {
            for case in [
                "a_missing_proxy",
                "c_missing_rule_target",
                "e_unknown_proxy_type",
                "f_duplicate_groups",
            ] {
                assert_unrecognized(
                    kind,
                    &fixture(directory, case, "stdout"),
                    &fixture(directory, case, "stderr"),
                );
            }
            assert_unrecognized(kind, &mihomo_line("proxy 0: invalid proxy"), "");
        }
    }

    #[test]
    fn incomplete_shapes_and_unknown_wrappers_fall_back() {
        for cause in [
            "proxy -1: invalid",
            "proxy +1: invalid",
            "proxy 4294967296: invalid",
            "proxy : invalid",
            "proxy 1: ",
            "proxy[1]: invalid",
            "proxy group[0]: G: 'MISSING'",
            "proxy group[0]: : 'MISSING' not found",
            "proxy group[0]: G: '' not found",
            "rules[-1] [MATCH,DIRECT] error: invalid",
            "rules[4294967296] [MATCH,DIRECT] error: invalid",
            "rules[1] [] error: invalid",
            "rules[1] [MATCH,DIRECT] error: ",
            "Parse config error: proxy 0: invalid",
            "yaml: line 2: invalid syntax",
        ] {
            assert_unrecognized(CoreKind::Mihomo, &mihomo_line(cause), "");
        }
        for stdout in [
            "",
            "proxy 0: invalid",
            "time=\"t\" level=error msg=\"proxy 0: invalid",
            "21:38:41 ERR [Other] configuration file test failed error=proxy 0: invalid path=/fixtures/check.yaml",
        ] {
            assert_unrecognized(CoreKind::Mihomo, stdout, "");
            assert_unrecognized(CoreKind::ClashPremium, stdout, "");
        }
    }

    #[test]
    fn competing_records_never_select_a_location() {
        let first = mihomo_line("proxy 1: invalid");
        for second in [
            mihomo_line("rules[2] [MATCH] error: invalid"),
            mihomo_line("unrecognized error"),
            first.replace("level=error", "level=fatal"),
        ] {
            assert_unrecognized(CoreKind::Mihomo, &format!("{first}{second}"), "");
            assert_unrecognized(CoreKind::Mihomo, &first, &second);
        }
        assert_unrecognized(CoreKind::Mihomo, &first, "unformatted stderr\n");
        assert_unrecognized(
            CoreKind::Mihomo,
            &first.replace("level=error", "level=warning"),
            "",
        );
        let premium = "21:38:41 ERR [Config] configuration file test failed error=proxy 0: invalid path=/fixtures/check.yaml\n";
        assert_unrecognized(CoreKind::ClashPremium, &format!("{premium}{premium}"), "");
    }

    #[test]
    fn decodes_logfmt_and_does_not_classify_joined_or_truncated_summaries() {
        let group = format!("自动优选 \\\"{}", "组".repeat(6000));
        let line = mihomo_line(&format!("proxy group[4]: {group}: 'X VA' not found"));
        let output = format!("{line}configuration file /fixtures/check.yaml test failed\n");
        assert_eq!(
            diagnostic(CoreKind::Mihomo, &output, ""),
            CoreConfigCheckDiagnostic::MissingGroupMember {
                group,
                member: "X VA".into()
            }
        );
        assert_eq!(
            diagnostic(CoreKind::Mihomo, "", &mihomo_line("proxy 12: invalid")),
            CoreConfigCheckDiagnostic::Proxy { index: 12 }
        );
    }

    #[test]
    fn premium_path_boundary_uses_the_exact_invocation_path() {
        let cause = "proxy group[0]: G path=decoy: 'X' not found";
        let output = format!(
            "21:38:41 ERR [Config] configuration file test failed error={cause} path=/fixtures/check.yaml\n"
        );
        assert_eq!(
            diagnostic(CoreKind::ClashPremium, &output, ""),
            CoreConfigCheckDiagnostic::MissingGroupMember {
                group: "G path=decoy".into(),
                member: "X".into()
            }
        );
        assert_unrecognized(
            CoreKind::ClashPremium,
            &output.replace("path=/fixtures/check.yaml", "path=/other/check.yaml"),
            "",
        );
    }

    #[test]
    fn issue_5922_windows_trailer_does_not_enter_the_group_fields() {
        let path = Utf8Path::new(r"C:\WINDOWS\TEMP\nyanpasu-core-check-example\check.yaml");
        let stdout = format!(
            "{}configuration file {path} test failed\n",
            mihomo_line("proxy group[0]: 自动优选: 'X VA' not found")
        );
        let failure = parse_config_check_output(
            CoreKind::Mihomo,
            CapturedOutput {
                stdout: &stdout,
                stderr: "",
            },
            path,
        );
        assert_eq!(
            failure.diagnostic,
            CoreConfigCheckDiagnostic::MissingGroupMember {
                group: "自动优选".into(),
                member: "X VA".into()
            }
        );
    }
}
