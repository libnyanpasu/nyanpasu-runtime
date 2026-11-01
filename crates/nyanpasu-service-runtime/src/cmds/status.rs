use std::{borrow::Cow, future::Future, io, time::Duration};

use super::CommandError;
use crate::consts::{APP_NAME, APP_VERSION};
use nyanpasu_ipc::{
    api::status::StatusResBody,
    client::shortcuts::Client,
    types::{ServiceStatus, StatusInfo},
};
use tokio::time::timeout;

#[derive(Debug, clap::Args)]
pub struct StatusCommand {
    /// Output the result in JSON format
    #[clap(long, default_value = "false")]
    json: bool,

    /// Include pending and unknown service states in the output
    #[clap(long, default_value = "false")]
    detailed: bool,

    /// Probe IPC only; a failed probe exits with an error
    #[clap(long, default_value = "false")]
    skip_service_check: bool,

    /// Report the service state through the process exit code as well:
    /// running, pending or unknown exits 0, stopped exits 102, not installed exits 100
    #[clap(long, default_value = "false")]
    exit_code: bool,
}

// TODO: impl the health check if service is running
// such as data dir, config dir, core status.
pub async fn status(ctx: StatusCommand) -> Result<(), CommandError> {
    let client = Client::service_default();
    status_with(ctx, read_service_status, || async {
        client
            .status()
            .await
            .map_err(|error| anyhow::Error::from(error).into())
    })
    .await
}

async fn read_service_status() -> Result<ServiceStatus, CommandError> {
    tokio::task::spawn_blocking(|| {
        #[cfg(windows)]
        {
            crate::utils::windows_service::query_status()
                .map_err(|error| anyhow::Error::from(error).into())
        }
        #[cfg(not(windows))]
        {
            let manager = crate::utils::get_service_manager()?;
            let label = crate::consts::SERVICE_LABEL.parse()?;
            Ok(map_service_status(crate::utils::service::status(
                manager.as_ref(),
                &label,
            )?))
        }
    })
    .await?
}

async fn status_with<R, P>(
    ctx: StatusCommand,
    read_status: impl FnMut() -> R,
    probe: impl FnOnce() -> P,
) -> Result<(), CommandError>
where
    R: Future<Output = Result<ServiceStatus, CommandError>>,
    P: Future<Output = Result<StatusResBody<'static>, CommandError>>,
{
    let info = observe_status(ctx.skip_service_check, read_status, probe).await?;
    let result = if ctx.exit_code {
        match info.status {
            ServiceStatus::Running
            | ServiceStatus::StartPending
            | ServiceStatus::StopPending
            | ServiceStatus::Unknown => Ok(()),
            ServiceStatus::Stopped => Err(CommandError::ServiceAlreadyStopped),
            ServiceStatus::NotInstalled => Err(CommandError::ServiceNotInstalled),
        }
    } else {
        Ok(())
    };
    println!("{}", format_status(&ctx, info)?);
    result
}

async fn observe_status<R, P>(
    skip_service_check: bool,
    mut read_status: impl FnMut() -> R,
    probe: impl FnOnce() -> P,
) -> Result<StatusInfo<'static>, CommandError>
where
    R: Future<Output = Result<ServiceStatus, CommandError>>,
    P: Future<Output = Result<StatusResBody<'static>, CommandError>>,
{
    let mut status = if skip_service_check {
        ServiceStatus::Running
    } else {
        read_status().await?
    };
    let mut server = None;
    if status == ServiceStatus::Running {
        let response = match timeout(Duration::from_secs(3), probe()).await {
            Ok(result) => result,
            Err(error) => Err(io::Error::new(io::ErrorKind::TimedOut, error).into()),
        };
        if skip_service_check {
            server = Some(response?);
        } else {
            status = read_status().await?;
            match response {
                Ok(reply) if status == ServiceStatus::Running => server = Some(reply),
                Ok(_) => {}
                Err(error) => tracing::debug!(%error, "failed to get server status"),
            }
        }
    }
    Ok(StatusInfo {
        name: Cow::Borrowed(APP_NAME),
        version: Cow::Borrowed(APP_VERSION),
        status,
        server,
    })
}

fn format_status(ctx: &StatusCommand, mut info: StatusInfo<'_>) -> Result<String, CommandError> {
    if info.status != ServiceStatus::Running {
        info.server = None;
    }
    if !ctx.detailed
        && matches!(
            info.status,
            ServiceStatus::StartPending | ServiceStatus::StopPending | ServiceStatus::Unknown
        )
    {
        info.status = ServiceStatus::Running;
    }
    if ctx.json {
        Ok(simd_json::serde::to_string_pretty(&info)?)
    } else {
        Ok(format!("{info:#?}"))
    }
}

#[cfg(not(windows))]
fn map_service_status(status: service_manager::ServiceStatus) -> ServiceStatus {
    match status {
        service_manager::ServiceStatus::NotInstalled => ServiceStatus::NotInstalled,
        service_manager::ServiceStatus::Stopped(_) => ServiceStatus::Stopped,
        service_manager::ServiceStatus::Running => ServiceStatus::Running,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, future::ready};
    use tokio::sync::oneshot;

    fn command() -> StatusCommand {
        StatusCommand {
            json: true,
            detailed: false,
            skip_service_check: false,
            exit_code: false,
        }
    }

    fn info(status: ServiceStatus) -> StatusInfo<'static> {
        StatusInfo {
            name: Cow::Borrowed(APP_NAME),
            version: Cow::Borrowed(APP_VERSION),
            status,
            server: Some(server_reply()),
        }
    }

    fn server_reply() -> StatusResBody<'static> {
        serde_json::from_value(serde_json::json!({
            "version": "2.0.2",
            "core_infos": {"type": null, "state": {"Stopped": null}, "state_changed_at": 0, "config_path": null},
            "runtime_infos": {"service_data_dir": "", "service_config_dir": "", "nyanpasu_config_dir": "", "nyanpasu_data_dir": ""}
        })).unwrap()
    }

    fn no_probe() -> std::future::Ready<Result<StatusResBody<'static>, CommandError>> {
        panic!("IPC must not be probed for this SCM state")
    }

    #[cfg(not(windows))]
    #[test]
    fn service_status_maps_onto_the_ipc_status() {
        for (status, expected) in [
            (
                service_manager::ServiceStatus::NotInstalled,
                ServiceStatus::NotInstalled,
            ),
            (
                service_manager::ServiceStatus::Stopped(None),
                ServiceStatus::Stopped,
            ),
            (
                service_manager::ServiceStatus::Stopped(Some("details".into())),
                ServiceStatus::Stopped,
            ),
            (
                service_manager::ServiceStatus::Running,
                ServiceStatus::Running,
            ),
        ] {
            assert_eq!(map_service_status(status), expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn running_with_ipc_timeout_stays_running() {
        let (_send, receive) = oneshot::channel::<StatusResBody<'static>>();
        let reads = Cell::new(0);
        let started = tokio::time::Instant::now();
        let info = observe_status(
            false,
            || {
                reads.set(reads.get() + 1);
                ready(Ok(ServiceStatus::Running))
            },
            || async { Ok(receive.await.unwrap()) },
        )
        .await
        .unwrap();
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_secs(3)
        );
        assert_eq!(info.status, ServiceStatus::Running);
        assert!(info.server.is_none());
        assert_eq!(reads.get(), 2);
    }

    #[tokio::test]
    async fn running_with_ipc_failure_stays_running() {
        let info = observe_status(
            false,
            || ready(Ok(ServiceStatus::Running)),
            || ready(Err(io::Error::from(io::ErrorKind::ConnectionRefused).into())),
        )
        .await
        .unwrap();
        assert_eq!(info.status, ServiceStatus::Running);
        assert!(info.server.is_none());
    }

    #[tokio::test]
    async fn running_with_ipc_reply_keeps_server() {
        let info = observe_status(
            false,
            || ready(Ok(ServiceStatus::Running)),
            || ready(Ok(server_reply())),
        )
        .await
        .unwrap();
        assert_eq!(info.status, ServiceStatus::Running);
        assert!(info.server.is_some());
    }

    #[tokio::test]
    async fn start_and_stop_pending_are_preserved() {
        for state in [
            ServiceStatus::StartPending,
            ServiceStatus::StopPending,
            ServiceStatus::Unknown,
        ] {
            let reads = Cell::new(0);
            let info = observe_status(
                false,
                || {
                    reads.set(reads.get() + 1);
                    ready(Ok(state))
                },
                no_probe,
            )
            .await
            .unwrap();
            assert_eq!(info.status, state);
            assert!(info.server.is_none());
            assert_eq!(reads.get(), 1);
        }
    }

    #[tokio::test]
    async fn scm_transition_discards_stale_ipc_reply() {
        for state in [
            ServiceStatus::Stopped,
            ServiceStatus::NotInstalled,
            ServiceStatus::StartPending,
            ServiceStatus::StopPending,
            ServiceStatus::Unknown,
        ] {
            let mut states = [ServiceStatus::Running, state].into_iter();
            let (send, receive) = oneshot::channel();
            let observation = observe_status(
                false,
                || ready(Ok(states.next().unwrap())),
                || async { Ok(receive.await.unwrap()) },
            );
            tokio::pin!(observation);
            assert!(futures_util::poll!(&mut observation).is_pending());
            send.send(server_reply()).unwrap();
            let info = observation.await.unwrap();
            assert_eq!(info.status, state);
            assert!(info.server.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn scm_is_rechecked_after_ipc_timeout_or_failure() {
        for pending in [false, true] {
            let mut states = [ServiceStatus::Running, ServiceStatus::StopPending].into_iter();
            let (_send, receive) = oneshot::channel::<StatusResBody<'static>>();
            let info = observe_status(
                false,
                || ready(Ok(states.next().unwrap())),
                || async {
                    if pending {
                        Ok(receive.await.unwrap())
                    } else {
                        Err(io::Error::from(io::ErrorKind::ConnectionRefused).into())
                    }
                },
            )
            .await
            .unwrap();
            assert_eq!(info.status, ServiceStatus::StopPending);
            assert!(info.server.is_none());
        }
    }

    #[tokio::test]
    async fn access_denied_is_not_not_installed() {
        for fail_on_read in [1, 2] {
            let reads = Cell::new(0);
            let result = observe_status(
                false,
                || {
                    reads.set(reads.get() + 1);
                    ready(if reads.get() == fail_on_read {
                        Err(io::Error::from(io::ErrorKind::PermissionDenied).into())
                    } else {
                        Ok(ServiceStatus::Running)
                    })
                },
                || {
                    assert_eq!(fail_on_read, 2);
                    ready(Ok(server_reply()))
                },
            )
            .await;
            assert!(matches!(result, Err(CommandError::Io(error))
                if error.kind() == io::ErrorKind::PermissionDenied));
        }
    }

    #[derive(Debug, serde::Deserialize, PartialEq)]
    #[serde(rename_all = "snake_case")]
    enum LegacyServiceStatus {
        NotInstalled,
        Stopped,
        Running,
    }

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LegacyStatusInfo {
        name: String,
        version: String,
        status: LegacyServiceStatus,
        server: Option<StatusResBody<'static>>,
    }

    #[test]
    fn legacy_json_never_emits_new_status_variants() {
        for (state, expected) in [
            (
                ServiceStatus::NotInstalled,
                LegacyServiceStatus::NotInstalled,
            ),
            (ServiceStatus::Stopped, LegacyServiceStatus::Stopped),
            (ServiceStatus::Running, LegacyServiceStatus::Running),
            (ServiceStatus::StartPending, LegacyServiceStatus::Running),
            (ServiceStatus::StopPending, LegacyServiceStatus::Running),
            (ServiceStatus::Unknown, LegacyServiceStatus::Running),
        ] {
            let json = format_status(&command(), info(state)).unwrap();
            let decoded: LegacyStatusInfo = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.name, APP_NAME);
            assert_eq!(decoded.version, APP_VERSION);
            assert_eq!(decoded.status, expected);
            assert_eq!(decoded.server.is_some(), state == ServiceStatus::Running);
        }
    }

    #[test]
    fn detailed_json_roundtrips_pending_states() {
        for state in [
            ServiceStatus::StartPending,
            ServiceStatus::StopPending,
            ServiceStatus::Unknown,
        ] {
            let ctx = StatusCommand {
                detailed: true,
                ..command()
            };
            let json = format_status(&ctx, info(state)).unwrap();
            let decoded: StatusInfo<'static> = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.status, state);
            assert!(decoded.server.is_none());
        }
    }

    #[tokio::test]
    async fn exit_code_and_output_flags_preserve_confirmed_scm_states() {
        for state in [
            ServiceStatus::NotInstalled,
            ServiceStatus::Stopped,
            ServiceStatus::Running,
            ServiceStatus::StartPending,
            ServiceStatus::StopPending,
            ServiceStatus::Unknown,
        ] {
            for exit_code in [false, true] {
                for detailed in [false, true] {
                    for json in [false, true] {
                        let result = status_with(
                            StatusCommand {
                                exit_code,
                                detailed,
                                json,
                                ..command()
                            },
                            || ready(Ok(state)),
                            || {
                                assert_eq!(state, ServiceStatus::Running);
                                ready(Err(io::Error::from(io::ErrorKind::ConnectionRefused).into()))
                            },
                        )
                        .await;
                        match (exit_code, state) {
                            (true, ServiceStatus::Stopped) => {
                                assert!(matches!(result, Err(CommandError::ServiceAlreadyStopped)))
                            }
                            (true, ServiceStatus::NotInstalled) => {
                                assert!(matches!(result, Err(CommandError::ServiceNotInstalled)))
                            }
                            _ => assert!(result.is_ok(), "state {state:?}: {result:?}"),
                        }
                    }
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn skip_service_check_reports_probe_failures_without_stopped_exit_code() {
        for exit_code in [false, true] {
            for detailed in [false, true] {
                for json in [false, true] {
                    for pending in [false, true] {
                        let (_send, receive) = oneshot::channel::<StatusResBody<'static>>();
                        let result = status_with(
                            StatusCommand {
                                skip_service_check: true,
                                exit_code,
                                detailed,
                                json,
                            },
                            || -> std::future::Ready<Result<ServiceStatus, CommandError>> {
                                panic!("SCM must be skipped")
                            },
                            || async {
                                if pending {
                                    Ok(receive.await.unwrap())
                                } else {
                                    Err(io::Error::from(io::ErrorKind::ConnectionRefused).into())
                                }
                            },
                        )
                        .await;
                        assert!(matches!(result, Err(CommandError::Io(error))
                            if error.kind() == if pending { io::ErrorKind::TimedOut } else { io::ErrorKind::ConnectionRefused }));
                    }
                    let info = observe_status(
                        true,
                        || -> std::future::Ready<Result<ServiceStatus, CommandError>> {
                            panic!("SCM must be skipped")
                        },
                        || ready(Ok(server_reply())),
                    )
                    .await
                    .unwrap();
                    assert_eq!(info.status, ServiceStatus::Running);
                    assert!(info.server.is_some());
                    status_with(
                        StatusCommand {
                            skip_service_check: true,
                            exit_code,
                            detailed,
                            json,
                        },
                        || -> std::future::Ready<Result<ServiceStatus, CommandError>> {
                            panic!("SCM must be skipped")
                        },
                        || ready(Ok(server_reply())),
                    )
                    .await
                    .unwrap();
                }
            }
        }
    }

    #[test]
    fn detailed_status_is_opt_in_and_flags_can_be_combined() {
        use crate::cmds::{Cli, Commands};
        use clap::Parser;
        let cli = Cli::try_parse_from(["nyanpasu-service", "status", "--json"]).unwrap();
        let Some(Commands::Status(ctx)) = cli.command else {
            panic!("expected status command")
        };
        assert!(!ctx.detailed);
        let cli = Cli::try_parse_from([
            "nyanpasu-service",
            "status",
            "--json",
            "--detailed",
            "--skip-service-check",
            "--exit-code",
        ])
        .unwrap();
        let Some(Commands::Status(ctx)) = cli.command else {
            panic!("expected status command")
        };
        assert!(ctx.json && ctx.detailed && ctx.skip_service_check && ctx.exit_code);
    }
}
