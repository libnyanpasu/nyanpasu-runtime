//! The desktop execution mechanism: supervised child processes with PID-file
//! death confirmation, fronted by the [`RuntimeBackend`] boundary.

use std::{sync::Arc, time::Duration};

use nyanpasu_utils::process::AtomicStopStrategy;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    epoch::Epoch,
    error::{CoreConfigCheckDiagnostic, Error},
    instance::Instance,
    probe::{ProbeHandle, ProbePhase, ProbeResult},
    spec::{InstanceSpec, ResolvedController},
    state::InstanceStatus,
};

use super::{BoxFuture, RuntimeBackend, RuntimeInstance, RuntimeLaunchRequest};

/// Probe wiring for instances the default backend launches. Collected by the
/// manager builder; a custom backend owns its own probing instead.
#[derive(Clone, Default)]
pub(crate) struct ProbePlan {
    pub(crate) controller_access: Option<std::sync::Arc<dyn crate::ControllerAccess>>,
    pub(crate) readiness: Option<ProbeHandle>,
    pub(crate) liveness: Option<ProbeHandle>,
    pub(crate) liveness_with_readiness: bool,
}

pub(crate) struct ProcessRuntimeBackend {
    probes: ProbePlan,
    cancel_token: CancellationToken,
    native_store: Option<std::sync::Arc<dyn crate::native_store::NativeStore>>,
    windows_shutdown_helper: Option<std::path::PathBuf>,
    stop_strategy: Option<Arc<AtomicStopStrategy>>,
}

impl ProcessRuntimeBackend {
    pub(crate) fn new(
        probes: ProbePlan,
        cancel_token: CancellationToken,
        native_store: Option<std::sync::Arc<dyn crate::native_store::NativeStore>>,
        windows_shutdown_helper: Option<std::path::PathBuf>,
        stop_strategy: Option<Arc<AtomicStopStrategy>>,
    ) -> Self {
        Self {
            probes,
            cancel_token,
            native_store,
            windows_shutdown_helper,
            stop_strategy,
        }
    }
}

impl RuntimeBackend for ProcessRuntimeBackend {
    fn launch(
        &self,
        request: RuntimeLaunchRequest,
    ) -> BoxFuture<'_, Result<Box<dyn RuntimeInstance>, Error>> {
        Box::pin(async move {
            let RuntimeLaunchRequest {
                effective_spec,
                capabilities,
                epoch,
                controller,
                log_tx,
            } = request;
            let descriptor = if matches!(controller.host, crate::Host::NamedPipe(_))
                && capabilities.contains(crate::Feature::NamedPipeSecurityDescriptor)
            {
                self.probes
                    .controller_access
                    .as_ref()
                    .and_then(|access| access.pipe_security_descriptor())
                    .map(str::to_owned)
            } else {
                None
            };
            let lease = self
                .native_store
                .as_ref()
                .map(|store| store.acquire(effective_spec.core.kind))
                .transpose()?;
            let mode = if descriptor.is_some() {
                crate::ControllerAuthorization::Core
            } else {
                crate::ControllerAuthorization::Host
            };
            let mut builder = Instance::builder(
                effective_spec,
                epoch,
                controller.clone(),
                self.cancel_token.clone(),
            )
            .log_sender(log_tx)
            .pipe_security_descriptor(descriptor)
            .native_store(
                lease,
                self.native_store
                    .as_ref()
                    .map(|store| store.data_dir().to_owned()),
            );
            builder = builder.windows_shutdown_helper(self.windows_shutdown_helper.clone());
            if let Some(strategy) = &self.stop_strategy {
                builder = builder.stop_strategy(Arc::clone(strategy));
            }
            if let Some(access) = &self.probes.controller_access {
                let readiness = match self.probes.readiness.clone() {
                    Some(probe) => probe,
                    None => ProbeHandle::new(
                        "controller-version",
                        crate::ControllerVersionProbe::new(&controller)?,
                    ),
                };
                let liveness = if self.probes.liveness_with_readiness {
                    readiness.clone()
                } else {
                    self.probes.liveness.clone().unwrap_or_else(|| {
                        ProbeHandle::from_fn("process-alive", |_| async { ProbeResult::Healthy })
                    })
                };
                let wrap = |inner| {
                    ProbeHandle::new(
                        "authorized-controller",
                        crate::controller_access::AuthorizedProbe {
                            access: access.clone(),
                            inner,
                            mode,
                        },
                    )
                };
                builder = builder
                    .readiness_probe(wrap(readiness))
                    .liveness_probe(wrap(liveness));
            } else {
                if let Some(probe) = self.probes.readiness.clone() {
                    builder = builder.readiness_probe(probe);
                }
                if let Some(probe) = self.probes.liveness.clone() {
                    builder = builder.liveness_probe(probe);
                }
                if self.probes.liveness_with_readiness {
                    builder = builder.liveness_with_readiness_probe();
                }
            }
            let instance = builder.spawn().await?;
            Ok(Box::new(instance) as Box<dyn RuntimeInstance>)
        })
    }

    fn check_config<'a>(&'a self, spec: &'a InstanceSpec) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            if let Some(store) = &self.native_store {
                let home = store.check_home()?;
                let mut isolated = spec.clone();
                isolated.working_dir =
                    camino::Utf8PathBuf::from_path_buf(home.path().to_owned())
                        .map_err(|_| Error::InvalidConfig("temporary path is not UTF-8".into()))?;
                let mut document: serde_yaml_ng::Mapping =
                    serde_yaml_ng::from_slice(&tokio::fs::read(&spec.config_path).await?)?;
                crate::native_store::rewrite_paths(spec.core.kind, &mut document, store.data_dir());
                isolated.config_path = isolated.working_dir.join("check.yaml");
                tokio::fs::write(&isolated.config_path, serde_yaml_ng::to_string(&document)?)
                    .await?;
                crate::kind::check_config_with_source(&isolated, store.data_dir())
                    .await
                    .map_err(|error| replace_check_path(error, &isolated.config_path))
            } else {
                crate::kind::check_config(spec).await
            }
        })
    }
}

fn replace_check_path(mut error: Error, path: &camino::Utf8Path) -> Error {
    if let Error::ConfigCheckFailed {
        message,
        config_check_diagnostic,
    } = &mut error
    {
        *message = message.replace(path.as_str(), "<checked-config>");
        if let Some(CoreConfigCheckDiagnostic::Unrecognized { raw }) = config_check_diagnostic {
            *raw = raw.replace(path.as_str(), "<checked-config>");
        }
    }
    error
}

impl RuntimeInstance for Instance {
    fn epoch(&self) -> Epoch {
        Instance::epoch(self)
    }

    fn spec(&self) -> &InstanceSpec {
        Instance::spec(self)
    }

    fn controller(&self) -> &ResolvedController {
        Instance::controller(self)
    }

    fn pid(&self) -> Option<u32> {
        Instance::pid(self)
    }

    fn state(&self) -> watch::Receiver<InstanceStatus> {
        Instance::state(self)
    }

    fn wait_ready<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Instance::wait_ready(self))
    }

    fn probe_now<'a>(&'a self, phase: ProbePhase) -> BoxFuture<'a, ProbeResult> {
        Box::pin(Instance::probe_now(self, phase))
    }

    fn stop_and_confirm_dead(
        self: Box<Self>,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(Instance::stop_and_confirm_dead(*self, timeout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8Path;

    #[test]
    fn replaces_only_the_exact_owned_check_path_in_message_and_fallback() {
        for path in ["/tmp/检查 dir/check.yaml", r"C:\TEMP\检查 dir\check.yaml"] {
            let other = "/unrelated/check.yaml";
            let message = format!("{path}\n{other}\n{path}");
            let error = replace_check_path(
                Error::ConfigCheckFailed {
                    message: message.clone(),
                    config_check_diagnostic: Some(CoreConfigCheckDiagnostic::Unrecognized {
                        raw: message,
                    }),
                },
                Utf8Path::new(path),
            );
            let Error::ConfigCheckFailed {
                message,
                config_check_diagnostic,
            } = error
            else {
                panic!("expected check failure")
            };
            let expected = format!("<checked-config>\n{other}\n<checked-config>");
            assert_eq!(message, expected);
            assert_eq!(
                config_check_diagnostic,
                Some(CoreConfigCheckDiagnostic::Unrecognized { raw: expected })
            );
        }
    }

    #[test]
    fn replacement_preserves_typed_names_and_unrelated_errors() {
        let path = Utf8Path::new("/tmp/check.yaml");
        let diagnostic = CoreConfigCheckDiagnostic::MissingGroupMember {
            group: path.as_str().into(),
            member: "X".into(),
        };
        let error = replace_check_path(
            Error::ConfigCheckFailed {
                message: "no path here".into(),
                config_check_diagnostic: Some(diagnostic.clone()),
            },
            path,
        );
        assert_eq!(error.to_string(), "config check failed: no path here");
        assert_eq!(error.config_check_diagnostic(), Some(&diagnostic));
        let error = replace_check_path(Error::InvalidConfig(path.as_str().into()), path);
        assert_eq!(error.to_string(), "invalid runtime config: /tmp/check.yaml");
    }
}
