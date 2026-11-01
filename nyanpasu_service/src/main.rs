#[cfg(windows)]
mod win_service;

use std::future::Future;

use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use nyanpasu_service::{
    Command,
    consts::ExitCode,
    handler,
    utils::{os::register_ctrlc_handler, register_panic_hook},
};
use nyanpasu_utils::runtime::block_on;

fn main() -> ExitCode {
    #[cfg(windows)]
    if let Some(result) = nyanpasu_utils::process::windows_shutdown_helper_entry() {
        return match result {
            Ok(()) => ExitCode::Normal,
            Err(error) => {
                eprintln!("console shutdown helper: {error}");
                ExitCode::Other
            }
        };
    }
    let mut rx = register_ctrlc_handler();
    register_panic_hook();
    #[cfg(windows)]
    {
        let args = std::env::args_os().any(|arg| &arg == "--service");
        if args {
            return match crate::win_service::run() {
                Ok(()) => ExitCode::Normal,
                Err(error) => {
                    eprintln!("service dispatcher failed: {error}");
                    ExitCode::Other
                }
            };
        }
    }

    let command = Command::parse();
    let is_server = command.is_server();
    let shutdown = CancellationToken::new();
    block_on(run_until_signal(
        &mut rx,
        handler(command, shutdown.clone()),
        is_server,
        shutdown,
    ))
}

async fn run_until_signal(
    signals: &mut Receiver<()>,
    command: impl Future<Output = ExitCode>,
    is_server: bool,
    shutdown: CancellationToken,
) -> ExitCode {
    tokio::pin!(command);
    tokio::select! {
        biased;
        Some(()) = signals.recv() => {}
        exit_code = &mut command => return exit_code,
    }
    if !is_server {
        return ExitCode::Normal;
    }
    shutdown.cancel();
    tokio::select! {
        biased;
        exit_code = &mut command => exit_code,
        Some(()) = signals.recv() => {
            eprintln!("Stopping without waiting for the service to finish");
            ExitCode::Normal
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::{Notify, mpsc};

    #[tokio::test]
    async fn stop_before_token_publication_still_drains_server() {
        let (signal, mut rx) = mpsc::channel(1);
        signal.send(()).await.unwrap();
        let shutdown = CancellationToken::new();
        let server_token = shutdown.clone();
        let exit = run_until_signal(
            &mut rx,
            async move {
                assert!(server_token.is_cancelled());
                ExitCode::Other
            },
            true,
            shutdown,
        )
        .await;
        assert!(matches!(exit, ExitCode::Other));
    }

    #[tokio::test]
    async fn a_signal_waits_for_the_server_to_stop_its_core() {
        let (signal, mut rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let server_token = shutdown.clone();
        let stopping = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (server_stopping, server_release) = (stopping.clone(), release.clone());
        let running = tokio::spawn(async move {
            run_until_signal(
                &mut rx,
                async move {
                    server_token.cancelled().await;
                    server_stopping.notify_one();
                    server_release.notified().await;
                    ExitCode::Other
                },
                true,
                shutdown,
            )
            .await
        });
        signal.send(()).await.unwrap();
        stopping.notified().await;
        assert!(!running.is_finished());
        release.notify_one();
        assert!(matches!(running.await.unwrap(), ExitCode::Other));
    }

    #[tokio::test]
    async fn server_second_signal_still_ends_wait() {
        let (signal, mut rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let observed = shutdown.clone();
        let running = tokio::spawn(async move {
            run_until_signal(&mut rx, std::future::pending(), true, shutdown).await
        });
        signal.send(()).await.unwrap();
        observed.cancelled().await;
        assert!(!running.is_finished());
        signal.send(()).await.unwrap();
        assert!(matches!(running.await.unwrap(), ExitCode::Normal));
    }

    #[tokio::test]
    async fn non_server_first_signal_still_exits() {
        let (signal, mut rx) = mpsc::channel(1);
        signal.send(()).await.unwrap();
        let exit = run_until_signal(
            &mut rx,
            std::future::pending(),
            false,
            CancellationToken::new(),
        )
        .await;
        assert!(matches!(exit, ExitCode::Normal));
    }

    #[tokio::test]
    async fn a_command_that_ends_first_keeps_its_exit_code() {
        let (_signal, mut rx) = mpsc::channel(1);
        let exit = run_until_signal(
            &mut rx,
            async { ExitCode::PermissionDenied },
            false,
            CancellationToken::new(),
        )
        .await;
        assert!(matches!(exit, ExitCode::PermissionDenied));
    }
}

#[cfg(any(windows, test))]
mod service_host {
    use super::ExitCode;
    use std::{
        future::{Future, poll_fn},
        pin::Pin,
        sync::{Arc, Mutex},
        task::Poll,
        thread::JoinHandle,
        time::Duration,
    };
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    pub const STOP_PENDING_TICK: Duration = Duration::from_secs(2);
    const STOP_PENDING_DEADLINE: Duration = Duration::from_secs(90);

    #[derive(Clone)]
    pub struct Shutdown {
        pub token: CancellationToken,
        started: Arc<Mutex<Option<Instant>>>,
    }

    impl Shutdown {
        pub fn new() -> Self {
            Self {
                token: CancellationToken::new(),
                started: Arc::new(Mutex::new(None)),
            }
        }

        pub fn request(&self) {
            let mut started = self.started.lock().unwrap();
            started.get_or_insert_with(Instant::now);
            self.token.cancel();
        }
    }

    pub fn start_worker<W, E>(
        shutdown: &Shutdown,
        configure: impl FnOnce() -> Result<(), E>,
        start: impl FnOnce() -> W,
    ) -> Result<Option<W>, E> {
        configure()?;
        let _started = shutdown.started.lock().unwrap();
        if shutdown.token.is_cancelled() {
            return Ok(None);
        }
        Ok(Some(start()))
    }

    pub async fn join_worker(
        handle: JoinHandle<ExitCode>,
        completion: tokio::sync::oneshot::Receiver<()>,
    ) -> ExitCode {
        let mut handle = Some(handle);
        let mut completion = Some(completion);
        let mut ticks = tokio::time::interval(STOP_PENDING_TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        poll_fn(move |cx| {
            if handle.as_ref().unwrap().is_finished() {
                return Poll::Ready(handle.take().unwrap().join().unwrap_or_else(|_| {
                    tracing::error!("service worker panicked");
                    ExitCode::Other
                }));
            }
            if let Some(receiver) = completion.as_mut()
                && Pin::new(receiver).poll(cx).is_ready()
            {
                completion = None;
            }
            if ticks.poll_tick(cx).is_ready() {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
        .await
    }

    pub async fn wait_for_service(
        shutdown: &Shutdown,
        worker: impl Future<Output = ExitCode>,
        mut report_stop_pending: impl FnMut(u32),
    ) -> ExitCode {
        tokio::pin!(worker);
        tokio::select! {
            biased;
            _ = shutdown.token.cancelled() => {},
            exit = &mut worker => return exit,
        }
        let deadline = shutdown
            .started
            .lock()
            .unwrap()
            .expect("SCM stop timestamp")
            + STOP_PENDING_DEADLINE;
        let mut checkpoint = 1;
        report_stop_pending(checkpoint);
        let mut ticks =
            tokio::time::interval_at(Instant::now() + STOP_PENDING_TICK, STOP_PENDING_TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                exit = &mut worker => return exit,
                _ = tokio::time::sleep_until(deadline) => {
                    tracing::error!("service shutdown exceeded 90s while waiting for the server worker; exiting with failure");
                    return ExitCode::Other;
                }
                _ = ticks.tick() => {
                    checkpoint += 1;
                    report_stop_pending(checkpoint);
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::cell::RefCell;
        use tokio::sync::oneshot;

        #[tokio::test]
        async fn finished_thread_keeps_its_exit_code() {
            let (done, completion) = oneshot::channel();
            let handle = std::thread::spawn(move || {
                let _ = done.send(());
                ExitCode::PermissionDenied
            });
            assert!(matches!(
                join_worker(handle, completion).await,
                ExitCode::PermissionDenied
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn completion_notification_never_joins_a_live_thread() {
            let release = Arc::new(std::sync::Barrier::new(2));
            let worker_release = release.clone();
            let (done, completion) = oneshot::channel();
            let (ready, entered) = oneshot::channel();
            let (exited, exit_ack) = oneshot::channel();
            let handle = std::thread::spawn(move || {
                let _ = done.send(());
                let _ = ready.send(());
                worker_release.wait();
                let _ = exited.send(());
                ExitCode::Normal
            });
            entered.await.unwrap();
            let shutdown = Shutdown::new();
            shutdown.request();
            let exit = wait_for_service(&shutdown, join_worker(handle, completion), |_| {}).await;
            release.wait();
            exit_ack.await.unwrap();
            assert!(matches!(exit, ExitCode::Other));
        }

        #[tokio::test(start_paused = true)]
        async fn worker_failure_does_not_wait_for_scm_stop() {
            let shutdown = Shutdown::new();
            let (tx, rx) = oneshot::channel();
            tx.send(ExitCode::PermissionDenied).ok().unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                wait_for_service(&shutdown, async { rx.await.unwrap() }, |_| {
                    panic!("no stop requested")
                }),
            )
            .await;
            assert!(matches!(result, Ok(ExitCode::PermissionDenied)));
        }

        #[tokio::test(start_paused = true)]
        async fn shutdown_deadline_never_joins_an_unfinished_worker() {
            let shutdown = Shutdown::new();
            shutdown.request();
            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(91),
                wait_for_service(&shutdown, std::future::pending(), |_| {}),
            )
            .await;
            assert!(matches!(result, Ok(ExitCode::Other)));
            assert_eq!(started.elapsed(), Duration::from_secs(90));
        }

        #[tokio::test(start_paused = true)]
        async fn completed_worker_at_deadline_keeps_its_exit_code() {
            let shutdown = Shutdown::new();
            shutdown.request();
            let exit = wait_for_service(
                &shutdown,
                async {
                    tokio::time::sleep(STOP_PENDING_DEADLINE).await;
                    ExitCode::PermissionDenied
                },
                |_| {},
            )
            .await;
            assert!(matches!(exit, ExitCode::PermissionDenied));
        }

        #[tokio::test(start_paused = true)]
        async fn repeated_stop_does_not_reset_the_callback_deadline() {
            let shutdown = Shutdown::new();
            shutdown.request();
            tokio::time::advance(Duration::from_secs(89)).await;
            shutdown.request();
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                wait_for_service(&shutdown, std::future::pending(), |_| {}),
            )
            .await;
            assert!(matches!(result, Ok(ExitCode::Other)));
        }

        #[test]
        fn startup_repairs_preshutdown_before_running() {
            let calls = RefCell::new(Vec::new());
            start_worker(
                &Shutdown::new(),
                || {
                    calls.borrow_mut().push("configure");
                    Ok::<_, ()>(())
                },
                || calls.borrow_mut().push("running"),
            )
            .unwrap();
            assert_eq!(*calls.borrow(), ["configure", "running"]);
        }

        #[test]
        fn each_start_repairs_the_preshutdown_budget() {
            let calls = RefCell::new(0);
            for _ in 0..2 {
                start_worker(
                    &Shutdown::new(),
                    || {
                        *calls.borrow_mut() += 1;
                        Ok::<_, ()>(())
                    },
                    || {},
                )
                .unwrap();
            }
            assert_eq!(*calls.borrow(), 2);
        }

        #[test]
        fn stop_during_configuration_never_reports_running() {
            let shutdown = Shutdown::new();
            let result = start_worker(
                &shutdown,
                || {
                    shutdown.request();
                    Ok::<_, ()>(())
                },
                || panic!("stop arrived during configuration"),
            );
            assert!(matches!(result, Ok(None)));
        }

        #[test]
        fn configuration_failure_never_reports_running() {
            let result = start_worker(
                &Shutdown::new(),
                || Err("configuration failed"),
                || panic!("must not report Running"),
            );
            assert!(result.is_err());
        }

        #[test]
        fn early_stop_never_reports_running_or_starts_worker() {
            let shutdown = Shutdown::new();
            shutdown.request();
            let result = start_worker(
                &shutdown,
                || Ok::<_, ()>(()),
                || panic!("already cancelled"),
            );
            assert!(matches!(result, Ok(None)));
        }
    }
}
