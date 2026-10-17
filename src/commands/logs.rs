use crate::output::UserOutput;
use fed::{Error as FedError, Orchestrator};

pub async fn run_logs(
    orchestrator: &Orchestrator,
    service: &str,
    tail: Option<usize>,
    follow: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    if follow {
        out.status(&format!(
            "Following logs for {} (Press Ctrl+C to stop):",
            service
        ));
        out.status(&format!("{:-<50}", ""));

        let mut previous = Vec::<String>::new();
        #[cfg(unix)]
        let mut file_follower = match orchestrator.log_file_path(service).await {
            Ok(path) => {
                path.map(|path| fed::log_follow::FileFollower::new(path, tail.unwrap_or(10_000)))
            }
            Err(error) => return Err(logs_error(orchestrator, service, error).await),
        };

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown_tx.send(()).await.ok();
        });

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    out.status("\nStopped following logs");
                    break;
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    #[cfg(unix)]
                    if let Some(mut follower) = file_follower.take() {
                        let (next, batch) = tokio::task::spawn_blocking(move || {
                            let batch = follower.poll();
                            (follower, batch)
                        }).await?;
                        file_follower = Some(next);
                        let batch = batch?;
                        if batch.history_lost {
                            out.status("Some log history expired before it could be read.");
                        }
                        for line in batch.lines {
                            out.status(&line);
                        }
                        continue;
                    }
                    match orchestrator.get_logs(service, None).await {
                        Ok(logs) => {
                            let overlap = (0..=previous.len().min(logs.len()))
                                .rev()
                                .find(|&count| previous[previous.len() - count..] == logs[..count])
                                .unwrap_or(0);
                            for line in logs.iter().skip(overlap) {
                                out.status(line);
                            }
                            previous = logs;
                        }
                        Err(e) => {
                            return Err(logs_error(orchestrator, service, e).await);
                        }
                    }
                }
            }
        }
    } else {
        match orchestrator.get_logs(service, tail).await {
            Ok(logs) => {
                if logs.is_empty() {
                    out.status(&format!("No logs available for service '{}'", service));
                } else {
                    out.status(&format!("Logs for {}:", service));
                    out.status(&format!("{:-<50}", ""));
                    for line in logs {
                        out.status(&line);
                    }
                }
            }
            Err(e) => {
                return Err(logs_error(orchestrator, service, e).await);
            }
        }
    }

    Ok(())
}

/// Turn a get_logs failure into a single, rich error for main to print once.
/// For unknown services this includes a did-you-mean hint and the service list.
async fn logs_error(orchestrator: &Orchestrator, service: &str, e: FedError) -> anyhow::Error {
    if matches!(e, FedError::ServiceNotFound(_)) {
        let status = orchestrator.get_status().await;
        if !status.is_empty() {
            let mut msg = super::suggest::with_did_you_mean(
                &format!("Service '{}' not found.", service),
                service,
                status.keys().map(String::as_str),
            );
            msg.push_str("\n\nAvailable services:");
            let mut names: Vec<_> = status.keys().collect();
            names.sort();
            for name in names {
                msg.push_str(&format!("\n  - {}", name));
            }
            return anyhow::anyhow!(msg);
        }
    }
    e.into()
}
