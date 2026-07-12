use crate::output::UserOutput;
use fed::Orchestrator;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

pub async fn run_top(
    orchestrator: &Orchestrator,
    interval: u64,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    out.status(&format!(
        "Service Federation - Resource Monitor (refresh every {}s, press Ctrl+C to exit)\n",
        interval
    ));

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        shutdown_tx.send(()).await.ok();
    });

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                out.status("\nStopped monitoring");
                break;
            }
            _ = sleep(Duration::from_secs(interval)) => {
                // Clear screen (ANSI escape code)
                out.progress("\x1B[2J\x1B[1;1H");
                out.finish_progress("");

                out.status(&format!("Service Federation - Resource Monitor (refresh every {}s, press Ctrl+C to exit)\n", interval));

                let status = orchestrator.get_status().await;

                if status.is_empty() {
                    out.status("No services running");
                    continue;
                }

                // Header
                out.status(&format!("{:<20} {:<12} {:<10} {:<10} {:<10}", "SERVICE", "STATUS", "CPU %", "MEM (MB)", "PID"));
                out.status(&format!("{:-<62}", ""));

                let mut stats: HashMap<String, (String, String, String, String)> = HashMap::new();

                for (name, stat) in &status {
                    let status_str = match stat {
                        fed::Status::Running | fed::Status::Healthy => "running",
                        fed::Status::Completed => "completed",
                        fed::Status::Stopped => "stopped",
                        fed::Status::Starting => "starting",
                        fed::Status::Failing => "failing",
                        fed::Status::Stopping => "stopping",
                    };

                    let (cpu, mem, pid) = match orchestrator.get_service_pid(name).await {
                        Ok(Some(service_pid)) => {
                            let output = tokio::process::Command::new("ps")
                                .args(["-p", &service_pid.to_string(), "-o", "pid=,pcpu=,rss="])
                                .output()
                                .await;

                            match output {
                                Ok(out) if out.status.success() => {
                                    let output_str = String::from_utf8_lossy(&out.stdout);
                                    let parts: Vec<&str> = output_str.split_whitespace().collect();

                                    if parts.len() >= 3 {
                                        let pid_str = parts[0].to_string();
                                        let cpu_str = format!("{:.1}", parts[1].parse::<f64>().unwrap_or(0.0));
                                        let mem_kb = parts[2].parse::<f64>().unwrap_or(0.0);
                                        let mem_str = format!("{:.1}", mem_kb / 1024.0);
                                        (cpu_str, mem_str, pid_str)
                                    } else {
                                        ("-".to_string(), "-".to_string(), service_pid.to_string())
                                    }
                                }
                                _ => ("-".to_string(), "-".to_string(), service_pid.to_string())
                            }
                        }
                        _ => ("-".to_string(), "-".to_string(), "-".to_string())
                    };

                    stats.insert(name.clone(), (status_str.to_string(), cpu, mem, pid));
                }

                let mut sorted_names: Vec<_> = stats.keys().cloned().collect();
                sorted_names.sort();

                for name in sorted_names {
                    let (status, cpu, mem, pid) = &stats[&name];
                    out.status(&format!("{:<20} {:<12} {:<10} {:<10} {:<10}", name, status, cpu, mem, pid));
                }

                out.blank();
                out.status(&format!("Next refresh in {}s...", interval));
            }
        }
    }

    Ok(())
}
