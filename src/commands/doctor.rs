use crate::output::UserOutput;
use fed::docker::DockerClient;

pub async fn run_doctor(out: &dyn UserOutput) -> anyhow::Result<()> {
    out.status("Checking system requirements...\n");

    let mut all_ok = true;
    let mut daemon_running = false;
    let docker = DockerClient::new();

    // Check Docker
    out.progress("Docker: ");
    match docker.version().await {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
            out.finish_progress(&version);

            // Check Docker daemon is actually running
            out.progress("Docker daemon: ");
            if docker.info_status().await {
                out.finish_progress("Running");
                daemon_running = true;
            } else {
                out.finish_progress(
                    "Not running (start Docker Desktop or run: sudo systemctl start docker)",
                );
                all_ok = false;
            }
        }
        _ => {
            out.finish_progress("Not found");
            all_ok = false;
        }
    }

    // Check docker-compose
    out.progress("docker-compose: ");
    match docker.compose_version().await {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
            out.finish_progress(&version);
        }
        _ => {
            out.finish_progress("Not found (optional)");
        }
    }

    // Check Gradle
    out.progress("Gradle: ");
    match tokio::process::Command::new("gradle")
        .arg("--version")
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Some(line) = stdout.lines().find(|l| l.contains("Gradle")) {
                out.finish_progress(line.trim());
            } else {
                out.finish_progress("Installed");
            }
        }
        _ => {
            out.finish_progress("Not found (optional, needed for gradle services)");
        }
    }

    // Check Java
    out.progress("Java: ");
    match tokio::process::Command::new("java")
        .arg("--version")
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Some(line) = stdout.lines().next() {
                out.finish_progress(line.trim());
            } else {
                out.finish_progress("Installed");
            }
        }
        _ => {
            out.finish_progress("Not found (optional, needed for Gradle/Java services)");
        }
    }

    // Check Git
    out.progress("Git: ");
    match tokio::process::Command::new("git")
        .arg("--version")
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
            out.finish_progress(&version);
        }
        _ => {
            out.finish_progress("Not found (optional)");
        }
    }

    // Orphaned fed volumes — isolation and per-run stacks accumulate these and nothing else
    // reaps them. Surfacing the count here is the natural place to notice a growing graveyard.
    if daemon_running {
        out.progress("Orphaned fed volumes: ");
        match docker.orphaned_fed_volumes().await {
            Ok(v) if v.is_empty() => out.finish_progress("none"),
            Ok(v) => out.finish_progress(&format!(
                "{} reclaimable — run `fed prune` to remove",
                v.len()
            )),
            Err(_) => out.finish_progress("(could not check)"),
        }
    }

    out.blank();
    if all_ok {
        out.success("All required dependencies are installed");
    } else {
        out.status("Some required dependencies are missing");
        out.error("\nInstallation guides:");
        out.error("  Docker: https://docs.docker.com/get-docker/");
    }

    Ok(())
}
