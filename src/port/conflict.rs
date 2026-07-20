use crate::docker::DockerClient;
use std::process::Command;

#[derive(Debug, Clone)]
pub struct PortConflict {
    pub port: u16,
    pub processes: Vec<ProcessInfo>,
}

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    pub name: String,
    pub command: Option<String>,
}

/// Check if a process name indicates Docker is holding the port.
/// On macOS, Docker Desktop reports as "com.docker.backend".
/// On Linux, Docker uses "docker-proxy" for port forwarding.
fn is_docker_process(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    name_lower.contains("docker") || name_lower.contains("com.docker")
}

impl PortConflict {
    /// Check if a port is in use and return conflict info
    pub fn check(port: u16) -> Option<Self> {
        // Check if port is actually in use by trying to bind to both addresses.
        // On macOS, binding to 127.0.0.1 can succeed even when 0.0.0.0 is in use,
        // so we need to check both.
        let localhost_available = std::net::TcpListener::bind(("127.0.0.1", port)).is_ok();
        let any_available = std::net::TcpListener::bind(("0.0.0.0", port)).is_ok();

        if localhost_available && any_available {
            return None; // Port is truly available
        }

        // Port is in use - try to find ALL processes using it
        let processes = Self::find_processes_on_port(port);

        Some(PortConflict { port, processes })
    }

    /// Check if a port is available (can bind to it)
    pub fn is_port_available(port: u16) -> bool {
        std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
            && std::net::TcpListener::bind(("0.0.0.0", port)).is_ok()
    }

    /// Find ALL processes using a port (cross-platform)
    fn find_processes_on_port(port: u16) -> Vec<ProcessInfo> {
        #[cfg(target_os = "macos")]
        {
            Self::find_processes_macos(port)
        }

        #[cfg(target_os = "linux")]
        {
            Self::find_processes_linux(port)
        }

        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Vec::new()
        }
    }

    #[cfg(target_os = "macos")]
    fn find_processes_macos(port: u16) -> Vec<ProcessInfo> {
        // Use lsof to find LISTENING processes on macOS
        // -sTCP:LISTEN filters to only show listeners, not clients
        let output = match Command::new("lsof")
            .args([
                "-i",
                &format!(":{}", port),
                "-P",
                "-n",
                "-sTCP:LISTEN",
                "-F",
                "pcn",
            ])
            .output()
        {
            Ok(o) => o,
            Err(_) => return Vec::new(),
        };

        if !output.status.success() {
            return Vec::new();
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut processes = Vec::new();
        let mut seen_pids = std::collections::HashSet::new();
        let mut current_pid: Option<u32> = None;
        let mut current_command: Option<String> = None;

        // Parse lsof output (field format: pPID, cCOMMAND, nNAME)
        // Each process block starts with 'p' line
        for line in stdout.lines() {
            if let Some(stripped) = line.strip_prefix('p') {
                // New process - save the previous one if valid
                if let Some(pid) = current_pid
                    && !seen_pids.contains(&pid)
                {
                    seen_pids.insert(pid);
                    processes.push(ProcessInfo {
                        pid,
                        name: current_command
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                        command: current_command.clone(),
                    });
                }
                current_pid = stripped.parse::<u32>().ok();
                current_command = None;
            } else if let Some(stripped) = line.strip_prefix('c') {
                current_command = Some(stripped.to_string());
            }
        }

        // Don't forget the last process
        if let Some(pid) = current_pid
            && !seen_pids.contains(&pid)
        {
            processes.push(ProcessInfo {
                pid,
                name: current_command
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                command: current_command,
            });
        }

        processes
    }

    #[cfg(target_os = "linux")]
    fn find_processes_linux(port: u16) -> Vec<ProcessInfo> {
        // Combine results from ss and lsof for completeness
        let mut processes = Self::find_processes_linux_ss(port);
        let lsof_processes = Self::find_processes_linux_lsof(port);

        // Add lsof results that aren't already in the list
        let seen_pids: std::collections::HashSet<u32> = processes.iter().map(|p| p.pid).collect();
        for p in lsof_processes {
            if !seen_pids.contains(&p.pid) {
                processes.push(p);
            }
        }

        processes
    }

    #[cfg(target_os = "linux")]
    fn find_processes_linux_ss(port: u16) -> Vec<ProcessInfo> {
        let output = match Command::new("ss")
            .args(["-tlnp", &format!("sport = :{}", port)])
            .output()
        {
            Ok(o) => o,
            Err(_) => return Vec::new(),
        };

        if !output.status.success() {
            return Vec::new();
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut processes = Vec::new();
        let mut seen_pids = std::collections::HashSet::new();

        // Parse ss output: look for pid=PID,fd=...
        for line in stdout.lines().skip(1) {
            // Skip header
            if let Some(users_part) = line.split_whitespace().last() {
                // ss can report multiple pids per line
                for part in users_part.split(',') {
                    if let Some(pid_str) = part.strip_prefix("pid=")
                        && let Ok(pid) = pid_str.parse::<u32>()
                        && !seen_pids.contains(&pid)
                    {
                        seen_pids.insert(pid);
                        // Get process name from /proc
                        let name = std::fs::read_to_string(format!("/proc/{}/comm", pid))
                            .ok()
                            .map(|s| s.trim().to_string())
                            .unwrap_or_else(|| "unknown".to_string());

                        let command = std::fs::read_to_string(format!("/proc/{}/cmdline", pid))
                            .ok()
                            .map(|s| s.replace('\0', " ").trim().to_string());

                        processes.push(ProcessInfo { pid, name, command });
                    }
                }
            }
        }

        processes
    }

    #[cfg(target_os = "linux")]
    fn find_processes_linux_lsof(port: u16) -> Vec<ProcessInfo> {
        // -sTCP:LISTEN filters to only show listeners, not clients
        let output = match Command::new("lsof")
            .args([
                "-i",
                &format!(":{}", port),
                "-P",
                "-n",
                "-sTCP:LISTEN",
                "-F",
                "pcn",
            ])
            .output()
        {
            Ok(o) => o,
            Err(_) => return Vec::new(),
        };

        if !output.status.success() {
            return Vec::new();
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut processes = Vec::new();
        let mut seen_pids = std::collections::HashSet::new();
        let mut current_pid: Option<u32> = None;
        let mut current_command: Option<String> = None;

        for line in stdout.lines() {
            if let Some(stripped) = line.strip_prefix('p') {
                if let Some(pid) = current_pid
                    && !seen_pids.contains(&pid)
                {
                    seen_pids.insert(pid);
                    processes.push(ProcessInfo {
                        pid,
                        name: current_command
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                        command: current_command.clone(),
                    });
                }
                current_pid = stripped.parse::<u32>().ok();
                current_command = None;
            } else if let Some(stripped) = line.strip_prefix('c') {
                current_command = Some(stripped.to_string());
            }
        }

        if let Some(pid) = current_pid
            && !seen_pids.contains(&pid)
        {
            processes.push(ProcessInfo {
                pid,
                name: current_command
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                command: current_command,
            });
        }

        processes
    }

    /// Free the port by stopping whatever is using it.
    /// Handles both Docker containers and regular processes.
    /// Returns a description of what was stopped, or an error.
    pub fn free_port(&self) -> std::result::Result<String, String> {
        let current_pid = std::process::id();
        let other_processes: Vec<_> = self
            .processes
            .iter()
            .filter(|p| p.pid != current_pid)
            .collect();

        if other_processes.is_empty() {
            return Ok("port already available".to_string());
        }

        // Check if Docker is holding the port
        let is_docker = other_processes.iter().any(|p| is_docker_process(&p.name));

        if is_docker {
            return self.free_docker_port();
        }

        // Regular processes - kill them
        let names: Vec<_> = other_processes
            .iter()
            .map(|p| format!("'{}' (PID {})", p.name, p.pid))
            .collect();

        self.kill_and_verify(3)?;
        Ok(format!("killed {}", names.join(", ")))
    }

    /// Free a port held by Docker containers.
    fn free_docker_port(&self) -> std::result::Result<String, String> {
        let containers = Self::find_docker_containers_on_port(self.port);

        if containers.is_empty() {
            return Err(format!(
                "port {} is held by Docker but no container found (try: docker ps --filter \"publish={}\")",
                self.port, self.port
            ));
        }

        let mut stopped = Vec::new();
        let mut errors = Vec::new();

        let docker = DockerClient::new();
        for container in &containers {
            match docker.rm_force_sync(container) {
                Ok(()) => stopped.push(container.clone()),
                Err(e) => errors.push(format!("{}: {}", container, e)),
            }
        }

        if !errors.is_empty() {
            return Err(errors.join(", "));
        }

        // Wait for port release
        std::thread::sleep(std::time::Duration::from_millis(200));

        if Self::is_port_available(self.port) {
            Ok(format!("stopped container(s) {}", stopped.join(", ")))
        } else {
            Err(format!(
                "stopped {} but port {} still in use",
                stopped.join(", "),
                self.port
            ))
        }
    }

    /// Find Docker containers bound to a specific port.
    ///
    /// This lists all containers and parses their port mappings to find ones
    /// that have the target port published on the host.
    fn find_docker_containers_on_port(port: u16) -> Vec<String> {
        let entries = DockerClient::new().ps_names_and_ports_sync();
        let port_str = port.to_string();
        let mut containers = Vec::new();

        for (name, ports) in &entries {
            // Port mappings look like: "0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp"
            // We need to match the host port (before "->")
            for mapping in ports.split(", ") {
                // Extract the host port: "0.0.0.0:5433->5432/tcp" -> "5433"
                if let Some(arrow_pos) = mapping.find("->") {
                    let host_part = &mapping[..arrow_pos];
                    // Host part is "0.0.0.0:5433" or "[::]:5433" - get port after last ':'
                    if let Some(colon_pos) = host_part.rfind(':') {
                        let host_port = &host_part[colon_pos + 1..];
                        if host_port == port_str {
                            containers.push(name.clone());
                            break; // Found this container, move to next
                        }
                    }
                }
            }
        }

        containers
    }

    /// Kill ALL processes using the port (except the current process)
    pub fn kill_all_blocking_processes(&self) -> Vec<(u32, std::result::Result<(), String>)> {
        let current_pid = std::process::id();
        let mut results = Vec::new();

        #[cfg(unix)]
        {
            for process in &self.processes {
                // Never kill ourselves - we may be holding the port to reserve it
                if process.pid == current_pid {
                    tracing::debug!(
                        "Skipping self (PID {}) when killing port conflicts on port {}",
                        process.pid,
                        self.port
                    );
                    continue;
                }

                // Use output() to capture stderr and suppress "No such process" noise
                // when processes exit between detection and kill (race condition)
                let result = Command::new("kill")
                    .arg(process.pid.to_string())
                    .stderr(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .output()
                    .map_err(|e| format!("Failed to kill process {}: {}", process.pid, e))
                    .and_then(|output| {
                        if output.status.success() {
                            Ok(())
                        } else {
                            // Check if it's just "No such process" (ESRCH) - treat as success
                            // since the process is gone which is what we wanted
                            let stderr = String::from_utf8_lossy(&output.stderr);
                            if stderr.contains("No such process") {
                                tracing::debug!(
                                    "Process {} already exited before kill",
                                    process.pid
                                );
                                Ok(())
                            } else {
                                Err(format!("kill failed: {}", stderr.trim()))
                            }
                        }
                    });
                results.push((process.pid, result));
            }
        }

        #[cfg(not(unix))]
        {
            for process in &self.processes {
                // Never kill ourselves
                if process.pid == current_pid {
                    tracing::debug!(
                        "Skipping self (PID {}) when killing port conflicts on port {}",
                        process.pid,
                        self.port
                    );
                    continue;
                }

                results.push((
                    process.pid,
                    Err("Killing processes is only supported on Unix".to_string()),
                ));
            }
        }

        results
    }

    /// Kill all processes and verify port becomes available, with retries
    pub fn kill_and_verify(&self, max_attempts: u32) -> std::result::Result<(), String> {
        for attempt in 1..=max_attempts {
            // Kill all processes we know about
            let _results = self.kill_all_blocking_processes();

            // Short wait for processes to die
            std::thread::sleep(std::time::Duration::from_millis(100));

            // Check if port is now available
            if Self::is_port_available(self.port) {
                return Ok(());
            }

            // Port still in use - find any new processes that appeared
            if attempt < max_attempts {
                if let Some(new_conflict) = Self::check(self.port)
                    && !new_conflict.processes.is_empty()
                {
                    // Kill these too
                    let _ = new_conflict.kill_all_blocking_processes();
                    std::thread::sleep(std::time::Duration::from_millis(100));

                    if Self::is_port_available(self.port) {
                        return Ok(());
                    }
                }

                // Exponential backoff: 100ms, 200ms, 400ms...
                std::thread::sleep(std::time::Duration::from_millis(100 * (1 << attempt)));
            }
        }

        Err(format!(
            "Port {} still in use after {} attempts to kill blocking processes",
            self.port, max_attempts
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_docker_process_macos() {
        assert!(is_docker_process("com.docker.backend"));
        assert!(is_docker_process("com.docker.vpnkit"));
    }

    #[test]
    fn test_is_docker_process_linux() {
        assert!(is_docker_process("docker-proxy"));
        assert!(is_docker_process("dockerd"));
    }

    #[test]
    fn test_is_docker_process_negative() {
        assert!(!is_docker_process("node"));
        assert!(!is_docker_process("nginx"));
        assert!(!is_docker_process("postgres"));
    }

    #[test]
    fn test_free_port_already_available() {
        // Port held only by current process should return success
        let conflict = PortConflict {
            port: 9999,
            processes: vec![ProcessInfo {
                pid: std::process::id(),
                name: "self".to_string(),
                command: None,
            }],
        };
        assert!(conflict.free_port().unwrap().contains("already available"));
    }
}
