//! Who listens on TCP ports of this machine, for many ports in one `lsof`.
//!
//! `lsof` without root sees only the caller's own processes.

use std::process::Command;

/// One listening TCP socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpListenerInfo {
    pub pid: u32,
    pub name: String,
    /// The address it listens on, such as `127.0.0.1`, `::1` or `*`.
    pub address: String,
    pub port: u16,
}

/// The `lsof` call that lists every TCP listener on `ports`.
pub fn lsof_command(ports: &[u16]) -> Command {
    let mut cmd = Command::new("lsof");
    cmd.args(["-n", "-P", "-sTCP:LISTEN", "-F", "pcn"]);
    for port in ports {
        cmd.arg(format!("-iTCP:{port}"));
    }
    cmd
}

/// The listeners in the output of [`lsof_command`]. Each process block
/// starts with a `p<pid>` line, then `c<command>`, then one `n<address>:<port>`
/// line per socket.
pub fn parse_lsof(output: &str) -> Vec<TcpListenerInfo> {
    let mut listeners = Vec::new();
    let mut pid = None;
    let mut name = String::new();
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix('p') {
            pid = rest.parse().ok();
            name.clear();
        } else if let Some(rest) = line.strip_prefix('c') {
            name = rest.to_string();
        } else if let Some(rest) = line.strip_prefix('n')
            && let Some(pid) = pid
            && let Some((address, port)) = rest.rsplit_once(':')
            && let Ok(port) = port.parse()
        {
            listeners.push(TcpListenerInfo {
                pid,
                name: name.clone(),
                address: address
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_string(),
                port,
            });
        }
    }
    listeners
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsof_output_gives_each_socket_with_its_process() {
        let output = "p42\ncssh\nf3\nn127.0.0.1:3000\nf4\nn[::1]:3000\np7\ncnode\nf9\nn*:5432\n";
        let listener = |pid, name: &str, address: &str, port| TcpListenerInfo {
            pid,
            name: name.into(),
            address: address.into(),
            port,
        };
        assert_eq!(
            parse_lsof(output),
            vec![
                listener(42, "ssh", "127.0.0.1", 3000),
                listener(42, "ssh", "::1", 3000),
                listener(7, "node", "*", 5432),
            ]
        );
    }

    #[test]
    fn a_udp_socket_is_not_listed() {
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = udp.local_addr().unwrap().port();
        let output = lsof_command(&[port]).output().unwrap();
        assert!(
            parse_lsof(&String::from_utf8_lossy(&output.stdout)).is_empty(),
            "a UDP socket on {port} was listed as a TCP listener"
        );
    }
}
