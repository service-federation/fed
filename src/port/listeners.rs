//! Who listens on TCP ports of this machine, for many ports in one call of
//! `lsof`, or of `ss` where `lsof` is missing.
//!
//! Without root, both show the process only for the caller's own sockets.

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

/// A program that lists TCP listeners with their process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerTool {
    Lsof,
    Ss,
}

impl ListenerTool {
    /// `lsof` when it is on PATH, else `ss`, else none.
    pub fn find() -> Option<Self> {
        if on_path("lsof") {
            Some(Self::Lsof)
        } else if on_path("ss") {
            Some(Self::Ss)
        } else {
            None
        }
    }

    /// The call that lists every TCP listener on `ports`, and maybe others.
    pub fn command(self, ports: &[u16]) -> Command {
        match self {
            Self::Lsof => lsof_command(ports),
            Self::Ss => ss_command(),
        }
    }

    /// The listeners on `ports` in the output of [`Self::command`].
    pub fn parse(self, output: &str, ports: &[u16]) -> Vec<TcpListenerInfo> {
        let all = match self {
            Self::Lsof => parse_lsof(output),
            Self::Ss => parse_ss(output),
        };
        all.into_iter()
            .filter(|l| ports.contains(&l.port))
            .collect()
    }
}

/// Whether an executable file called `name` is in a directory of PATH.
fn on_path(name: &str) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| {
        std::fs::metadata(dir.join(name))
            .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
    })
}

/// An address as both tools print it, without brackets or an interface
/// (`%lo`). An IPv4 address in IPv6 form (`::ffff:127.0.0.1`) is the IPv4
/// address.
fn plain_address(address: &str) -> String {
    let address = address.trim_start_matches('[').trim_end_matches(']');
    let address = address.split('%').next().unwrap_or(address);
    match address.strip_prefix("::ffff:") {
        Some(ipv4) if ipv4.parse::<std::net::Ipv4Addr>().is_ok() => ipv4.to_string(),
        _ => address.to_string(),
    }
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
                address: plain_address(address),
                port,
            });
        }
    }
    listeners
}

/// The `ss` call that lists every TCP listener, with its process.
pub fn ss_command() -> Command {
    let mut cmd = Command::new("ss");
    cmd.args(["-H", "-l", "-t", "-n", "-p"]);
    cmd
}

/// The listeners in the output of [`ss_command`]: one line per socket,
/// `LISTEN <recv-q> <send-q> <address>:<port> <peer> users:(("<name>",pid=<pid>,fd=<fd>),...)`.
/// A socket shared by several processes gives one listener per process. A
/// socket without `users:` belongs to another user, and is left out.
pub fn parse_ss(output: &str) -> Vec<TcpListenerInfo> {
    let mut listeners = Vec::new();
    for line in output.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let Some(local) = fields.get(3) else {
            continue;
        };
        let Some((address, port)) = local.rsplit_once(':') else {
            continue;
        };
        let Ok(port) = port.parse() else {
            continue;
        };
        let Some(users) = line.split_once("users:((").map(|(_, users)| users) else {
            continue;
        };
        for process in users.trim_end().trim_end_matches("))").split("),(") {
            let name = process.split('"').nth(1).unwrap_or("unknown");
            let pid = process
                .split(',')
                .find_map(|part| part.strip_prefix("pid="))
                .and_then(|pid| pid.parse().ok());
            if let Some(pid) = pid {
                listeners.push(TcpListenerInfo {
                    pid,
                    name: name.to_string(),
                    address: plain_address(address),
                    port,
                });
            }
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
    fn ss_output_gives_each_socket_with_its_processes() {
        let output = "\
LISTEN 0      128          127.0.0.1:3000       0.0.0.0:*    users:((\"ssh\",pid=42,fd=3))
LISTEN 0      128              [::1]:3000          [::]:*    users:((\"ssh\",pid=42,fd=4))
LISTEN 0      128 [::ffff:127.0.0.1]:4000             *:*    users:((\"node\",pid=7,fd=9))
LISTEN 0      4096                 *:5000             *:*    users:((\"web\",pid=8,fd=3),(\"web\",pid=9,fd=3))
LISTEN 0      128            0.0.0.0:6000       0.0.0.0:*    users:((\"pg\",pid=10,fd=5))
LISTEN 0      128      127.0.0.53%lo:7000       0.0.0.0:*
";
        let listener = |pid, name: &str, address: &str, port| TcpListenerInfo {
            pid,
            name: name.into(),
            address: address.into(),
            port,
        };
        assert_eq!(
            parse_ss(output),
            vec![
                listener(42, "ssh", "127.0.0.1", 3000),
                listener(42, "ssh", "::1", 3000),
                listener(7, "node", "127.0.0.1", 4000),
                listener(8, "web", "*", 5000),
                listener(9, "web", "*", 5000),
                listener(10, "pg", "0.0.0.0", 6000),
            ]
        );
        assert_eq!(
            ListenerTool::Ss.parse(output, &[3000]),
            vec![
                listener(42, "ssh", "127.0.0.1", 3000),
                listener(42, "ssh", "::1", 3000),
            ]
        );
    }

    #[test]
    fn addresses_lose_brackets_interfaces_and_the_ipv4_prefix() {
        assert_eq!(plain_address("[::1]"), "::1");
        assert_eq!(plain_address("[::ffff:127.0.0.1]"), "127.0.0.1");
        assert_eq!(plain_address("127.0.0.53%lo"), "127.0.0.53");
        assert_eq!(plain_address("[fe80::1%eth0]"), "fe80::1");
        assert_eq!(plain_address("*"), "*");
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
