//! `tty: true`: a service declaring that it needs a terminal.
//!
//! The field is accepted on process services only, and a restart policy is
//! refused alongside it.

mod support;

use support::parse_checked;

#[test]
fn tty_is_off_unless_the_service_asks_for_it() {
    let config = parse_checked(
        r#"
services:
  repl:
    process: "python3 -i"
    tty: true
  worker:
    process: "./worker"
"#,
    );

    assert!(config.services["repl"].tty);
    assert!(!config.services["worker"].tty);
    config.validate().expect("a tty process service is valid");
}

#[test]
fn tty_on_a_docker_service_is_rejected() {
    let config = parse_checked(
        r#"
services:
  db:
    image: "postgres:15"
    tty: true
"#,
    );

    let error = config
        .validate()
        .expect_err("a tty image service is invalid");
    assert_eq!(
        error.to_string(),
        "Invalid configuration: Service 'db': tty: true needs a process: command. \
         Only process services can run under a terminal."
    );
}

#[test]
fn tty_with_a_restart_policy_is_rejected() {
    let config = parse_checked(
        r#"
services:
  repl:
    process: "python3 -i"
    tty: true
    restart: !on_failure
      max_retries: 3
"#,
    );

    let error = config
        .validate()
        .expect_err("a restarted tty service is invalid");
    assert_eq!(
        error.to_string(),
        "Invalid configuration: Service 'repl': tty: true cannot be combined with restart:. \
         A restarted tty service would lose its terminal."
    );
}

#[test]
fn tty_with_restart_no_is_accepted() {
    let config = parse_checked(
        r#"
services:
  repl:
    process: "python3 -i"
    tty: true
    restart: no
"#,
    );

    config
        .validate()
        .expect("restart: no is the absence of restarts");
}

/// Every new module of the attach feature is unix only, so the field is too.
#[cfg(not(unix))]
#[test]
fn tty_is_rejected_off_unix() {
    let config = parse_checked(
        r#"
services:
  repl:
    process: "python3 -i"
    tty: true
"#,
    );

    let error = config.validate().expect_err("tty needs a unix platform");
    assert_eq!(
        error.to_string(),
        "Invalid configuration: Service 'repl': tty: true is not supported on this platform."
    );
}
