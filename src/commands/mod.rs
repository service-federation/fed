mod auth;
mod build;
mod clean;
mod debug;
mod docker;
mod doctor;
mod init;
mod install;
mod isolate;
mod lifecycle;
mod link;
mod logs;
mod package;
mod ports;
mod prune;
mod restart;
mod script;
mod secrets_cmd;
mod start;
mod status;
mod stop;
mod top;
mod tui;
mod validate;
mod workspace;

pub(crate) mod suggest;

pub use auth::{run_login, run_logout, run_whoami};
pub use build::run_build;
pub use clean::run_clean;
pub use debug::{run_debug, DebugCommand};
pub use docker::{run_docker_build, run_docker_push};
pub use doctor::run_doctor;
pub use init::run_init;
pub use install::run_install;
pub use isolate::run_isolate;
pub use link::run_link;
pub use logs::run_logs;
pub use package::run_package;
pub use ports::run_ports;
pub use prune::run_prune;
pub use restart::run_restart;
pub use script::run_script;
pub use secrets_cmd::run_secrets;
pub use start::{run_start, StartOptions};
pub use status::run_status;
pub use stop::{run_stop, run_stop_from_state};
pub use top::run_top;
pub use tui::run_tui;
pub use validate::run_validate;
pub use workspace::run_workspace;

/// Emit non-breaking warnings for unknown (typo'd) config keys — used by `fed validate`
/// and `fed start`. fed keeps parsing permissive: a mistyped key is a warning with a
/// "did you mean?" hint, not a hard error.
pub fn emit_config_warnings(config: &fed::Config, out: &dyn crate::output::UserOutput) {
    for w in config.unknown_key_warnings() {
        let msg = format!("{}: unknown field '{}'", w.location, w.key);
        out.warning(&suggest::with_did_you_mean(
            &msg,
            &w.key,
            w.candidates.iter().copied(),
        ));
    }
}
