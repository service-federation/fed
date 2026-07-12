use super::{ServiceManager, Status};
use crate::config::Service as ServiceConfig;
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// Hook-only service manager — the oneshot node.
///
/// A hook-only service declares `install:` and/or `migrate:` but no
/// process/image/gradle/compose field. Its actual work (the install/migrate
/// hooks) is run by the orchestrator *before* this manager's [`start`] is
/// called; the manager itself is a pure completion marker. There is no
/// long-running process: once its hooks have run, [`start`] marks the node
/// [`Status::Completed`] and its dependents may proceed.
///
/// The node "re-runs" on every `fed start` because managers are rebuilt fresh
/// each invocation — `has_run` is `false` at the start of every startup — and
/// `migrate:` is executed unconditionally by the orchestrator each time.
///
/// [`start`]: OneshotService::start
pub struct OneshotService {
    name: String,
    status: Status,
    /// Whether this node already completed in THIS `fed` process.
    ///
    /// Managers are rebuilt fresh each `fed` invocation, so this is `false` at
    /// the start of every `fed start`/`fed restart`. Within a single startup it
    /// is the concurrency-dedup signal: a second dependent that reaches a shared
    /// hook-only node sees `has_run` and skips re-running its hooks (it only
    /// gets here after the first execution finished).
    has_run: bool,
}

impl OneshotService {
    /// Create a hook-only node manager. `config`/`environment`/`work_dir` are
    /// accepted for a uniform factory signature but unused: the node runs no
    /// process of its own — its hooks are executed by the orchestrator.
    pub fn new(
        name: String,
        _config: ServiceConfig,
        _environment: HashMap<String, String>,
        _work_dir: String,
    ) -> Self {
        Self {
            name,
            status: Status::Stopped,
            has_run: false,
        }
    }

    /// Whether this node already completed this session.
    pub fn has_run(&self) -> bool {
        self.has_run
    }

    /// Restore a persisted status for display (e.g. `Completed` from a previous
    /// session's state tracker). Deliberately does NOT set `has_run`, so the
    /// node's hooks still re-run on the next `fed start`.
    pub fn restore_status(&mut self, status: Status) {
        self.status = status;
    }
}

#[async_trait]
impl ServiceManager for OneshotService {
    async fn start(&mut self) -> Result<()> {
        // A hook-only node runs no process of its own. Its install/migrate hooks
        // were already executed by the orchestrator before this point, so start
        // is simply the completion signal that lets dependents proceed. A hook
        // failure surfaces earlier (from the orchestrator's install/migrate
        // step) and aborts the start before we get here.
        self.status = Status::Completed;
        self.has_run = true;
        tracing::info!("Hook-only service '{}' completed", self.name);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        // Nothing to kill — a hook-only node has no running process. Stopping
        // clears the completion so a subsequent start re-runs its hooks.
        self.status = Status::Stopped;
        self.has_run = false;
        Ok(())
    }

    async fn kill(&mut self) -> Result<()> {
        self.status = Status::Stopped;
        self.has_run = false;
        Ok(())
    }

    async fn health(&self) -> Result<bool> {
        Ok(self.status == Status::Completed)
    }

    fn status(&self) -> Status {
        self.status
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A hook-only node has no run command of its own — its hooks are executed by
    // the orchestrator. The manager only tracks completion. (Hook-failure naming
    // is covered end-to-end by tests/oneshot_test.rs, where a failing `migrate:`
    // aborts `fed start` naming the node.)
    fn hook_node(name: &str) -> OneshotService {
        OneshotService::new(
            name.to_string(),
            ServiceConfig::default(),
            HashMap::new(),
            "/tmp".to_string(),
        )
    }

    #[tokio::test]
    async fn completes_and_marks_has_run() {
        let mut svc = hook_node("schema");

        assert_eq!(svc.status(), Status::Stopped);
        assert!(!svc.has_run());

        svc.start().await.expect("hook-only node should complete");

        assert_eq!(svc.status(), Status::Completed);
        assert!(svc.has_run());
    }

    #[tokio::test]
    async fn stop_is_a_clean_noop_that_clears_completion() {
        let mut svc = hook_node("schema");
        svc.start().await.unwrap();
        assert!(svc.has_run());

        svc.stop().await.expect("stop must be a graceful no-op");
        assert_eq!(svc.status(), Status::Stopped);
        assert!(!svc.has_run());
    }

    #[tokio::test]
    async fn restore_status_does_not_set_has_run() {
        let mut svc = hook_node("schema");
        svc.restore_status(Status::Completed);
        assert_eq!(svc.status(), Status::Completed);
        assert!(
            !svc.has_run(),
            "restored completion must not suppress the next re-run"
        );
    }
}
