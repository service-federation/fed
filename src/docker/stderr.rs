//! Classification of container-runtime stderr into the few outcomes fed treats
//! as success.
//!
//! These are string matches against a CLI's human-facing diagnostics — the only
//! signal available, since the runtimes reuse exit code 1 for "container gone"
//! and for real failures alike. That fragility is the reason the matching lives
//! here rather than inline at four call sites: the phrases are a compatibility
//! surface, and a future runtime with different wording extends these functions
//! instead of being grepped for.
//!
//! Matching is case-insensitive. The phrases below are Docker's exact wording,
//! but capitalization is the least stable part of a CLI's diagnostics and
//! nothing is gained by being strict about it.

/// Whether stderr says the container does not exist.
///
/// A missing container is the desired end state for `rm`, so callers treat this
/// as success rather than surfacing it.
pub fn stderr_indicates_missing_container(stderr: &str) -> bool {
    stderr.to_lowercase().contains("no such container")
}

/// Whether stderr says the container exists but is not running.
///
/// Only meaningful for `kill`: a stopped container is already in the state
/// `kill` was asked to produce.
pub fn stderr_indicates_not_running(stderr: &str) -> bool {
    stderr.to_lowercase().contains("is not running")
}

/// Whether a failed `pull` actually left the image present locally.
///
/// "up to date" / "already exists" come back on a non-zero exit in the
/// digest-pinned and concurrent-pull cases; the image is there, so failing the
/// caller would be wrong.
///
/// Only the LAST non-empty line is consulted: a multi-layer pull writes
/// per-layer progress ("abc123: Already exists") to the same stream, so a pull
/// that reused some layers and then hit a real error ("pull access denied")
/// would otherwise classify as a no-op and the failure would be swallowed. The
/// terminal status line is the one that says how the pull actually ended.
pub fn stderr_indicates_pull_noop(stderr: &str) -> bool {
    let Some(last_line) = stderr.lines().rev().find(|l| !l.trim().is_empty()) else {
        return false;
    };
    let lower = last_line.to_lowercase();
    lower.contains("up to date") || lower.contains("already exists")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_container_matches_dockers_phrasing() {
        assert!(stderr_indicates_missing_container(
            "Error response from daemon: No such container: fed-api"
        ));
        assert!(stderr_indicates_missing_container(
            "error during connect: No such container"
        ));
    }

    #[test]
    fn missing_container_is_case_insensitive() {
        assert!(stderr_indicates_missing_container("no such container: x"));
        assert!(stderr_indicates_missing_container("NO SUCH CONTAINER: x"));
        assert!(stderr_indicates_missing_container("No Such Container: x"));
    }

    #[test]
    fn missing_container_rejects_unrelated_failures() {
        assert!(!stderr_indicates_missing_container(
            "Error response from daemon: conflict: unable to remove container"
        ));
        assert!(!stderr_indicates_missing_container(""));
        // "No such image" is a different failure and must not be swallowed by
        // a container-removal call.
        assert!(!stderr_indicates_missing_container(
            "Error: No such image: alpine"
        ));
    }

    #[test]
    fn not_running_matches_and_is_case_insensitive() {
        assert!(stderr_indicates_not_running(
            "Error response from daemon: Cannot kill container: fed-db: Container fed-db is not running"
        ));
        assert!(stderr_indicates_not_running("IS NOT RUNNING"));
        assert!(!stderr_indicates_not_running(
            "Cannot kill container: permission denied"
        ));
    }

    #[test]
    fn pull_noop_matches_both_phrases_in_any_case() {
        assert!(stderr_indicates_pull_noop(
            "Status: Image is up to date for postgres:16"
        ));
        assert!(stderr_indicates_pull_noop("Image is UP TO DATE"));
        assert!(stderr_indicates_pull_noop("abc123: Already exists"));
        assert!(stderr_indicates_pull_noop("abc123: already exists"));
    }

    #[test]
    fn pull_noop_rejects_real_failures() {
        assert!(!stderr_indicates_pull_noop(
            "Error response from daemon: manifest for x:1 not found"
        ));
        assert!(!stderr_indicates_pull_noop(
            "Error response from daemon: pull access denied"
        ));
        assert!(!stderr_indicates_pull_noop(""));
    }

    #[test]
    fn pull_noop_ignores_layer_progress_before_a_real_failure() {
        // Per-layer "Already exists" progress followed by a terminal error is
        // a FAILED pull; only the final status line decides.
        assert!(!stderr_indicates_pull_noop(
            "abc123: Already exists\nError response from daemon: pull access denied"
        ));
        assert!(!stderr_indicates_pull_noop(
            "def456: Pulling fs layer\nabc123: Already exists\n\nmanifest unknown"
        ));
        // Conversely a terminal no-op status still classifies as one even with
        // progress lines above it, and with trailing blank lines.
        assert!(stderr_indicates_pull_noop(
            "abc123: Already exists\nStatus: Image is up to date for postgres:16\n"
        ));
    }
}
