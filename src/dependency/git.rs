use crate::error::Result;
use git2::Repository;
use std::path::{Path, PathBuf};

/// Git operations for external dependencies
pub struct GitOperations;

impl GitOperations {
    /// Clone or update a repository
    pub fn clone_or_update(
        repo_url: &str,
        branch: Option<&str>,
        target_path: &Path,
    ) -> Result<PathBuf> {
        if target_path.exists() {
            // Repository already exists, try to update it
            Self::update_repository(target_path, branch)?;
        } else {
            // Clone the repository
            Self::clone_repository(repo_url, branch, target_path)?;
        }

        Ok(target_path.to_path_buf())
    }

    fn clone_repository(repo_url: &str, branch: Option<&str>, target_path: &Path) -> Result<()> {
        let mut builder = git2::build::RepoBuilder::new();

        if let Some(branch_name) = branch {
            builder.branch(branch_name);
        }

        builder.clone(repo_url, target_path)?;

        Ok(())
    }

    fn update_repository(repo_path: &Path, branch: Option<&str>) -> Result<()> {
        let repo = Repository::open(repo_path)?;

        // Fetch from remote
        let mut remote = repo.find_remote("origin")?;
        remote.fetch(&["refs/heads/*:refs/heads/*"], None, None)?;

        // Checkout branch if specified
        if let Some(branch_name) = branch {
            let branch_ref = format!("refs/heads/{}", branch_name);
            let obj = repo.revparse_single(&branch_ref)?;
            repo.checkout_tree(&obj, None)?;
            repo.set_head(&branch_ref)?;
        }

        Ok(())
    }

    /// Check if a path is a git repository
    pub fn is_git_repo(path: &Path) -> bool {
        Repository::open(path).is_ok()
    }

    /// Get current branch name
    pub fn get_current_branch(path: &Path) -> Result<String> {
        let repo = Repository::open(path)?;
        let head = repo.head()?;

        if let Ok(branch_name) = head.shorthand() {
            Ok(branch_name.to_string())
        } else {
            Ok("HEAD".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_dependency_transports_remain_enabled() {
        let version = git2::Version::get();
        assert!(version.https(), "HTTPS repository dependencies must work");
        assert!(version.ssh(), "SSH repository dependencies must work");
    }

    #[test]
    fn branch_detection_handles_named_and_detached_heads() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repository::init(dir.path()).unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let author = git2::Signature::now("Test", "test@example.invalid").unwrap();
        let commit = repo
            .commit(
                Some("refs/heads/audit"),
                &author,
                &author,
                "test",
                &tree,
                &[],
            )
            .unwrap();
        repo.set_head("refs/heads/audit").unwrap();
        assert_eq!(
            GitOperations::get_current_branch(dir.path()).unwrap(),
            "audit"
        );
        repo.set_head_detached(commit).unwrap();
        assert_eq!(
            GitOperations::get_current_branch(dir.path()).unwrap(),
            "HEAD"
        );
    }
}
