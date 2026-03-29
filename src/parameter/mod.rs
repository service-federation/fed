pub mod generate;
mod port;
mod resolver;
pub mod secret;

pub use port::*;
pub use resolver::*;

use std::path::{Path, PathBuf};

/// Expand a leading `~` or `~/` to the user's home directory.
/// Returns the path unchanged if it doesn't start with `~` or if the home
/// directory can't be determined.
pub(crate) fn expand_tilde(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if s == "~" {
        dirs::home_dir().unwrap_or_else(|| path.to_path_buf())
    } else if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            home.join(rest)
        } else {
            path.to_path_buf()
        }
    } else {
        path.to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_tilde_home() {
        let result = expand_tilde(Path::new("~"));
        let home = dirs::home_dir().unwrap();
        assert_eq!(result, home);
    }

    #[test]
    fn expand_tilde_subpath() {
        let result = expand_tilde(Path::new("~/.env.shared"));
        let home = dirs::home_dir().unwrap();
        assert_eq!(result, home.join(".env.shared"));
    }

    #[test]
    fn expand_tilde_nested_subpath() {
        let result = expand_tilde(Path::new("~/secrets/team.env"));
        let home = dirs::home_dir().unwrap();
        assert_eq!(result, home.join("secrets/team.env"));
    }

    #[test]
    fn expand_tilde_relative_unchanged() {
        let result = expand_tilde(Path::new(".env"));
        assert_eq!(result, PathBuf::from(".env"));
    }

    #[test]
    fn expand_tilde_absolute_unchanged() {
        let result = expand_tilde(Path::new("/etc/env"));
        assert_eq!(result, PathBuf::from("/etc/env"));
    }

    #[test]
    fn expand_tilde_mid_path_unchanged() {
        let result = expand_tilde(Path::new("foo/~/bar"));
        assert_eq!(result, PathBuf::from("foo/~/bar"));
    }
}
