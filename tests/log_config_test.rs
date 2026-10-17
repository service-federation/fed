mod support;

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const PROJECT: &str = "services:\n  hello:\n    process: 'true'\n";

fn write_project(path: &Path, yaml: &str) {
    support::parse_checked(yaml);
    fs::write(path, yaml).unwrap();
}

fn run_validate(project: &Path, home: &Path, xdg_home: Option<&OsStr>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fed"));
    command
        .arg("-c")
        .arg(project)
        .arg("validate")
        .env("HOME", home);
    match xdg_home {
        Some(path) => {
            command.env("XDG_CONFIG_HOME", path);
        }
        None => {
            command.env_remove("XDG_CONFIG_HOME");
        }
    }
    command.output().unwrap()
}

fn assert_global_error(output: Output, path: &Path, field: &str) {
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&path.display().to_string()), "{stderr}");
    assert!(stderr.contains(field), "{stderr}");
}

#[test]
fn relative_xdg_home_falls_back_to_home_without_writing_config() {
    let dir = tempfile::tempdir().unwrap();
    let global = dir.path().join(".config/fed/config.yaml");
    fs::create_dir_all(global.parent().unwrap()).unwrap();
    fs::write(&global, "logs:\n  max_files: 0\n").unwrap();
    let project = dir.path().join("fed.yaml");
    write_project(&project, PROJECT);

    let output = run_validate(&project, dir.path(), Some(OsStr::new("relative/config")));
    assert_global_error(output, &global, "logs.max_files");
    assert!(!dir.path().join("relative/config").exists());
}

#[test]
fn unset_and_empty_xdg_home_fall_back_to_home() {
    let dir = tempfile::tempdir().unwrap();
    let global = dir.path().join(".config/fed/config.yaml");
    fs::create_dir_all(global.parent().unwrap()).unwrap();
    fs::write(&global, "logs:\n  max_size: 0B\n").unwrap();
    let project = dir.path().join("fed.yaml");
    write_project(&project, PROJECT);

    for xdg_home in [None, Some(OsStr::new(""))] {
        assert_global_error(
            run_validate(&project, dir.path(), xdg_home),
            &global,
            "logs.max_size",
        );
    }
}

#[test]
fn absolute_xdg_home_overrides_home_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let xdg_home = dir.path().join("elsewhere");
    let global = xdg_home.join("fed/config.yaml");
    fs::create_dir_all(global.parent().unwrap()).unwrap();
    fs::write(&global, "logs:\n  max_files: 0\n").unwrap();
    let project = dir.path().join("fed.yaml");
    write_project(&project, PROJECT);

    let output = run_validate(&project, dir.path(), Some(xdg_home.as_os_str()));
    assert_global_error(output, &global, "logs.max_files");
    assert!(!dir.path().join(".config").exists());
}

#[test]
fn missing_global_config_does_not_create_a_directory() {
    let dir = tempfile::tempdir().unwrap();
    let xdg_home = dir.path().join("uncreated");
    let project = dir.path().join("fed.yaml");
    write_project(&project, PROJECT);

    let output = run_validate(&project, dir.path(), Some(xdg_home.as_os_str()));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!xdg_home.exists());
}

#[test]
fn malformed_global_config_reports_its_path() {
    let dir = tempfile::tempdir().unwrap();
    let global = dir.path().join("fed/config.yaml");
    fs::create_dir_all(global.parent().unwrap()).unwrap();
    fs::write(&global, "logs: [broken]\n").unwrap();
    let project = dir.path().join("fed.yaml");
    write_project(&project, PROJECT);

    let output = run_validate(&project, dir.path(), Some(dir.path().as_os_str()));
    assert_global_error(output, &global, "Failed to parse global config");
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_xdg_home_is_used_without_lossy_conversion() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let mut bytes = dir.path().as_os_str().as_encoded_bytes().to_vec();
    bytes.extend_from_slice(b"/config-\xff");
    let xdg = std::ffi::OsString::from_vec(bytes);
    let global = PathBuf::from(&xdg).join("fed/config.yaml");
    fs::create_dir_all(global.parent().unwrap()).unwrap();
    fs::write(&global, "logs:\n  max_files: 0\n").unwrap();
    let project = dir.path().join("fed.yaml");
    write_project(&project, PROJECT);

    let output = run_validate(&project, dir.path(), Some(&xdg));
    assert_global_error(output, &global, "logs.max_files");
}

#[test]
fn custom_config_path_merges_its_fields_with_global_limits() {
    let dir = tempfile::tempdir().unwrap();
    let global = dir.path().join("fed/config.yaml");
    fs::create_dir_all(global.parent().unwrap()).unwrap();
    fs::write(&global, "logs:\n  max_size: 20MiB\n  max_files: 8\n").unwrap();
    write_project(
        &dir.path().join("fed.yaml"),
        "logs:\n  max_size: 1MiB\nservices:\n  hello:\n    process: 'true'\n",
    );
    let custom = dir.path().join("custom.yaml");
    write_project(
        &custom,
        "logs:\n  max_files: 2\nservices:\n  hello:\n    process: 'true'\n",
    );

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("subprocess_reports_logging")
        .arg("--nocapture")
        .env("FED_LOG_CONFIG_PROBE", &custom)
        .env("HOME", dir.path())
        .env("XDG_CONFIG_HOME", dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("FED_LOG_CONFIG_RESULT=20971520,2"));
}

#[test]
fn subprocess_reports_logging() {
    let Some(path) = std::env::var_os("FED_LOG_CONFIG_PROBE") else {
        return;
    };
    let config = fed::Parser::new().load_config(PathBuf::from(path)).unwrap();
    println!(
        "FED_LOG_CONFIG_RESULT={},{}",
        config.logging.max_size, config.logging.max_files
    );
}
