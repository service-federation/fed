use crate::config::{ComposeImport, Config, DependsOn, Service};
use crate::docker::runtime;
use crate::error::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Expand top-level Compose imports into ordinary, service-scoped Fed nodes.
pub async fn expand(mut config: Config, work_dir: &Path) -> Result<Config> {
    let imports = std::mem::take(&mut config.compose);
    let mut imported_files = HashSet::new();
    for import in imports {
        let file = import.options().file;
        let canonical = absolute_path(work_dir, &file)?;
        if !imported_files.insert(canonical) {
            return Err(Error::Config(format!(
                "Compose file '{}' is imported more than once; include it once and use the generated services",
                file
            )));
        }
        expand_one(&mut config, import, work_dir).await?;
    }
    Ok(config)
}

async fn expand_one(config: &mut Config, import: ComposeImport, work_dir: &Path) -> Result<()> {
    let options = import.options();
    let path = absolute_path(work_dir, &options.file)?;
    let path_text = path.to_string_lossy().to_string();
    let mut command = tokio::process::Command::new(runtime::binary());
    command.args(["compose", "-f", &path_text]);
    for profile in &options.profiles {
        command.args(["--profile", profile]);
    }
    command.args(["config", "--format", "json"]);
    command.envs(&options.environment);
    let output = command.output().await.map_err(|e| {
        Error::Config(format!(
            "Could not inspect Compose import '{}': {}",
            options.file, e
        ))
    })?;
    if !output.status.success() {
        return Err(Error::Config(format!(
            "Could not normalize Compose import '{}': {}",
            options.file,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let model: Value = serde_json::from_slice(&output.stdout).map_err(|e| {
        Error::Config(format!(
            "Compose returned invalid JSON for '{}': {}",
            options.file, e
        ))
    })?;
    let services = model
        .get("services")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Error::Config(format!("Compose import '{}' has no services", options.file))
        })?;

    let mut generated = HashMap::new();
    for (compose_name, value) in services {
        let fed_name = qualified(options.namespace.as_deref(), compose_name);
        if config.services.contains_key(&fed_name) {
            return Err(Error::Config(format!(
                "Compose import '{}' generates service '{}', but that Fed service already exists; add a namespace to the import",
                options.file, fed_name
            )));
        }
        let mut service = Service {
            compose_file: Some(path_text.clone()),
            compose_service: Some(compose_name.clone()),
            environment: options.environment.clone(),
            compose_profiles: options.profiles.clone(),
            compose_imported: true,
            ..Default::default()
        };
        if let Some(dependencies) = value.get("depends_on").and_then(Value::as_object) {
            for (dependency, detail) in dependencies {
                let condition = detail
                    .get("condition")
                    .and_then(Value::as_str)
                    .unwrap_or("service_started");
                if condition != "service_started" {
                    return Err(Error::Config(format!(
                        "Compose import '{}' service '{}' uses depends_on condition '{}'; imported Compose services currently support only service_started",
                        options.file, compose_name, condition
                    )));
                }
                service.depends_on.push(DependsOn::Simple(qualified(
                    options.namespace.as_deref(),
                    dependency,
                )));
            }
        }
        generated.insert(fed_name, service);
    }
    config.services.extend(generated);
    Ok(())
}

fn qualified(namespace: Option<&str>, service: &str) -> String {
    namespace
        .map(|n| format!("{n}/{service}"))
        .unwrap_or_else(|| service.to_string())
}

fn absolute_path(work_dir: &Path, file: &str) -> Result<PathBuf> {
    let path = PathBuf::from(file);
    let path = if path.is_absolute() {
        path
    } else {
        work_dir.join(path)
    };
    path.canonicalize().map_err(|e| {
        Error::Config(format!(
            "Could not resolve Compose import '{}': {}",
            file, e
        ))
    })
}
