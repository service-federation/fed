use super::{Config, HealthCheck, ServiceType, parse_duration_string};
use crate::error::{Error, Result};
use std::collections::HashSet;

impl Config {
    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // `variables` was an alias for `parameters`; removed in 4.0.
        if self.legacy_variables.as_ref().is_some_and(|v| !v.is_null()) {
            return Err(Error::Validation(
                "Unknown top-level key `variables:`. \
                 Rename it to `parameters:` — the schema is identical."
                    .to_string(),
            ));
        }

        // `FED_PROJECT_ID` is a built-in template variable (a stable, cookie-safe
        // per-project identifier). It is injected automatically and must not be
        // declared as a user parameter.
        if self
            .get_effective_parameters()
            .contains_key("FED_PROJECT_ID")
        {
            return Err(Error::Validation(
                "`FED_PROJECT_ID` is a reserved built-in parameter and cannot be declared under `parameters:`. \
                 It is provided automatically for template resolution — just reference it as `{{FED_PROJECT_ID}}`."
                    .to_string(),
            ));
        }

        // Check entrypoint/entrypoints exclusivity
        if self.entrypoint.is_some() && !self.entrypoints.is_empty() {
            return Err(Error::Validation(
                "Cannot specify both 'entrypoint' and 'entrypoints'".to_string(),
            ));
        }

        // Validate entrypoint references
        if let Some(ref ep) = self.entrypoint
            && !self.services.contains_key(ep)
        {
            return Err(Error::Validation(format!(
                "Entrypoint '{}' references non-existent service",
                ep
            )));
        }

        for ep in &self.entrypoints {
            if !self.services.contains_key(ep) {
                return Err(Error::Validation(format!(
                    "Entrypoint '{}' references non-existent service",
                    ep
                )));
            }
        }

        // Warn if entrypoint services lack startup_message
        let mut entrypoint_names: Vec<&str> = Vec::new();
        if let Some(ref ep) = self.entrypoint {
            entrypoint_names.push(ep);
        }
        for ep in &self.entrypoints {
            entrypoint_names.push(ep);
        }
        for ep_name in &entrypoint_names {
            if let Some(service) = self.services.get(*ep_name)
                && service.startup_message.is_none()
            {
                eprintln!(
                    "Warning: Entrypoint service '{}' has no startup_message.\n\
                                  Without this, isolated mode won't show where to access the application.\n\
                                  Add: startup_message: \"http://localhost:{{{{PORT}}}}\"",
                    ep_name
                );
            }
        }

        self.warn_on_literal_local_ports();

        // Validate each service has exactly one type
        for (name, service) in &self.services {
            if service.service_type() == ServiceType::Undefined {
                return Err(Error::Validation(format!(
                    "Service '{}' has no type defined. Add one of: process, image, gradleTask, composeFile + composeService, or make it a hook-only node with install and/or migrate.",
                    name
                )));
            }

            // Reject ambiguous configs with multiple type-defining fields
            let type_fields: Vec<&str> = [
                service.process.as_ref().map(|_| "process"),
                service.image.as_ref().map(|_| "image"),
                service.gradle_task.as_ref().map(|_| "gradle_task"),
                service
                    .compose_file
                    .as_ref()
                    .and(service.compose_service.as_ref())
                    .map(|_| "compose_file + compose_service"),
            ]
            .into_iter()
            .flatten()
            .collect();

            if type_fields.len() > 1 {
                return Err(Error::Validation(format!(
                    "Service '{}' has multiple type-defining fields: {}. Remove all but one.",
                    name,
                    type_fields.join(", ")
                )));
            }

            // A hook-only node completes to signal readiness, so a healthcheck
            // or restart policy is contradictory — reject both. (A process/image
            // service with install/migrate is a normal service, not hook-only,
            // and may have either.)
            if service.service_type().is_hook_only() {
                if service.healthcheck.is_some() {
                    return Err(Error::Validation(format!(
                        "Hook-only service '{}' cannot have a `healthcheck` — completion of its install/migrate hooks IS its readiness signal. Remove the healthcheck.",
                        name
                    )));
                }
                if service.restart.is_some() {
                    return Err(Error::Validation(format!(
                        "Hook-only service '{}' cannot have a `restart` policy — its hooks run once to completion, they are never restarted. Remove the restart policy.",
                        name
                    )));
                }
            }
        }

        // Validate duration strings
        for (name, service) in &self.services {
            if let Some(ref gp) = service.grace_period
                && parse_duration_string(gp).is_none()
            {
                return Err(Error::Validation(format!(
                    "Service '{}' has invalid grace_period '{}'. Use formats like '5s', '30s', '1m', '500ms'",
                    name, gp
                )));
            }
            if let Some(ref st) = service.startup_timeout
                && parse_duration_string(st).is_none()
            {
                return Err(Error::Validation(format!(
                    "Service '{}' has invalid startup_timeout '{}'. Use formats like '60s', '5m', '500ms'",
                    name, st
                )));
            }
            if let Some(ref hc) = service.healthcheck {
                let timeout_str = match hc {
                    HealthCheck::HttpGet { timeout, .. } => timeout.as_deref(),
                    HealthCheck::CommandMap { timeout, .. } => timeout.as_deref(),
                    HealthCheck::Command(_) => None,
                };
                if let Some(t) = timeout_str
                    && parse_duration_string(t).is_none()
                {
                    return Err(Error::Validation(format!(
                        "Service '{}' has invalid healthcheck timeout '{}'. Use formats like '5s', '30s', '1m', '500ms'",
                        name, t
                    )));
                }
            }
        }

        for (name, script) in &self.scripts {
            if let Some(ref t) = script.timeout
                && parse_duration_string(t).is_none()
            {
                return Err(Error::Validation(format!(
                    "Script '{}' has invalid timeout '{}'. Use formats like '5s', '30s', '1m', '500ms'",
                    name, t
                )));
            }

            // `keep_services` keeps the started stack up for later use, but an
            // isolated script runs in a throwaway context that is always torn
            // down on exit — the two requests directly contradict each other.
            if script.keep_services && script.isolated {
                return Err(Error::Validation(format!(
                    "Script '{name}' can't set both `keep_services: true` and `isolated: true` — they contradict each other.\n\n\
                    scripts:\n  \
                      {name}:\n    \
                        isolated: true        # throwaway stack, always cleaned up on exit\n    \
                        keep_services: true   # ← keep the stack running — but there's nothing left to keep\n\n\
                    Pick one:\n  \
                      • drop `keep_services: true` to run in throwaway isolation, or\n  \
                      • drop `isolated: true` to leave the started stack running after the script.",
                    name = name
                )));
            }
        }

        // Validate parameter defaults against either constraints
        for (name, param) in self.get_effective_parameters() {
            if param.either.is_empty() {
                continue;
            }
            if let Some(ref default_value) = param.default {
                let default_str = match default_value {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    _ => continue, // Skip complex values
                };
                if !param.either.contains(&default_str) {
                    return Err(Error::Validation(format!(
                        "Parameter '{}' has default '{}' which is not in allowed values: {:?}",
                        name, default_str, param.either
                    )));
                }
            }
        }

        // Validate secret parameter constraints
        for (name, param) in self.get_effective_parameters() {
            if !param.is_secret_type() {
                continue;
            }

            // Secrets must not have a default value (they are generated or manually provided)
            if param.default.is_some() {
                return Err(Error::Validation(format!(
                    "Secret parameter '{}' must not have a default value — secrets are generated or provided via .env",
                    name
                )));
            }

            // Secrets must not have environment-specific values
            if param.development.is_some()
                || param.develop.is_some()
                || param.staging.is_some()
                || param.production.is_some()
            {
                return Err(Error::Validation(format!(
                    "Secret parameter '{}' must not have environment-specific values — secrets are generated or provided via .env",
                    name
                )));
            }

            // Secrets must not have either constraints
            if !param.either.is_empty() {
                return Err(Error::Validation(format!(
                    "Secret parameter '{}' must not have 'either' constraints — secret values are generated or provided externally",
                    name
                )));
            }

            // Only known source values are allowed
            if let Some(ref source) = param.source
                && source != "manual"
            {
                return Err(Error::Validation(format!(
                    "Secret parameter '{}' has unknown source '{}' — only 'manual' is currently supported",
                    name, source
                )));
            }

            // Auto-generated secrets need no extra configuration: they default
            // to .fed/secrets.generated.env. The deprecated
            // generated_secrets_file key is still honored when set.
        }

        // Validate external service dependencies
        for (name, service) in &self.services {
            if service.service_type() == ServiceType::External
                && let Some(ref dep_name) = service.dependency
                && !self.dependencies.contains_key(dep_name)
            {
                return Err(Error::Validation(format!(
                    "External service '{}' references undefined dependency '{}'",
                    name, dep_name
                )));
            }

            // Check service dependencies exist
            for dep in &service.depends_on {
                let dep_name = dep.service_name();
                // For external dependencies, we can't validate at config load time
                // They will be validated when external configs are loaded.
                // Simple and structured (service: + on_failure:) deps must exist locally.
                if !dep.is_external() && !self.services.contains_key(dep_name) {
                    // Build a dynamically-sized hint box
                    let hint_text =
                        format!("Did you remove '{}'? Update the depends_on list:", dep_name);
                    let box_width = hint_text.len() + 2; // +2 for padding
                    let border = "─".repeat(box_width);

                    return Err(Error::Validation(format!(
                        "Service '{}' depends on non-existent service '{}'\n\n\
                        ╭{}╮\n\
                        │ {} │\n\
                        ╰{}╯\n\n\
                        services:\n  \
                          {}:\n    \
                            depends_on:\n      \
                              - {}  # ← remove this line",
                        name, dep_name, border, hint_text, border, name, dep_name
                    )));
                }
            }
        }

        // Check for circular dependencies
        self.check_circular_dependencies()?;

        // Check for script circular dependencies
        self.check_script_circular_dependencies()?;

        // Check for service/script name conflicts
        for script_name in self.scripts.keys() {
            if self.services.contains_key(script_name) {
                return Err(Error::Validation(format!(
                    "Script '{}' has the same name as a service. Service and script names must be unique.",
                    script_name
                )));
            }
        }

        // Validate script dependencies exist
        for (script_name, script) in &self.scripts {
            for dep in &script.depends_on {
                if !self.services.contains_key(dep) && !self.scripts.contains_key(dep) {
                    let hint_text =
                        format!("Did you remove '{}'? Update the depends_on list:", dep);
                    let box_width = hint_text.len() + 2;
                    let border = "─".repeat(box_width);

                    return Err(Error::Validation(format!(
                        "Script '{}' depends on non-existent service or script '{}'\n\n\
                        ╭{}╮\n\
                        │ {} │\n\
                        ╰{}╯\n\n\
                        scripts:\n  \
                          {}:\n    \
                            depends_on:\n      \
                              - {}  # ← remove this line",
                        script_name, dep, border, hint_text, border, script_name, dep
                    )));
                }
            }
        }

        // Validate resource limits for all services
        for (service_name, service) in &self.services {
            if let Some(ref resources) = service.resources {
                validate_resource_limits(service_name, resources)?;
            }
        }

        // Validate environment variables for all services
        for (service_name, service) in &self.services {
            if !service.environment.is_empty() {
                crate::config::env_loader::validate_and_sanitize_env(&service.environment)
                    .map_err(|e| {
                        Error::Config(format!(
                            "Service '{}' has invalid environment variable: {}",
                            service_name, e
                        ))
                    })?;
            }
        }

        // Validate environment variables for all scripts
        for (script_name, script) in &self.scripts {
            if !script.environment.is_empty() {
                crate::config::env_loader::validate_and_sanitize_env(&script.environment).map_err(
                    |e| {
                        Error::Config(format!(
                            "Script '{}' has invalid environment variable: {}",
                            script_name, e
                        ))
                    },
                )?;
            }
        }

        Ok(())
    }

    fn check_circular_dependencies(&self) -> Result<()> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for service_name in self.services.keys() {
            if !visited.contains(service_name)
                && let Some(cycle) =
                    self.find_cycle(service_name, &mut visited, &mut rec_stack, &mut path)
            {
                return Err(Error::CircularDependency(cycle));
            }
        }

        Ok(())
    }

    fn find_cycle(
        &self,
        service: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        visited.insert(service.to_string());
        rec_stack.insert(service.to_string());
        path.push(service.to_string());

        if let Some(svc) = self.services.get(service) {
            for dep in &svc.depends_on {
                let dep_name = dep.service_name();
                if !visited.contains(dep_name) {
                    if let Some(cycle) = self.find_cycle(dep_name, visited, rec_stack, path) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(dep_name) {
                    // Found cycle - extract it from path
                    let cycle_start = path.iter().position(|n| n == dep_name).unwrap_or(0);
                    let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                    cycle.push(dep_name.to_string()); // Complete the cycle
                    return Some(cycle);
                }
            }
        }

        rec_stack.remove(service);
        path.pop();
        None
    }

    fn check_script_circular_dependencies(&self) -> Result<()> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for script_name in self.scripts.keys() {
            if !visited.contains(script_name)
                && let Some(cycle) =
                    self.find_script_cycle(script_name, &mut visited, &mut rec_stack, &mut path)
            {
                return Err(Error::CircularDependency(cycle));
            }
        }

        Ok(())
    }

    fn find_script_cycle(
        &self,
        script: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        visited.insert(script.to_string());
        rec_stack.insert(script.to_string());
        path.push(script.to_string());

        if let Some(scr) = self.scripts.get(script) {
            for dep in &scr.depends_on {
                // Only follow script→script edges
                // Script→service edges are valid and don't create script cycles
                if self.scripts.contains_key(dep) {
                    if !visited.contains(dep) {
                        if let Some(cycle) = self.find_script_cycle(dep, visited, rec_stack, path) {
                            return Some(cycle);
                        }
                    } else if rec_stack.contains(dep) {
                        // Found cycle - extract it from path
                        let cycle_start = path.iter().position(|n| n == dep).unwrap_or(0);
                        let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                        cycle.push(dep.to_string()); // Complete the cycle
                        return Some(cycle);
                    }
                }
            }
        }

        rec_stack.remove(script);
        path.pop();
        None
    }

    /// Warn (once, aggregated) when the config references literal local ports
    /// instead of `type: port` parameters. Literal ports work in normal mode
    /// but silently break isolation: every isolated workspace needs its own
    /// ports, and only `{{PARAM}}` interpolation gets remapped.
    fn warn_on_literal_local_ports(&self) {
        use std::fmt::Write as _;
        let findings = self.literal_local_port_findings();
        if !findings.is_empty() {
            let mut msg = String::new();
            let _ = write!(
                msg,
                "Warning: literal local ports in config — service {}.\n\
                 Literal ports work in normal mode but break isolated mode, where every\n\
                 workspace gets its own ports. Declare a port parameter and interpolate it:\n\
                 \x20 parameters:\n\
                 \x20   APP_PORT:\n\
                 \x20     type: port\n\
                 \x20     default: 4321\n\
                 \x20 healthcheck:\n\
                 \x20   httpGet: \"http://localhost:{{{{APP_PORT}}}}/health\"",
                findings.join(", service ")
            );
            eprintln!("{}", msg);
        }
    }

    /// One entry per offending service: `'name' (place, place)`.
    fn literal_local_port_findings(&self) -> Vec<String> {
        use std::sync::OnceLock;
        static LOCAL_PORT: OnceLock<regex::Regex> = OnceLock::new();
        let local_port = LOCAL_PORT.get_or_init(|| {
            regex::Regex::new(r"(?:localhost|127\.0\.0\.1|0\.0\.0\.0):\d{2,5}").unwrap()
        });

        let mut findings: Vec<String> = Vec::new();
        for (name, service) in &self.services {
            let mut places: Vec<&str> = Vec::new();
            if let Some(hc) = &service.healthcheck
                && hc.get_http_url().is_some_and(|u| local_port.is_match(u))
            {
                places.push("healthcheck");
            }
            if service
                .startup_message
                .as_deref()
                .is_some_and(|m| local_port.is_match(m))
            {
                places.push("startup_message");
            }
            if service
                .process
                .as_deref()
                .is_some_and(|p| local_port.is_match(p))
            {
                places.push("process");
            }
            if service.environment.values().any(|v| local_port.is_match(v)) {
                places.push("environment");
            }
            // Docker port mappings: a literal host port ("5433:5432") is the
            // same antipattern; the fed way is "{{DB_PORT}}:5432".
            if service.ports.iter().any(|p| {
                p.split(':').next().is_some_and(|host| {
                    !host.is_empty() && host.chars().all(|c| c.is_ascii_digit())
                })
            }) {
                places.push("ports");
            }
            if !places.is_empty() {
                findings.push(format!("'{}' ({})", name, places.join(", ")));
            }
        }
        // services is a HashMap — sort for deterministic warning output
        findings.sort();
        findings
    }
}

/// Validate parameter value against constraints
pub fn validate_parameter_value(
    param_name: &str,
    value: &str,
    param_type: Option<&str>,
    either: &[String],
) -> Result<()> {
    // Type validation
    if param_type == Some("port") {
        let port: u16 = value.parse().map_err(|_| Error::InvalidParameter {
            name: param_name.to_string(),
            reason: format!("Invalid port value: {}", value),
        })?;

        if port == 0 {
            return Err(Error::InvalidParameter {
                name: param_name.to_string(),
                reason: "Port must be between 1 and 65535".to_string(),
            });
        }
    }

    // Either constraint validation
    if !either.is_empty() && !either.contains(&value.to_string()) {
        return Err(Error::InvalidParameter {
            name: param_name.to_string(),
            reason: format!("Value '{}' not in allowed values: {:?}", value, either),
        });
    }

    Ok(())
}

/// Validate resource limits for a service
fn validate_resource_limits(service_name: &str, resources: &super::ResourceLimits) -> Result<()> {
    // Validate memory limit
    if let Some(ref memory) = resources.memory {
        validate_memory_string(memory).map_err(|e| {
            Error::Validation(format!(
                "Service '{}': invalid memory limit '{}': {}",
                service_name, memory, e
            ))
        })?;
    }

    // Validate memory reservation
    if let Some(ref memory_reservation) = resources.memory_reservation {
        validate_memory_string(memory_reservation).map_err(|e| {
            Error::Validation(format!(
                "Service '{}': invalid memory_reservation '{}': {}",
                service_name, memory_reservation, e
            ))
        })?;
    }

    // Validate memory swap
    if let Some(ref memory_swap) = resources.memory_swap {
        // memory_swap can be "0" to disable swap, or a memory string
        if memory_swap != "0" && memory_swap != "-1" {
            validate_memory_string(memory_swap).map_err(|e| {
                Error::Validation(format!(
                    "Service '{}': invalid memory_swap '{}': {}",
                    service_name, memory_swap, e
                ))
            })?;
        }
    }

    // Validate CPUs
    if let Some(ref cpus) = resources.cpus {
        validate_cpus_string(cpus).map_err(|e| {
            Error::Validation(format!(
                "Service '{}': invalid cpus '{}': {}",
                service_name, cpus, e
            ))
        })?;
    }

    // Validate CPU shares (must be > 0, typically 2-262144)
    if let Some(cpu_shares) = resources.cpu_shares {
        if cpu_shares == 0 {
            return Err(Error::Validation(format!(
                "Service '{}': cpu_shares must be greater than 0",
                service_name
            )));
        }
        if cpu_shares > 262144 {
            return Err(Error::Validation(format!(
                "Service '{}': cpu_shares {} exceeds maximum of 262144",
                service_name, cpu_shares
            )));
        }
    }

    // Validate PIDs limit (must be > 0 or -1 for unlimited)
    if let Some(pids) = resources.pids
        && pids == 0
    {
        return Err(Error::Validation(format!(
            "Service '{}': pids limit must be greater than 0",
            service_name
        )));
    }

    // Validate nofile limit (must be > 0)
    if let Some(nofile) = resources.nofile {
        if nofile == 0 {
            return Err(Error::Validation(format!(
                "Service '{}': nofile limit must be greater than 0",
                service_name
            )));
        }
        // Typical max on Linux is 1048576 (2^20), but we'll be lenient
        if nofile > 1048576 {
            return Err(Error::Validation(format!(
                "Service '{}': nofile limit {} exceeds typical maximum of 1048576",
                service_name, nofile
            )));
        }
    }

    Ok(())
}

/// Validate memory string format (e.g., "512m", "2g", "1024mb")
fn validate_memory_string(memory: &str) -> std::result::Result<(), String> {
    if memory.is_empty() {
        return Err("memory string cannot be empty".to_string());
    }

    // Must start with a digit
    if !memory.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Err("memory string must start with a number".to_string());
    }

    // Find where the suffix starts (first non-digit, non-dot character)
    let suffix_start = memory
        .chars()
        .position(|c| !c.is_ascii_digit() && c != '.')
        .unwrap_or(memory.len());

    let num_part = &memory[..suffix_start];
    let suffix = &memory[suffix_start..];

    if num_part.is_empty() {
        return Err("memory string must start with a number".to_string());
    }

    // Parse numeric part
    let value: f64 = num_part
        .parse()
        .map_err(|_| format!("invalid numeric value '{}'", num_part))?;

    if value <= 0.0 {
        return Err("memory value must be positive".to_string());
    }

    // Validate suffix
    let suffix_lower = suffix.to_lowercase();
    match suffix_lower.as_str() {
        "" | "b" => {
            // Plain bytes, ensure it's a reasonable value (at least 4KB)
            if value < 4096.0 {
                return Err(
                    "memory must be at least 4KB (4096 bytes or use k/m/g suffix)".to_string(),
                );
            }
        }
        "k" | "kb" => {
            if value < 4.0 {
                return Err("memory must be at least 4KB".to_string());
            }
        }
        "m" | "mb" => {
            // Common case, no minimum check needed
        }
        "g" | "gb" => {
            // Common case, no minimum check needed
        }
        "t" | "tb" => {
            // Terabytes - warn if excessive
            if value > 100.0 {
                return Err("memory limit exceeds 100TB, likely a typo".to_string());
            }
        }
        _ => {
            return Err(format!(
                "invalid memory suffix '{}' (valid: b, k, kb, m, mb, g, gb, t, tb)",
                suffix
            ));
        }
    }

    Ok(())
}

/// Validate CPUs string (e.g., "0.5", "2.0", "4")
fn validate_cpus_string(cpus: &str) -> std::result::Result<(), String> {
    if cpus.is_empty() {
        return Err("cpus string cannot be empty".to_string());
    }

    let value: f64 = cpus
        .parse()
        .map_err(|_| format!("invalid cpus value '{}', must be a decimal number", cpus))?;

    if value <= 0.0 {
        return Err("cpus value must be positive".to_string());
    }

    if value > 1024.0 {
        return Err(format!(
            "cpus value {} exceeds reasonable maximum of 1024",
            value
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Parameter, Service};

    #[test]
    fn literal_local_ports_detected_once_per_service() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("npm run dev".to_string()),
                startup_message: Some("http://localhost:4321".to_string()),
                healthcheck: Some(HealthCheck::HttpGet {
                    http_get: "http://localhost:4321/login".to_string(),
                    timeout: None,
                }),
                ..Default::default()
            },
        );
        config.services.insert(
            "db".to_string(),
            Service {
                image: Some("postgres:16".to_string()),
                ports: vec!["5433:5432".to_string()],
                ..Default::default()
            },
        );

        let findings = config.literal_local_port_findings();
        assert_eq!(
            findings,
            vec![
                "'app' (healthcheck, startup_message)".to_string(),
                "'db' (ports)".to_string(),
            ]
        );
    }

    #[test]
    fn interpolated_ports_produce_no_findings() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("npm run dev -- --port {{APP_PORT}}".to_string()),
                startup_message: Some("http://localhost:{{APP_PORT}}".to_string()),
                healthcheck: Some(HealthCheck::HttpGet {
                    http_get: "http://localhost:{{APP_PORT}}/login".to_string(),
                    timeout: None,
                }),
                environment: [(
                    "DATABASE_URL".to_string(),
                    "postgres://localhost:{{DB_PORT}}/x".to_string(),
                )]
                .into(),
                ..Default::default()
            },
        );
        config.services.insert(
            "db".to_string(),
            Service {
                image: Some("postgres:16".to_string()),
                ports: vec!["{{DB_PORT}}:5432".to_string()],
                ..Default::default()
            },
        );

        assert!(config.literal_local_port_findings().is_empty());
    }

    #[test]
    fn test_validate_circular_dependency() {
        use crate::config::DependsOn;
        let mut config = Config::default();

        let service_a = Service {
            process: Some("echo a".to_string()),
            depends_on: vec![DependsOn::Simple("b".to_string())],
            ..Default::default()
        };

        let service_b = Service {
            process: Some("echo b".to_string()),
            depends_on: vec![DependsOn::Simple("a".to_string())],
            ..Default::default()
        };

        config.services.insert("a".to_string(), service_a);
        config.services.insert("b".to_string(), service_b);

        assert!(matches!(
            config.validate(),
            Err(Error::CircularDependency(_))
        ));
    }

    #[test]
    fn test_circular_dependency_error_shows_cycle_path() {
        use crate::config::DependsOn;
        let mut config = Config::default();

        // Create a 3-service cycle: x -> y -> z -> x
        let service_x = Service {
            process: Some("echo x".to_string()),
            depends_on: vec![DependsOn::Simple("y".to_string())],
            ..Default::default()
        };

        let service_y = Service {
            process: Some("echo y".to_string()),
            depends_on: vec![DependsOn::Simple("z".to_string())],
            ..Default::default()
        };

        let service_z = Service {
            process: Some("echo z".to_string()),
            depends_on: vec![DependsOn::Simple("x".to_string())],
            ..Default::default()
        };

        config.services.insert("x".to_string(), service_x);
        config.services.insert("y".to_string(), service_y);
        config.services.insert("z".to_string(), service_z);

        let result = config.validate();
        assert!(result.is_err());

        // Verify the error message contains the cycle path
        let error = result.unwrap_err();
        let error_msg = error.to_string();

        // The cycle should show something like "x -> y -> z -> x"
        assert!(
            error_msg.contains("->"),
            "Error should show cycle path with arrows, got: {}",
            error_msg
        );

        // At least some of the services should appear in the message
        let contains_services =
            error_msg.contains("x") || error_msg.contains("y") || error_msg.contains("z");
        assert!(
            contains_services,
            "Error should mention services in the cycle, got: {}",
            error_msg
        );
    }

    #[test]
    fn test_structured_dependency_on_missing_service_rejected() {
        use crate::config::{DependencyFailurePolicy, DependsOn};
        let mut config = Config::default();

        let service = Service {
            process: Some("echo app".to_string()),
            depends_on: vec![DependsOn::Structured {
                service: "databse".to_string(), // typo of "database"
                on_failure: DependencyFailurePolicy::default(),
            }],
            ..Default::default()
        };

        config.services.insert("app".to_string(), service);
        config.services.insert(
            "database".to_string(),
            Service {
                process: Some("echo db".to_string()),
                ..Default::default()
            },
        );

        let result = config.validate();
        assert!(
            result.is_err(),
            "structured dependency on a missing service must fail validation"
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("depends on non-existent service 'databse'"));
    }

    #[test]
    fn test_validate_missing_entrypoint() {
        let config = Config {
            entrypoint: Some("nonexistent".to_string()),
            ..Default::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_service_script_name_conflict() {
        use crate::config::Script;

        let mut config = Config::default();

        let service = Service {
            process: Some("echo service".to_string()),
            ..Default::default()
        };

        let script = Script {
            script: "echo script".to_string(),
            cwd: None,
            depends_on: vec![],
            ..Default::default()
        };

        config.services.insert("app".to_string(), service);
        config.scripts.insert("app".to_string(), script);

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("same name as a service"));
    }

    #[test]
    fn test_script_missing_dependency() {
        use crate::config::Script;

        let mut config = Config::default();

        let script = Script {
            script: "echo test".to_string(),
            cwd: None,
            depends_on: vec!["nonexistent".to_string()],
            ..Default::default()
        };

        config.scripts.insert("test".to_string(), script);

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("depends on non-existent service or script"));
    }

    #[test]
    fn test_script_valid_service_dependency() {
        use crate::config::Script;

        let mut config = Config::default();

        let service = Service {
            process: Some("echo service".to_string()),
            ..Default::default()
        };

        let script = Script {
            script: "echo script".to_string(),
            cwd: None,
            depends_on: vec!["database".to_string()],
            ..Default::default()
        };

        config.services.insert("database".to_string(), service);
        config.scripts.insert("migrate".to_string(), script);

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_keep_services_with_isolated_is_rejected() {
        use crate::config::Script;

        let mut config = Config::default();
        config.scripts.insert(
            "scenario".to_string(),
            Script {
                script: "echo hi".to_string(),
                isolated: true,
                keep_services: true,
                ..Default::default()
            },
        );

        let err = config
            .validate()
            .expect_err("keep_services + isolated must be rejected")
            .to_string();
        // Names the script and both conflicting keys so the message is actionable.
        assert!(err.contains("scenario"));
        assert!(err.contains("keep_services"));
        assert!(err.contains("isolated"));
    }

    #[test]
    fn test_keep_services_without_isolated_is_allowed() {
        use crate::config::Script;

        let mut config = Config::default();
        config.scripts.insert(
            "scenario".to_string(),
            Script {
                script: "echo hi".to_string(),
                keep_services: true,
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_script_valid_script_dependency() {
        use crate::config::Script;

        let mut config = Config::default();

        let script1 = Script {
            script: "echo script1".to_string(),
            cwd: None,
            depends_on: vec![],
            ..Default::default()
        };

        let script2 = Script {
            script: "echo script2".to_string(),
            cwd: None,
            depends_on: vec!["script1".to_string()],
            ..Default::default()
        };

        config.scripts.insert("script1".to_string(), script1);
        config.scripts.insert("script2".to_string(), script2);

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_memory_string_valid() {
        assert!(validate_memory_string("512m").is_ok());
        assert!(validate_memory_string("2g").is_ok());
        assert!(validate_memory_string("1024mb").is_ok());
        assert!(validate_memory_string("1.5g").is_ok());
        assert!(validate_memory_string("4096").is_ok()); // bytes
        assert!(validate_memory_string("100000k").is_ok());
    }

    #[test]
    fn test_validate_memory_string_invalid() {
        assert!(validate_memory_string("").is_err()); // empty
        assert!(validate_memory_string("m512").is_err()); // no number
        assert!(validate_memory_string("0m").is_err()); // zero
        assert!(validate_memory_string("-512m").is_err()); // negative
        assert!(validate_memory_string("512x").is_err()); // invalid suffix
        assert!(validate_memory_string("100").is_err()); // too small for bytes
        assert!(validate_memory_string("200t").is_err()); // excessive terabytes
    }

    #[test]
    fn test_validate_cpus_string_valid() {
        assert!(validate_cpus_string("0.5").is_ok());
        assert!(validate_cpus_string("1").is_ok());
        assert!(validate_cpus_string("2.0").is_ok());
        assert!(validate_cpus_string("4").is_ok());
        assert!(validate_cpus_string("16.5").is_ok());
    }

    #[test]
    fn test_validate_cpus_string_invalid() {
        assert!(validate_cpus_string("").is_err()); // empty
        assert!(validate_cpus_string("0").is_err()); // zero
        assert!(validate_cpus_string("-1").is_err()); // negative
        assert!(validate_cpus_string("abc").is_err()); // not a number
        assert!(validate_cpus_string("2000").is_err()); // excessive
    }

    #[test]
    fn test_validate_resource_limits_valid() {
        use crate::config::ResourceLimits;

        let resources = ResourceLimits {
            memory: Some("512m".to_string()),
            memory_reservation: Some("256m".to_string()),
            memory_swap: Some("1g".to_string()),
            cpus: Some("2.0".to_string()),
            cpu_shares: Some(1024),
            pids: Some(100),
            nofile: Some(1024),
            strict_limits: false,
        };

        assert!(validate_resource_limits("test-service", &resources).is_ok());
    }

    #[test]
    fn test_validate_resource_limits_invalid_memory() {
        use crate::config::ResourceLimits;

        let resources = ResourceLimits {
            memory: Some("invalid".to_string()),
            memory_reservation: None,
            memory_swap: None,
            cpus: None,
            cpu_shares: None,
            pids: None,
            nofile: None,
            strict_limits: false,
        };

        let result = validate_resource_limits("test-service", &resources);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid memory limit")
        );
    }

    #[test]
    fn test_validate_resource_limits_invalid_cpus() {
        use crate::config::ResourceLimits;

        let resources = ResourceLimits {
            memory: None,
            memory_reservation: None,
            memory_swap: None,
            cpus: Some("0".to_string()),
            cpu_shares: None,
            pids: None,
            nofile: None,
            strict_limits: false,
        };

        let result = validate_resource_limits("test-service", &resources);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid cpus"));
    }

    #[test]
    fn test_validate_resource_limits_zero_cpu_shares() {
        use crate::config::ResourceLimits;

        let resources = ResourceLimits {
            memory: None,
            memory_reservation: None,
            memory_swap: None,
            cpus: None,
            cpu_shares: Some(0),
            pids: None,
            nofile: None,
            strict_limits: false,
        };

        let result = validate_resource_limits("test-service", &resources);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cpu_shares must be greater than 0")
        );
    }

    #[test]
    fn test_validate_resource_limits_zero_pids() {
        use crate::config::ResourceLimits;

        let resources = ResourceLimits {
            memory: None,
            memory_reservation: None,
            memory_swap: None,
            cpus: None,
            cpu_shares: None,
            pids: Some(0),
            nofile: None,
            strict_limits: false,
        };

        let result = validate_resource_limits("test-service", &resources);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("pids limit must be greater than 0")
        );
    }

    #[test]
    fn test_script_circular_dependency_simple() {
        use crate::config::Script;

        let mut config = Config::default();

        // Create a 2-script cycle: script_a -> script_b -> script_a
        let script_a = Script {
            script: "echo a".to_string(),
            cwd: None,
            depends_on: vec!["script_b".to_string()],
            ..Default::default()
        };

        let script_b = Script {
            script: "echo b".to_string(),
            cwd: None,
            depends_on: vec!["script_a".to_string()],
            ..Default::default()
        };

        config.scripts.insert("script_a".to_string(), script_a);
        config.scripts.insert("script_b".to_string(), script_b);

        let result = config.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::CircularDependency(_)));
    }

    #[test]
    fn test_script_circular_dependency_three_way() {
        use crate::config::Script;

        let mut config = Config::default();

        // Create a 3-script cycle: x -> y -> z -> x
        let script_x = Script {
            script: "echo x".to_string(),
            cwd: None,
            depends_on: vec!["y".to_string()],
            ..Default::default()
        };

        let script_y = Script {
            script: "echo y".to_string(),
            cwd: None,
            depends_on: vec!["z".to_string()],
            ..Default::default()
        };

        let script_z = Script {
            script: "echo z".to_string(),
            cwd: None,
            depends_on: vec!["x".to_string()],
            ..Default::default()
        };

        config.scripts.insert("x".to_string(), script_x);
        config.scripts.insert("y".to_string(), script_y);
        config.scripts.insert("z".to_string(), script_z);

        let result = config.validate();
        assert!(result.is_err());

        // Verify the error message contains the cycle path
        let error = result.unwrap_err();
        let error_msg = error.to_string();

        // The cycle should show something like "x -> y -> z -> x"
        assert!(
            error_msg.contains("->"),
            "Error should show cycle path with arrows, got: {}",
            error_msg
        );
    }

    #[test]
    fn test_script_depending_on_service_is_valid() {
        use crate::config::Script;

        let mut config = Config::default();

        let service = Service {
            process: Some("echo service".to_string()),
            ..Default::default()
        };

        let script = Script {
            script: "echo script".to_string(),
            cwd: None,
            depends_on: vec!["database".to_string()],
            ..Default::default()
        };

        config.services.insert("database".to_string(), service);
        config.scripts.insert("migrate".to_string(), script);

        // This should be valid - script→service dependencies are allowed
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_script_chain_with_service_no_cycle() {
        use crate::config::Script;

        let mut config = Config::default();

        // Create a chain: script_a -> script_b -> service
        // This is valid - no cycle
        let service = Service {
            process: Some("echo db".to_string()),
            ..Default::default()
        };

        let script_b = Script {
            script: "echo b".to_string(),
            cwd: None,
            depends_on: vec!["db".to_string()],
            ..Default::default()
        };

        let script_a = Script {
            script: "echo a".to_string(),
            cwd: None,
            depends_on: vec!["script_b".to_string()],
            ..Default::default()
        };

        config.services.insert("db".to_string(), service);
        config.scripts.insert("script_b".to_string(), script_b);
        config.scripts.insert("script_a".to_string(), script_a);

        // This should be valid - no script→script cycle
        assert!(config.validate().is_ok());
    }

    // ==================== Service Type Validation (SF-00115) ====================

    #[test]
    fn test_undefined_service_type_rejected() {
        let mut config = Config::default();
        config
            .services
            .insert("empty".to_string(), Service::default());

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("has no type defined"));
        assert!(err.contains("empty"));
    }

    #[test]
    fn test_process_service_accepted() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_docker_service_accepted() {
        let mut config = Config::default();
        config.services.insert(
            "db".to_string(),
            Service {
                image: Some("postgres:15".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_gradle_service_accepted() {
        let mut config = Config::default();
        config.services.insert(
            "api".to_string(),
            Service {
                gradle_task: Some(":api:run".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_compose_service_accepted() {
        let mut config = Config::default();
        config.services.insert(
            "web".to_string(),
            Service {
                compose_file: Some("docker-compose.yml".to_string()),
                compose_service: Some("web".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    // ==================== Duration Validation (SF-00116) ====================

    #[test]
    fn test_invalid_grace_period_rejected() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                grace_period: Some("invalid".to_string()),
                ..Default::default()
            },
        );

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid grace_period"));
        assert!(err.contains("invalid"));
    }

    #[test]
    fn test_valid_grace_period_accepted() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                grace_period: Some("30s".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_startup_timeout_rejected() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                startup_timeout: Some("not-a-duration".to_string()),
                ..Default::default()
            },
        );

        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("invalid startup_timeout"));
        assert!(err.contains("not-a-duration"));
    }

    #[test]
    fn test_valid_startup_timeout_accepted() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                startup_timeout: Some("5m".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_healthcheck_timeout_rejected() {
        use crate::config::HealthCheck;

        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                healthcheck: Some(HealthCheck::HttpGet {
                    http_get: "http://localhost:8080/health".to_string(),
                    timeout: Some("not-a-duration".to_string()),
                }),
                ..Default::default()
            },
        );

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid healthcheck timeout"));
    }

    #[test]
    fn test_valid_healthcheck_timeout_accepted() {
        use crate::config::HealthCheck;

        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                healthcheck: Some(HealthCheck::HttpGet {
                    http_get: "http://localhost:8080/health".to_string(),
                    timeout: Some("5s".to_string()),
                }),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_healthcheck_no_timeout_accepted() {
        use crate::config::HealthCheck;

        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                healthcheck: Some(HealthCheck::Command("curl localhost".to_string())),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_command_healthcheck_timeout_rejected() {
        use crate::config::HealthCheck;

        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("echo hello".to_string()),
                healthcheck: Some(HealthCheck::CommandMap {
                    command: "curl localhost".to_string(),
                    timeout: Some("bogus".to_string()),
                }),
                ..Default::default()
            },
        );

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid healthcheck timeout"));
    }

    #[test]
    fn test_invalid_script_timeout_rejected() {
        use crate::config::Script;

        let mut config = Config::default();
        config.scripts.insert(
            "migrate".to_string(),
            Script {
                script: "echo migrate".to_string(),
                cwd: None,
                depends_on: vec![],
                timeout: Some("nope".to_string()),
                ..Default::default()
            },
        );

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid timeout"));
        assert!(err.contains("migrate"));
    }

    #[test]
    fn test_valid_script_timeout_accepted() {
        use crate::config::Script;

        let mut config = Config::default();
        config.scripts.insert(
            "migrate".to_string(),
            Script {
                script: "echo migrate".to_string(),
                cwd: None,
                depends_on: vec![],
                timeout: Some("5m".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    // ==================== Multi-Type Rejection (SF-00152) ====================

    #[test]
    fn test_multiple_service_types_rejected() {
        let mut config = Config::default();
        config.services.insert(
            "confused".to_string(),
            Service {
                process: Some("npm run".to_string()),
                image: Some("postgres:15".to_string()),
                ..Default::default()
            },
        );

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("multiple type-defining fields"));
        assert!(err.contains("process"));
        assert!(err.contains("image"));
    }

    #[test]
    fn test_single_service_type_accepted() {
        let mut config = Config::default();
        config.services.insert(
            "app".to_string(),
            Service {
                process: Some("npm run".to_string()),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    // ==================== Parameter Default Validation (SF-00153) ====================

    #[test]
    fn test_parameter_default_violating_either_rejected() {
        use crate::config::Parameter;

        let mut config = Config::default();
        config.parameters.insert(
            "ENV".to_string(),
            Parameter {
                either: vec![
                    "development".to_string(),
                    "staging".to_string(),
                    "production".to_string(),
                ],
                default: Some(serde_yaml::Value::String("invalid".to_string())),
                ..Default::default()
            },
        );

        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not in allowed values"));
        assert!(err.contains("ENV"));
    }

    #[test]
    fn test_parameter_default_satisfying_either_accepted() {
        use crate::config::Parameter;

        let mut config = Config::default();
        config.parameters.insert(
            "ENV".to_string(),
            Parameter {
                either: vec![
                    "development".to_string(),
                    "staging".to_string(),
                    "production".to_string(),
                ],
                default: Some(serde_yaml::Value::String("development".to_string())),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parameter_no_either_no_validation() {
        use crate::config::Parameter;

        let mut config = Config::default();
        config.parameters.insert(
            "PORT".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String("8080".to_string())),
                ..Default::default()
            },
        );

        assert!(config.validate().is_ok());
    }

    // ========================================================================
    // Secret parameter validation
    // ========================================================================

    #[test]
    fn secret_with_default_is_rejected() {
        let mut config = Config::default();
        config.parameters.insert(
            "DB_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                default: Some(serde_yaml::Value::String("oops".to_string())),
                ..Default::default()
            },
        );
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("must not have a default"));
    }

    #[test]
    fn secret_with_env_specific_value_is_rejected() {
        let mut config = Config::default();
        config.parameters.insert(
            "DB_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                production: Some(serde_yaml::Value::String("prod-secret".to_string())),
                ..Default::default()
            },
        );
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("must not have environment-specific")
        );
    }

    #[test]
    fn secret_with_unknown_source_is_rejected() {
        let mut config = Config::default();
        config.parameters.insert(
            "DB_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("1password".to_string()),
                ..Default::default()
            },
        );
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("unknown source '1password'"));
    }

    #[test]
    fn secret_with_manual_source_is_valid() {
        let mut config = Config::default();
        config.parameters.insert(
            "GITHUB_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                description: Some("GitHub OAuth secret".to_string()),
                ..Default::default()
            },
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn generated_secret_with_generated_secrets_file_is_valid() {
        let mut config = Config {
            generated_secrets_file: Some(".env.secrets".to_string()),
            ..Default::default()
        };
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn generated_secret_without_generated_secrets_file_is_valid() {
        // No generated_secrets_file needed — fed defaults to
        // .fed/secrets.generated.env (the key is deprecated).
        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn manual_secret_without_generated_secrets_file_is_valid() {
        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn secret_with_either_is_rejected() {
        let mut config = Config {
            generated_secrets_file: Some(".env.secrets".to_string()),
            ..Default::default()
        };
        config.parameters.insert(
            "DB_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                either: vec!["a".to_string(), "b".to_string()],
                ..Default::default()
            },
        );
        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("either"),
            "Error should mention 'either': {}",
            msg
        );
    }
}
