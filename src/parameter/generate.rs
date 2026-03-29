//! DAG-based parameter generation.
//!
//! Resolves `generate` commands in dependency order. Parameters that
//! reference other parameters via `{{PARAM}}` form a DAG. The DAG is
//! topologically sorted and resolved from roots to leaves.
//!
//! For `type: secret` parameters: generated values are persisted.
//! If a root secret is regenerated (was missing), its dependents are
//! invalidated and regenerated regardless of whether they have existing
//! values.

use crate::config::Parameter;
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet, VecDeque};

/// Result of resolving a single parameter's generate command.
#[derive(Debug)]
pub struct GenerateResult {
    pub name: String,
    pub value: String,
    pub was_generated: bool, // true if command was run (vs existing value kept)
}

/// Build a topological ordering of parameters with `generate` commands.
///
/// Returns parameter names in dependency order (roots first).
/// Errors on cycles.
pub fn topological_sort(
    params: &HashMap<String, Parameter>,
) -> Result<Vec<String>> {
    // Only consider params with generate commands.
    let generate_params: HashMap<&str, Vec<String>> = params
        .iter()
        .filter(|(_, p)| p.has_generate())
        .map(|(name, p)| (name.as_str(), p.generate_dependencies()))
        .collect();

    if generate_params.is_empty() {
        return Ok(Vec::new());
    }

    // Build in-degree map.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for (name, deps) in &generate_params {
        in_degree.entry(name).or_insert(0);
        for dep in deps {
            // Only count dependencies that are themselves generate params.
            // Dependencies on non-generate params (ports, defaults) are resolved first.
            if generate_params.contains_key(dep.as_str()) {
                *in_degree.entry(name).or_insert(0) += 1;
                dependents.entry(dep.as_str()).or_default().push(name);
            }
        }
    }

    // Kahn's algorithm.
    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&name, _)| name)
        .collect();

    let mut sorted = Vec::new();

    while let Some(name) = queue.pop_front() {
        sorted.push(name.to_string());
        if let Some(deps) = dependents.get(name) {
            for dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep);
                    }
                }
            }
        }
    }

    if sorted.len() != generate_params.len() {
        let unresolved: Vec<&str> = generate_params
            .keys()
            .filter(|name| !sorted.iter().any(|s| s == **name))
            .copied()
            .collect();
        return Err(Error::TemplateResolution(format!(
            "Circular dependency in generate commands: {:?}",
            unresolved
        )));
    }

    Ok(sorted)
}

/// Execute a generate command, interpolating `{{PARAM}}` references
/// from already-resolved values.
pub fn run_generate_command(
    command: &str,
    resolved: &HashMap<String, String>,
) -> Result<String> {
    // Interpolate {{PARAM}} references.
    let interpolated = crate::parameter::Resolver::resolve_template_static(
        command, resolved,
    )?;

    // Run via sh -c, capture stdout.
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(&interpolated)
        .output()
        .map_err(|e| Error::TemplateResolution(format!(
            "Failed to run generate command: {e}"
        )))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::TemplateResolution(format!(
            "Generate command failed (exit {}): {}\nCommand: {}",
            output.status.code().unwrap_or(-1),
            stderr.trim(),
            interpolated,
        )));
    }

    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(value)
}

/// Resolve all `generate` parameters in DAG order with invalidation.
///
/// `existing_values`: values already persisted (from .env.secrets).
/// Returns: list of (name, value, was_generated) for all generate params.
pub fn resolve_generate_params(
    params: &HashMap<String, Parameter>,
    existing_values: &HashMap<String, String>,
    resolved_so_far: &HashMap<String, String>,
) -> Result<Vec<GenerateResult>> {
    let order = topological_sort(params)?;

    let mut resolved = resolved_so_far.clone();
    let mut invalidated: HashSet<String> = HashSet::new();
    let mut results = Vec::new();

    for name in &order {
        let param = match params.get(name) {
            Some(p) => p,
            None => continue,
        };

        let cmd = match &param.generate {
            Some(c) => c,
            None => continue,
        };

        let deps = param.generate_dependencies();
        let is_secret = param.is_secret_type();

        // Check if any dependency was just generated (invalidated).
        let any_dep_invalidated = deps.iter().any(|d| invalidated.contains(d));

        // Decide whether to generate or keep existing.
        let should_generate = if !is_secret {
            // Non-secret: always recompute.
            true
        } else if any_dep_invalidated {
            // Secret with invalidated dependency: regenerate.
            true
        } else if existing_values.contains_key(name) {
            // Secret with existing value and no invalidated deps: keep.
            false
        } else {
            // Secret with no existing value: generate.
            true
        };

        if should_generate {
            let value = run_generate_command(cmd, &resolved)?;
            resolved.insert(name.clone(), value.clone());
            invalidated.insert(name.clone());
            results.push(GenerateResult {
                name: name.clone(),
                value,
                was_generated: true,
            });
        } else {
            let value = existing_values[name].clone();
            resolved.insert(name.clone(), value.clone());
            results.push(GenerateResult {
                name: name.clone(),
                value,
                was_generated: false,
            });
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param_with_generate(cmd: &str) -> Parameter {
        Parameter {
            generate: Some(cmd.to_string()),
            ..Default::default()
        }
    }

    fn secret_with_generate(cmd: &str) -> Parameter {
        Parameter {
            param_type: Some("secret".to_string()),
            generate: Some(cmd.to_string()),
            ..Default::default()
        }
    }

    // ── Topological Sort ────────────────────────────────────

    #[test]
    fn topo_sort_no_deps() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), param_with_generate("echo a"));
        params.insert("B".to_string(), param_with_generate("echo b"));

        let order = topological_sort(&params).unwrap();
        assert_eq!(order.len(), 2);
        // Both are roots — order doesn't matter, but both must appear.
        assert!(order.contains(&"A".to_string()));
        assert!(order.contains(&"B".to_string()));
    }

    #[test]
    fn topo_sort_linear_chain() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), param_with_generate("echo a"));
        params.insert("B".to_string(), param_with_generate("echo {{A}} | tr a b"));
        params.insert("C".to_string(), param_with_generate("echo {{B}} | tr b c"));

        let order = topological_sort(&params).unwrap();
        assert_eq!(order, vec!["A", "B", "C"]);
    }

    #[test]
    fn topo_sort_detects_cycle() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), param_with_generate("echo {{B}}"));
        params.insert("B".to_string(), param_with_generate("echo {{A}}"));

        let result = topological_sort(&params);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Circular dependency"), "got: {err}");
    }

    #[test]
    fn topo_sort_ignores_non_generate_deps() {
        let mut params = HashMap::new();
        // DB_PORT has no generate command — it's a port param.
        params.insert(
            "DB_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                ..Default::default()
            },
        );
        // DB_URL depends on DB_PORT but DB_PORT isn't a generate param.
        params.insert(
            "DB_URL".to_string(),
            param_with_generate("echo postgres://localhost:{{DB_PORT}}/db"),
        );

        let order = topological_sort(&params).unwrap();
        // Only DB_URL should appear (it's the only generate param).
        assert_eq!(order, vec!["DB_URL"]);
    }

    // ── Generate Command Execution ──────────────────────────

    #[test]
    fn run_simple_command() {
        let resolved = HashMap::new();
        let value = run_generate_command("echo hello", &resolved).unwrap();
        assert_eq!(value, "hello");
    }

    #[test]
    fn run_command_with_interpolation() {
        let mut resolved = HashMap::new();
        resolved.insert("NAME".to_string(), "world".to_string());
        let value = run_generate_command("echo hello-{{NAME}}", &resolved).unwrap();
        assert_eq!(value, "hello-world");
    }

    #[test]
    fn run_failing_command_returns_error() {
        let resolved = HashMap::new();
        let result = run_generate_command("false", &resolved);
        assert!(result.is_err());
    }

    #[test]
    fn run_command_captures_only_stdout() {
        let resolved = HashMap::new();
        let value = run_generate_command("echo stdout; echo stderr >&2", &resolved).unwrap();
        assert_eq!(value, "stdout");
    }

    // ── DAG Resolution with Invalidation ────────────────────

    #[test]
    fn resolve_fresh_install_generates_all() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), secret_with_generate("echo secret-a"));
        params.insert(
            "B".to_string(),
            secret_with_generate("echo derived-from-{{A}}"),
        );

        let existing = HashMap::new(); // Fresh install.
        let resolved = HashMap::new();

        let results = resolve_generate_params(&params, &existing, &resolved).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].was_generated);
        assert_eq!(results[0].value, "secret-a");
        assert!(results[1].was_generated);
        assert_eq!(results[1].value, "derived-from-secret-a");
    }

    #[test]
    fn resolve_existing_secrets_kept() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), secret_with_generate("echo new-a"));
        params.insert(
            "B".to_string(),
            secret_with_generate("echo derived-from-{{A}}"),
        );

        let mut existing = HashMap::new();
        existing.insert("A".to_string(), "old-a".to_string());
        existing.insert("B".to_string(), "old-b".to_string());
        let resolved = HashMap::new();

        let results = resolve_generate_params(&params, &existing, &resolved).unwrap();

        assert!(!results[0].was_generated); // A kept.
        assert_eq!(results[0].value, "old-a");
        assert!(!results[1].was_generated); // B kept.
        assert_eq!(results[1].value, "old-b");
    }

    #[test]
    fn resolve_missing_root_invalidates_dependents() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), secret_with_generate("echo new-a"));
        params.insert(
            "B".to_string(),
            secret_with_generate("echo derived-from-{{A}}"),
        );

        let mut existing = HashMap::new();
        // A is missing, B has a stale value.
        existing.insert("B".to_string(), "stale-b".to_string());
        let resolved = HashMap::new();

        let results = resolve_generate_params(&params, &existing, &resolved).unwrap();

        assert!(results[0].was_generated); // A generated.
        assert_eq!(results[0].value, "new-a");
        assert!(results[1].was_generated); // B invalidated and regenerated.
        assert_eq!(results[1].value, "derived-from-new-a");
    }

    #[test]
    fn resolve_non_secret_always_recomputes() {
        let mut params = HashMap::new();
        params.insert("HASH".to_string(), param_with_generate("echo abc123"));

        let mut existing = HashMap::new();
        existing.insert("HASH".to_string(), "old-hash".to_string());
        let resolved = HashMap::new();

        let results = resolve_generate_params(&params, &existing, &resolved).unwrap();

        assert!(results[0].was_generated); // Non-secret always recomputes.
        assert_eq!(results[0].value, "abc123");
    }

    #[test]
    fn resolve_uses_already_resolved_params() {
        // DB_PORT is resolved by the port allocator (not a generate param).
        // DB_URL uses {{DB_PORT}} in its generate command.
        let mut params = HashMap::new();
        params.insert(
            "DB_URL".to_string(),
            param_with_generate("echo postgres://localhost:{{DB_PORT}}/db"),
        );

        let existing = HashMap::new();
        let mut resolved = HashMap::new();
        resolved.insert("DB_PORT".to_string(), "5432".to_string());

        let results = resolve_generate_params(&params, &existing, &resolved).unwrap();

        assert_eq!(results[0].value, "postgres://localhost:5432/db");
    }
}
