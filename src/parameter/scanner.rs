//! Scope the vault query to what a target script transitively references.
//!
//! `fed <script>` never needs every manual secret in the project — only the
//! ones the script (and its transitive dependencies) actually interpolate via
//! `{{NAME}}`. This module derives that set so the resolver can query the vault
//! for those names alone, instead of the project-wide union.
//!
//! # Soundness
//!
//! A script cannot consume a fed-resolved secret without a `{{NAME}}` reference
//! somewhere in its config subtree (see `01-secret-scoping.md`): fed never
//! writes a resolved secret into its own environment, and children inherit only
//! the ambient shell plus the script's own (interpolated) `environment` map.
//! So scanning the transitive config subtree for `{{NAME}}` references captures
//! everything the script can read.
//!
//! # Deliberate over-approximation
//!
//! Rather than enumerate the specific fields that support interpolation (script
//! env, service env, compose, command healthchecks, generate strings, …), the
//! scanner serializes each visited script/service struct and regex-scans
//! *every* string in the value tree. Enumerating fields is a drift hazard: a
//! future field that gains `{{}}` support would silently under-fetch, and
//! under-fetch is the hard failure (a run that should have worked fails on a
//! missing secret). Over-fetch merely matches today's project-wide behavior for
//! that one name — and only if the matched name is actually a declared manual
//! secret, since the resolver intersects this set with the missing-secret
//! analysis, so non-parameter matches drop out for free.

use crate::config::Config;
use std::collections::HashSet;

/// A node in the dependency graph — a script or a service. `depends_on` may
/// point at either kind, so the worklist tracks both.
enum Node {
    Script(String),
    Service(String),
}

/// Compute the set of parameter names a script transitively references.
///
/// Walks the target script's config subtree, following `depends_on` (to both
/// scripts and services, whole-struct including healthchecks) and the
/// `generate` dependencies of any referenced parameter (a needed value may be
/// *generated from* a manual secret). Cycle-safe via a visited set.
///
/// The returned set is an over-approximation of the parameter names the script
/// needs — intended to be intersected with the resolver's missing-secret
/// analysis, so spurious matches that aren't declared secrets cost nothing.
pub fn required_parameter_names(config: &Config, script_name: &str) -> HashSet<String> {
    let mut referenced: HashSet<String> = HashSet::new();
    let mut visited_scripts: HashSet<String> = HashSet::new();
    let mut visited_services: HashSet<String> = HashSet::new();

    let mut work: Vec<Node> = vec![Node::Script(script_name.to_string())];
    while let Some(node) = work.pop() {
        match node {
            Node::Script(name) => {
                if !visited_scripts.insert(name.clone()) {
                    continue;
                }
                let Some(script) = config.scripts.get(&name) else {
                    continue;
                };
                scan_serializable(script, &mut referenced);
                // A script's depends_on entries can be scripts OR services.
                for dep in &script.depends_on {
                    enqueue(config, dep, &mut work);
                }
            }
            Node::Service(name) => {
                if !visited_services.insert(name.clone()) {
                    continue;
                }
                let Some(service) = config.services.get(&name) else {
                    continue;
                };
                scan_serializable(service, &mut referenced);
                for dep in &service.depends_on {
                    enqueue(config, dep.service_name(), &mut work);
                }
            }
        }
    }

    // Close over parameter-to-parameter references. A referenced parameter can
    // pull in another via ANY of its interpolating fields, not just `generate`:
    //   - `generate: "derive --from {{SECRET}}"`
    //   - `default: "prefix-{{SECRET}}"`
    // (The legacy `development`/`develop`/`staging`/`production` fields are
    // inert-capture-only post-fed-8.0 and never reach this scan in practice —
    // `Config::validate()` rejects any parameter that sets them before a run
    // gets this far — but whole-struct scanning still sees them for free.)
    // Whole-struct scanning each referenced Parameter (the same regex sweep used
    // for scripts/services) catches all of them and subsumes the generate-only
    // closure — enumerating fields here would re-introduce the drift hazard the
    // struct-level scan exists to avoid. Recurse over newly discovered names.
    let params = config.get_effective_parameters();
    let mut param_work: Vec<String> = referenced.iter().cloned().collect();
    while let Some(name) = param_work.pop() {
        if let Some(param) = params.get(&name) {
            let mut found = HashSet::new();
            scan_serializable(param, &mut found);
            for dep in found {
                if referenced.insert(dep.clone()) {
                    param_work.push(dep);
                }
            }
        }
    }

    referenced
}

/// Given `poison` — parameter names that cannot be resolved this run (in a
/// scoped run: manual secrets outside the scanned closure that were never
/// fetched) — return every OTHER parameter that transitively references one of
/// them through any interpolating field (`default`, `generate`,
/// environment-specific values, …).
///
/// In a scoped run these tainted parameters must be *deferred* — neither failed
/// for having unresolved templates nor executed (for `generate`) — because
/// their value depends on a secret this run deliberately never fetches. The
/// traversal reuses the same whole-struct `{{NAME}}` sweep the forward scanner
/// uses, so it stays field-drift-proof, and iterates to a fixpoint so chains
/// (`A` ← `B` ← poison) are fully closed. Poison names themselves are not
/// returned; the caller unions them in.
pub fn parameters_tainted_by(config: &Config, poison: &HashSet<String>) -> HashSet<String> {
    let params = config.get_effective_parameters();

    // Precompute each parameter's direct `{{NAME}}` references once.
    let direct: Vec<(String, HashSet<String>)> = params
        .iter()
        .map(|(name, param)| {
            let mut refs = HashSet::new();
            scan_serializable(param, &mut refs);
            (name.clone(), refs)
        })
        .collect();

    let mut tainted: HashSet<String> = HashSet::new();
    loop {
        let mut changed = false;
        for (name, refs) in &direct {
            if tainted.contains(name) || poison.contains(name) {
                continue;
            }
            if refs
                .iter()
                .any(|r| poison.contains(r) || tainted.contains(r))
            {
                tainted.insert(name.clone());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    tainted
}

/// Enqueue a dependency name as a service and/or a script node. A name may
/// resolve to either (or neither, if it's dangling — harmless, it's skipped on
/// visit). Whole-struct scanning happens when the node is popped.
fn enqueue(config: &Config, dep: &str, work: &mut Vec<Node>) {
    if config.scripts.contains_key(dep) {
        work.push(Node::Script(dep.to_string()));
    }
    if config.services.contains_key(dep) {
        work.push(Node::Service(dep.to_string()));
    }
}

/// Serialize a config struct to a value tree and collect every `{{NAME}}`
/// reference found in any string within it.
fn scan_serializable<T: serde::Serialize>(value: &T, out: &mut HashSet<String>) {
    if let Ok(v) = serde_yaml::to_value(value) {
        scan_value(&v, out);
    }
}

/// Recursively collect `{{NAME}}` references from every string in a value tree.
fn scan_value(value: &serde_yaml::Value, out: &mut HashSet<String>) {
    match value {
        serde_yaml::Value::String(s) => {
            for cap in super::resolver::get_template_regex().captures_iter(s) {
                out.insert(cap[1].trim().to_string());
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for item in seq {
                scan_value(item, out);
            }
        }
        serde_yaml::Value::Mapping(map) => {
            for (k, v) in map {
                // Keys are usually literal, but scanning them too is a harmless
                // over-approximation and future-proofs against interpolated keys.
                scan_value(k, out);
                scan_value(v, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DependsOn, HealthCheck, Parameter, Script, Service};

    fn secret_param() -> Parameter {
        Parameter {
            param_type: Some("secret".to_string()),
            source: Some("manual".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn scans_script_environment_references() {
        let mut config = Config::default();
        config.scripts.insert(
            "test:unit".to_string(),
            Script {
                script: "cargo test".to_string(),
                environment: [("KEY".to_string(), "{{API_KEY}}".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        );
        let names = required_parameter_names(&config, "test:unit");
        assert!(names.contains("API_KEY"));
    }

    #[test]
    fn scans_script_command_string() {
        let mut config = Config::default();
        config.scripts.insert(
            "seed".to_string(),
            Script {
                script: "psql {{DATABASE_URL}}".to_string(),
                ..Default::default()
            },
        );
        let names = required_parameter_names(&config, "seed");
        assert!(names.contains("DATABASE_URL"));
    }

    #[test]
    fn ignores_unreferenced_scripts() {
        let mut config = Config::default();
        config.scripts.insert(
            "test:unit".to_string(),
            Script {
                script: "cargo test".to_string(),
                ..Default::default()
            },
        );
        config.scripts.insert(
            "start".to_string(),
            Script {
                script: "cargo run".to_string(),
                environment: [("KEY".to_string(), "{{STRIPE_SECRET}}".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        );
        // Scanning test:unit must not reach start's STRIPE_SECRET.
        let names = required_parameter_names(&config, "test:unit");
        assert!(!names.contains("STRIPE_SECRET"));
        assert!(names.is_empty());
    }

    #[test]
    fn follows_depends_on_into_services_including_healthchecks() {
        let mut config = Config::default();
        config.scripts.insert(
            "e2e".to_string(),
            Script {
                script: "playwright test".to_string(),
                depends_on: vec!["api".to_string()],
                ..Default::default()
            },
        );
        config.services.insert(
            "api".to_string(),
            Service {
                process: Some("node server.js".to_string()),
                // Reference lives only in the healthcheck — must still be found.
                healthcheck: Some(HealthCheck::HttpGet {
                    http_get: "http://localhost/health?token={{HEALTH_TOKEN}}".to_string(),
                    timeout: None,
                }),
                environment: [("DB".to_string(), "{{DB_PASSWORD}}".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        );
        let names = required_parameter_names(&config, "e2e");
        assert!(names.contains("HEALTH_TOKEN"), "healthcheck ref: {names:?}");
        assert!(names.contains("DB_PASSWORD"), "service env ref: {names:?}");
    }

    #[test]
    fn follows_depends_on_into_nested_scripts() {
        let mut config = Config::default();
        config.scripts.insert(
            "outer".to_string(),
            Script {
                script: "echo outer".to_string(),
                depends_on: vec!["inner".to_string()],
                ..Default::default()
            },
        );
        config.scripts.insert(
            "inner".to_string(),
            Script {
                script: "echo {{NESTED_SECRET}}".to_string(),
                ..Default::default()
            },
        );
        let names = required_parameter_names(&config, "outer");
        assert!(names.contains("NESTED_SECRET"));
    }

    #[test]
    fn follows_structured_service_depends_on() {
        let mut config = Config::default();
        config.scripts.insert(
            "run".to_string(),
            Script {
                script: "app".to_string(),
                depends_on: vec!["web".to_string()],
                ..Default::default()
            },
        );
        config.services.insert(
            "web".to_string(),
            Service {
                process: Some("web".to_string()),
                depends_on: vec![DependsOn::Structured {
                    service: "db".to_string(),
                    on_failure: Default::default(),
                }],
                ..Default::default()
            },
        );
        config.services.insert(
            "db".to_string(),
            Service {
                image: Some("postgres".to_string()),
                environment: [("PW".to_string(), "{{PG_PASSWORD}}".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        );
        let names = required_parameter_names(&config, "run");
        assert!(
            names.contains("PG_PASSWORD"),
            "transitive service: {names:?}"
        );
    }

    #[test]
    fn closes_over_generate_dependencies() {
        // Script references {{X}}; X is generated from {{SECRET}}. Both needed.
        let mut config = Config::default();
        config.scripts.insert(
            "run".to_string(),
            Script {
                script: "app {{DERIVED}}".to_string(),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DERIVED".to_string(),
            Parameter {
                generate: Some("derive --from {{SECRET}}".to_string()),
                ..Default::default()
            },
        );
        config
            .parameters
            .insert("SECRET".to_string(), secret_param());
        let names = required_parameter_names(&config, "run");
        assert!(names.contains("DERIVED"));
        assert!(
            names.contains("SECRET"),
            "generate dependency must be pulled in: {names:?}"
        );
    }

    #[test]
    fn closes_over_default_interpolation() {
        // Script references {{DERIVED}}; DERIVED has no generate, but its
        // `default` interpolates {{SECRET}}. SECRET must still be pulled in —
        // the generate-only closure missed this.
        let mut config = Config::default();
        config.scripts.insert(
            "run".to_string(),
            Script {
                script: "app {{DERIVED}}".to_string(),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DERIVED".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String("prefix-{{SECRET}}".to_string())),
                ..Default::default()
            },
        );
        config
            .parameters
            .insert("SECRET".to_string(), secret_param());
        let names = required_parameter_names(&config, "run");
        assert!(
            names.contains("SECRET"),
            "a default that interpolates a secret must pull it in: {names:?}"
        );
    }

    #[test]
    fn closes_over_chained_default_and_generate() {
        // script → A, A.default → {{B}}, B.generate → {{C}}. All of A, B, C must
        // be in the set — the closure recurses over newly discovered names and
        // mixes default- and generate-based references.
        let mut config = Config::default();
        config.scripts.insert(
            "run".to_string(),
            Script {
                script: "app {{A}}".to_string(),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "A".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String("{{B}}".to_string())),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "B".to_string(),
            Parameter {
                generate: Some("gen --from {{C}}".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert("C".to_string(), secret_param());
        let names = required_parameter_names(&config, "run");
        assert!(names.contains("A"), "{names:?}");
        assert!(names.contains("B"), "{names:?}");
        assert!(names.contains("C"), "chained ref must reach C: {names:?}");
    }

    #[test]
    fn cycle_in_depends_on_terminates() {
        let mut config = Config::default();
        config.scripts.insert(
            "a".to_string(),
            Script {
                script: "echo {{A_SECRET}}".to_string(),
                depends_on: vec!["b".to_string()],
                ..Default::default()
            },
        );
        config.scripts.insert(
            "b".to_string(),
            Script {
                script: "echo {{B_SECRET}}".to_string(),
                depends_on: vec!["a".to_string()],
                ..Default::default()
            },
        );
        let names = required_parameter_names(&config, "a");
        assert!(names.contains("A_SECRET"));
        assert!(names.contains("B_SECRET"));
    }

    #[test]
    fn taint_closes_over_default_and_generate_chains() {
        // poison = {SECRET}. A.default → {{B}}, B.generate → {{SECRET}}.
        // Both A and B are tainted; an unrelated CLEAN param is not.
        let mut config = Config::default();
        config.parameters.insert(
            "A".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String("{{B}}".to_string())),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "B".to_string(),
            Parameter {
                generate: Some("gen --from {{SECRET}}".to_string()),
                ..Default::default()
            },
        );
        config
            .parameters
            .insert("SECRET".to_string(), secret_param());
        config.parameters.insert(
            "CLEAN".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String("literal".to_string())),
                ..Default::default()
            },
        );

        let poison = HashSet::from(["SECRET".to_string()]);
        let tainted = parameters_tainted_by(&config, &poison);
        assert!(tainted.contains("A"), "chained default taint: {tainted:?}");
        assert!(tainted.contains("B"), "direct generate taint: {tainted:?}");
        assert!(
            !tainted.contains("CLEAN"),
            "unrelated param must not be tainted: {tainted:?}"
        );
        assert!(
            !tainted.contains("SECRET"),
            "poison names are not returned; the caller unions them: {tainted:?}"
        );
    }

    #[test]
    fn taint_empty_when_no_poison() {
        let mut config = Config::default();
        config.parameters.insert(
            "A".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String("{{B}}".to_string())),
                ..Default::default()
            },
        );
        let tainted = parameters_tainted_by(&config, &HashSet::new());
        assert!(
            tainted.is_empty(),
            "no poison → nothing tainted: {tainted:?}"
        );
    }

    #[test]
    fn dangling_dependency_is_ignored() {
        let mut config = Config::default();
        config.scripts.insert(
            "a".to_string(),
            Script {
                script: "echo hi".to_string(),
                depends_on: vec!["ghost".to_string()],
                ..Default::default()
            },
        );
        // Must not panic; ghost simply contributes nothing.
        let names = required_parameter_names(&config, "a");
        assert!(names.is_empty());
    }
}
