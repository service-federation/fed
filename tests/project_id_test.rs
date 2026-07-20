//! Tests for the built-in `{{FED_PROJECT_ID}}` template variable.
//!
//! Motivation: localhost cookies are port-agnostic, so parallel isolated stacks
//! of the same project clobber each other's login cookies. Projects need a
//! stable, cookie-safe identifier to suffix cookie names with.
//!
//! Shape: `<project>-<hash>[-<isolation>]`, lowercased and restricted to
//! `[a-z0-9-]`. `<project>` is the sanitized basename of the work_dir, `<hash>`
//! is the 8-hex `hash_work_dir` digest, and `<isolation>` is appended when an
//! isolation session is active.

use std::fs;

use fed::config::{Config, Parameter};
use fed::parameter::{Resolver, compute_project_id};
use fed::service::hash_work_dir;
use tempfile::tempdir;

/// A dir with a controlled basename so we can assert the exact shape.
fn dir_named(parent: &std::path::Path, name: &str) -> std::path::PathBuf {
    let p = parent.join(name);
    fs::create_dir_all(&p).expect("create dir");
    p
}

#[test]
fn test_project_id_shape() {
    let temp = tempdir().unwrap();
    let work_dir = dir_named(temp.path(), "My_Project");

    let id = compute_project_id(&work_dir, None);
    let hash = hash_work_dir(&work_dir);

    assert_eq!(
        id,
        format!("my-project-{}", hash),
        "shape must be <sanitized-basename>-<hash8>"
    );
    // Cookie-safe: lowercase [a-z0-9-] only.
    assert!(
        id.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
        "FED_PROJECT_ID must be restricted to [a-z0-9-], got: {id}"
    );
}

#[test]
fn test_project_id_differs_per_work_dir() {
    let temp = tempdir().unwrap();
    let a = dir_named(temp.path(), "alpha");
    let b = dir_named(temp.path(), "beta");

    assert_ne!(
        compute_project_id(&a, None),
        compute_project_id(&b, None),
        "different work dirs must produce different project ids"
    );
}

#[test]
fn test_project_id_stable_across_calls() {
    let temp = tempdir().unwrap();
    let work_dir = dir_named(temp.path(), "stable");

    assert_eq!(
        compute_project_id(&work_dir, None),
        compute_project_id(&work_dir, None),
        "same work dir must produce a stable project id"
    );
}

#[test]
fn test_project_id_isolation_scope_changes_value() {
    let temp = tempdir().unwrap();
    let work_dir = dir_named(temp.path(), "scoped");

    let shared = compute_project_id(&work_dir, None);
    let isolated = compute_project_id(&work_dir, Some("iso-DEADBEEF"));

    assert_ne!(shared, isolated, "an isolation scope must change the value");
    assert!(
        isolated.starts_with(&shared),
        "the isolated id should extend the shared id, got shared={shared} isolated={isolated}"
    );
    assert!(
        isolated
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
        "isolated id must stay cookie-safe [a-z0-9-], got: {isolated}"
    );
}

#[test]
fn test_resolver_injects_project_id() {
    let temp = tempdir().unwrap();
    let work_dir = dir_named(temp.path(), "resolved");

    let mut resolver = Resolver::new();
    resolver.set_work_dir(&work_dir);

    // A user parameter that references the built-in, to prove it's available for
    // {{...}} resolution without being declared under `parameters:`.
    let mut config = Config::default();
    config.parameters.insert(
        "COOKIE_NAME".to_string(),
        Parameter {
            default: Some(serde_yaml::Value::String(
                "session-{{FED_PROJECT_ID}}".to_string(),
            )),
            ..Default::default()
        },
    );

    resolver.resolve_parameters(&mut config).unwrap();
    let resolved = resolver.get_resolved_parameters();

    let expected = compute_project_id(&work_dir, None);
    assert_eq!(
        resolved.get("FED_PROJECT_ID"),
        Some(&expected),
        "FED_PROJECT_ID must be materialized into the resolved parameter map"
    );
    assert_eq!(
        resolved.get("COOKIE_NAME"),
        Some(&format!("session-{}", expected)),
        "user parameters must be able to interpolate {{{{FED_PROJECT_ID}}}}"
    );
}

#[test]
fn test_resolver_isolation_id_reflected_in_project_id() {
    let temp = tempdir().unwrap();
    let work_dir = dir_named(temp.path(), "iso-resolved");

    let mut resolver = Resolver::new();
    resolver.set_work_dir(&work_dir);
    resolver.set_isolation_id(Some("iso-cafebabe".to_string()));

    let mut config = Config::default();
    resolver.resolve_parameters(&mut config).unwrap();

    let expected = compute_project_id(&work_dir, Some("iso-cafebabe"));
    assert_eq!(
        resolver.get_resolved_parameters().get("FED_PROJECT_ID"),
        Some(&expected),
        "an active isolation id must be reflected in FED_PROJECT_ID"
    );
    assert_ne!(
        expected,
        compute_project_id(&work_dir, None),
        "the isolated FED_PROJECT_ID must differ from the shared one"
    );
}

#[test]
fn test_declaring_fed_project_id_is_rejected() {
    let mut config = Config::default();
    config.parameters.insert(
        "FED_PROJECT_ID".to_string(),
        Parameter {
            default: Some(serde_yaml::Value::String("nope".to_string())),
            ..Default::default()
        },
    );

    let err = config
        .validate()
        .expect_err("declaring the reserved FED_PROJECT_ID must be rejected")
        .to_string();
    assert!(
        err.contains("FED_PROJECT_ID"),
        "the error must name the reserved parameter, got: {err}"
    );
}

/// The resolved map is what scripts read (`get_resolved_parameters`), so this
/// also covers scripts seeing the built-in.
#[test]
fn test_project_id_present_even_with_no_declared_parameters() {
    let temp = tempdir().unwrap();
    let work_dir = dir_named(temp.path(), "bare");

    let mut resolver = Resolver::new();
    resolver.set_work_dir(&work_dir);

    let mut config = Config::default();
    resolver.resolve_parameters(&mut config).unwrap();

    assert!(
        resolver
            .get_resolved_parameters()
            .contains_key("FED_PROJECT_ID"),
        "FED_PROJECT_ID must be present even when no parameters are declared"
    );
}
