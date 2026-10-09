//! `examples/variants-example.yaml` must stay valid, resolvable, and honest
//! about what it demonstrates — it is the file users copy from.

use fed::Parser;
use fed::config::variants::{VariantSelection, resolve_variants};

#[path = "support/mod.rs"]
mod support;

fn load() -> fed::Config {
    Parser::new()
        .load_config("examples/variants-example.yaml")
        .expect("variants example must parse")
}

fn resolved(cli: &[&str]) -> fed::Config {
    let mut config = load();
    config.validate().expect("variants example must validate");
    let entries: Vec<String> = cli.iter().map(|s| s.to_string()).collect();
    let tmp = tempfile::tempdir().unwrap();
    let selection = VariantSelection::load(&entries, tmp.path()).unwrap();
    resolve_variants(&mut config, &selection, &[]).unwrap();
    config
}

#[test]
fn example_validates_and_has_no_unknown_keys() {
    let config = load();
    let warnings = config.unknown_key_warnings();
    assert!(
        warnings.is_empty(),
        "variants example has unknown keys: {:?}",
        warnings.iter().map(|w| &w.key).collect::<Vec<_>>()
    );
    config.validate().expect("variants example must validate");
}

#[test]
fn example_defaults_to_java_and_switches_to_go_on_request() {
    assert_eq!(
        resolved(&[]).services["catalog"].variant.as_deref(),
        Some("java")
    );
    assert_eq!(
        resolved(&["go"]).services["catalog"].variant.as_deref(),
        Some("go")
    );
}

/// The point the example exists to make: the contract is the same either way,
/// and only the implementation moves.
#[test]
fn the_contract_is_identical_across_variants() {
    for cli in [vec![], vec!["go"]] {
        let config = resolved(&cli);
        let catalog = &config.services["catalog"];
        assert!(catalog.healthcheck.is_some());
        assert_eq!(catalog.tags, vec!["backend".to_string()]);
        assert!(
            catalog.environment.contains_key("PORT"),
            "the port belongs to the service, not to a variant"
        );
        assert_eq!(config.services["frontend"].depends_on.len(), 1);
    }
}

/// Every variant is exercised, so a broken one can't hide behind the default.
#[test]
fn every_variant_resolves_to_a_startable_service() {
    let names: Vec<String> = load().services["catalog"]
        .variants
        .keys()
        .cloned()
        .collect();
    assert!(names.len() >= 2, "the example should show more than one");
    for name in names {
        let config = resolved(&[&name]);
        assert_eq!(
            config.services["catalog"].service_type(),
            fed::config::ServiceType::Process,
            "variant '{name}' must produce a startable service"
        );
    }
}
