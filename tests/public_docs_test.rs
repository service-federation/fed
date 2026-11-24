mod support;

#[test]
fn readme_getting_started_config_is_valid() {
    let readme = include_str!("../README.md");
    let getting_started = readme
        .split_once("## Quick start")
        .expect("README must keep a 'Quick start' section")
        .1;
    let yaml = getting_started
        .split_once("```yaml")
        .expect("README getting-started section must contain a YAML example")
        .1
        .split_once("```")
        .expect("README getting-started YAML block must be closed")
        .0;

    let config = support::parse_checked(yaml);
    config
        .validate()
        .expect("README getting-started YAML must pass fed validation");

    assert_eq!(config.entrypoint.as_deref(), Some("api"));
    assert!(config.services.contains_key("database"));
    assert!(config.services.contains_key("api"));
}

#[test]
fn advanced_docs_registry_auth_config_is_valid() {
    let docs = include_str!("../docs/advanced.md");
    let section = docs
        .split_once("### Private registry images")
        .expect("docs/advanced.md must keep a 'Private registry images' section")
        .1;
    let yaml = section
        .split_once("```yaml")
        .expect("registry_auth section must contain a YAML example")
        .1
        .split_once("```")
        .expect("registry_auth YAML block must be closed")
        .0;

    let config = support::parse_checked(yaml);
    config
        .validate()
        .expect("docs/advanced.md registry_auth YAML must pass fed validation");
    assert!(config.registry_auth.contains_key("ghcr.io"));
}

#[test]
fn advanced_docs_image_platform_config_is_valid() {
    let docs = include_str!("../docs/advanced.md");
    let section = docs
        .split_once("### Images for another architecture")
        .expect("docs/advanced.md must keep an 'Images for another architecture' section")
        .1;
    let yaml = section
        .split_once("```yaml")
        .expect("platform section must contain a YAML example")
        .1
        .split_once("```")
        .expect("platform YAML block must be closed")
        .0;

    let config = support::parse_checked(yaml);
    config
        .validate()
        .expect("docs/advanced.md platform YAML must pass fed validation");
    assert_eq!(
        config.services["presentation"].platform.as_deref(),
        Some("linux/amd64")
    );
}

#[test]
fn advanced_docs_host_access_config_is_valid() {
    let docs = include_str!("../docs/advanced.md");
    let section = docs
        .split_once("### Reaching the host from a container")
        .expect("docs/advanced.md must keep a 'Reaching the host from a container' section")
        .1;
    let yaml = section
        .split_once("```yaml")
        .expect("host access section must contain a YAML example")
        .1
        .split_once("```")
        .expect("host access YAML block must be closed")
        .0;

    let config = support::parse_checked(yaml);
    config
        .validate()
        .expect("docs/advanced.md host access YAML must pass fed validation");
    assert!(config.services["worker"].environment["DATABASE_URL"].contains("host.docker.internal"));
}
