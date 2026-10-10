mod support;

#[test]
fn readme_getting_started_config_is_valid() {
    let readme = include_str!("../README.md");
    let getting_started = readme
        .split_once("## Add fed to a project")
        .expect("README must keep an 'Add fed to a project' section")
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
fn readme_registry_auth_config_is_valid() {
    let readme = include_str!("../README.md");
    let section = readme
        .split_once("### Private registry images")
        .expect("README must keep a 'Private registry images' section")
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
        .expect("README registry_auth YAML must pass fed validation");
    assert!(config.registry_auth.contains_key("ghcr.io"));
}

#[test]
fn readme_image_platform_config_is_valid() {
    let readme = include_str!("../README.md");
    let section = readme
        .split_once("### Images for another architecture")
        .expect("README must keep an 'Images for another architecture' section")
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
        .expect("README platform YAML must pass fed validation");
    assert_eq!(
        config.services["presentation"].platform.as_deref(),
        Some("linux/amd64")
    );
}
