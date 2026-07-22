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
