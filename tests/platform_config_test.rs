//! `platform:`: an image service pulled and run for another architecture.
//!
//! The field is accepted on image services only, in Docker's
//! `os/arch[/variant]` form.

mod support;

use support::parse_checked;

#[test]
fn an_image_service_accepts_a_platform() {
    let config = parse_checked(
        r#"
services:
  presentation:
    image: "example/presentation:latest"
    platform: linux/amd64
  db:
    image: "postgres:16"
"#,
    );

    config
        .validate()
        .expect("an image service may set a platform");
    assert_eq!(
        config.services["presentation"].platform.as_deref(),
        Some("linux/amd64")
    );
    assert_eq!(config.services["db"].platform, None);
}

#[test]
fn a_platform_with_a_variant_part_is_accepted() {
    let config = parse_checked(
        r#"
services:
  app:
    image: "example/app:latest"
    platform: linux/arm/v7
"#,
    );

    config
        .validate()
        .expect("os/arch/variant is a valid platform");
}

#[test]
fn a_platform_on_a_process_service_is_rejected() {
    let config = parse_checked(
        r#"
services:
  api:
    process: "cargo run"
    platform: linux/amd64
"#,
    );

    let error = config
        .validate()
        .expect_err("a process service has no image to pull");
    assert_eq!(
        error.to_string(),
        "Invalid configuration: Service 'api' sets platform: linux/amd64 but has no image:. \
         Only image services are pulled and run for a platform. \
         Remove platform:, or move it next to the image:."
    );
}

#[test]
fn a_platform_without_an_architecture_is_rejected() {
    let config = parse_checked(
        r#"
services:
  app:
    image: "example/app:latest"
    platform: amd64
"#,
    );

    let error = config
        .validate()
        .expect_err("amd64 alone is not a platform");
    assert_eq!(
        error.to_string(),
        "Invalid configuration: Service 'app' has invalid platform 'amd64'. \
         Use os/architecture, for example 'linux/amd64' or 'linux/arm64'."
    );
}

/// The outer service of a variant service is shared by every variant, so a
/// `platform:` there reaches the process variant too.
#[test]
fn an_outer_platform_reaching_a_process_variant_is_rejected() {
    let config = parse_checked(
        r#"
services:
  api:
    platform: linux/amd64
    default_variant: image
    variants:
      source:
        process: cargo run
      image:
        image: example/api:latest
"#,
    );

    let error = config
        .validate()
        .expect_err("the source variant has no image");
    assert!(
        error
            .to_string()
            .contains("Service 'api:source' sets platform: linux/amd64 but has no image:"),
        "{error}"
    );
}

#[test]
fn a_platform_inside_the_image_variant_is_accepted() {
    let config = parse_checked(
        r#"
services:
  api:
    default_variant: image
    variants:
      source:
        process: cargo run
      image:
        image: example/api:latest
        platform: linux/amd64
"#,
    );

    config
        .validate()
        .expect("the platform sits next to the image it applies to");
}
