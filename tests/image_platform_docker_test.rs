//! `DockerClient` against a real daemon, for an image built only for another
//! architecture than the host's.
//!
//! On an arm64 host the test uses `amd64/busybox`, and on an amd64 host
//! `arm64v8/busybox`. Both are published for one platform only.

use fed::docker::DockerClient;
use std::process::Command;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(120);

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker runs")
}

/// The image and platform foreign to the daemon, plus the daemon's own platform.
fn foreign_image() -> (&'static str, &'static str, &'static str) {
    let out = docker(&["info", "--format", "{{.Architecture}}"]);
    let arch = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if arch == "x86_64" || arch == "amd64" {
        ("arm64v8/busybox:1.37", "linux/arm64", "linux/amd64")
    } else {
        ("amd64/busybox:1.37", "linux/amd64", "linux/arm64")
    }
}

/// Removes the test image on drop.
struct RemoveImage(&'static str);

impl Drop for RemoveImage {
    fn drop(&mut self) {
        let _ = docker(&["rmi", "-f", self.0]);
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "docker-tests"), ignore)] // Requires Docker
async fn an_image_for_another_architecture_needs_its_platform() {
    let (image, platform, host_platform) = foreign_image();
    let _cleanup = RemoveImage(image);
    let _ = docker(&["rmi", "-f", image]);
    let client = DockerClient::new();

    let err = client
        .pull(image, None, TIMEOUT)
        .await
        .expect_err("the image has no build for this machine")
        .to_string();
    assert!(
        err.contains("set `platform:` on the service"),
        "the pull error should point at platform:, got: {err}"
    );

    assert!(!client.image_exists(image, Some(platform)).await);
    client
        .pull(image, Some(platform), TIMEOUT)
        .await
        .expect("the pull for the image's own platform succeeds");

    assert!(client.image_exists(image, Some(platform)).await);
    assert!(
        !client.image_exists(image, Some(host_platform)).await,
        "a local copy for {platform} must not count as present for {host_platform}"
    );
    assert!(client.image_exists(image, None).await);
}
