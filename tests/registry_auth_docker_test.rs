//! Pulls from a password-protected local registry with a `registry_auth`
//! team credential.
//!
//! The test starts `registry:2` with htpasswd auth on a random localhost port,
//! pushes a tiny image into it, removes the local copy, and then pulls it back
//! through `DockerClient::pull_with_credential`.

use fed::docker::DockerClient;
use fed::docker::registry_auth::RegistryCredential;
use std::process::Command;
use std::time::Duration;

const USER: &str = "fed-bot";
const PASSWORD: &str = "fed-test-pa55word";
const TIMEOUT: Duration = Duration::from_secs(120);

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker runs")
}

fn docker_ok(args: &[&str]) -> String {
    let out = docker(args);
    assert!(
        out.status.success(),
        "docker {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Removes the registry container and the test image tag on drop.
struct Cleanup {
    container: String,
    image: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", "-v", &self.container]);
        let _ = docker(&["rmi", "-f", &self.image]);
    }
}

fn leftover_auth_dirs() -> Vec<String> {
    let prefix = format!("fed-registry-auth-{}-", std::process::id());
    std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&prefix))
        .collect()
}

#[tokio::test]
#[ignore] // Requires Docker
async fn pulls_from_a_private_registry_with_the_team_credential() {
    let htpasswd = docker_ok(&[
        "run",
        "--rm",
        "--entrypoint",
        "htpasswd",
        "httpd:2-alpine",
        "-Bbn",
        USER,
        PASSWORD,
    ]);
    let ht_env = format!("HT={htpasswd}");
    let container = docker_ok(&[
        "run",
        "-d",
        "-p",
        "0:5000",
        "-e",
        &ht_env,
        "-e",
        "REGISTRY_AUTH=htpasswd",
        "-e",
        "REGISTRY_AUTH_HTPASSWD_REALM=fed-test",
        "-e",
        "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd",
        "--entrypoint",
        "sh",
        "registry:2",
        "-c",
        "mkdir -p /auth && printf '%s\\n' \"$HT\" > /auth/htpasswd && exec registry serve /etc/docker/registry/config.yml",
    ]);
    let port_line = docker_ok(&["port", &container, "5000/tcp"]);
    let port = port_line
        .lines()
        .next()
        .and_then(|l| l.rsplit(':').next())
        .unwrap()
        .to_string();
    let registry = format!("localhost:{port}");
    let image = format!("{registry}/fed-test/hello:1");
    let _cleanup = Cleanup {
        container: container.clone(),
        image: image.clone(),
    };

    // Seed the registry with an explicit, throwaway --config so the user's
    // own Docker config is not touched.
    // A one-file image built locally, so every layer is present to push.
    let layer = tempfile::tempdir().unwrap();
    std::fs::write(layer.path().join("hello.txt"), "hello\n").unwrap();
    let tar = layer.path().join("image.tar");
    let tar_ok = Command::new("tar")
        .arg("-cf")
        .arg(&tar)
        .arg("-C")
        .arg(layer.path())
        .arg("hello.txt")
        .status()
        .unwrap()
        .success();
    assert!(tar_ok);
    docker_ok(&["import", &tar.to_string_lossy(), &image]);
    let push_config = tempfile::tempdir().unwrap();
    let auth = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASSWORD}"))
    };
    std::fs::write(
        push_config.path().join("config.json"),
        format!(r#"{{"auths":{{"{registry}":{{"auth":"{auth}"}}}}}}"#),
    )
    .unwrap();
    let push_dir = push_config.path().to_string_lossy().into_owned();
    let mut pushed = false;
    for _ in 0..10 {
        let push = tokio::process::Command::new("docker")
            .args(["--config", &push_dir, "push", &image])
            .kill_on_drop(true)
            .output();
        if let Ok(Ok(out)) = tokio::time::timeout(Duration::from_secs(30), push).await
            && out.status.success()
        {
            pushed = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        pushed,
        "could not push the test image to the local registry"
    );
    docker_ok(&["rmi", &image]);

    let client = DockerClient::new();

    // Without the team credential the registry refuses the pull.
    let err = client
        .pull(&image, TIMEOUT)
        .await
        .expect_err("anonymous pull must be refused");
    assert!(!err.to_string().contains(PASSWORD));

    // A wrong team credential warns, falls back to the environment's own
    // credentials, and still fails. The password stays out of the error.
    let wrong = RegistryCredential::new(&registry, USER, "wrong-password", "REGISTRY_TOKEN");
    let err = client
        .pull_with_credential(&image, Some(&wrong), TIMEOUT)
        .await
        .expect_err("wrong credential and no ambient login must fail");
    assert!(!err.to_string().contains("wrong-password"));
    assert!(leftover_auth_dirs().is_empty());

    // The right team credential pulls the image.
    let right = RegistryCredential::new(&registry, USER, PASSWORD, "REGISTRY_TOKEN");
    client
        .pull_with_credential(&image, Some(&right), TIMEOUT)
        .await
        .expect("pull with the team credential");
    assert!(client.image_exists(&image).await);
    assert!(
        leftover_auth_dirs().is_empty(),
        "temp auth dir was not removed"
    );
}
