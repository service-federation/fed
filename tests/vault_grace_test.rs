//! End-to-end check of the cold-vault grace window (02-cold-vault.md).
//!
//! This file runs as its own test binary with a single test, so setting
//! process-global env (`FED_TOKEN`, `FED_CLOUD_URL`, the vault knobs) is safe —
//! nothing else races it.
//!
//! Scenario: a "cold" vault that accepts the TCP connection but never answers,
//! plus a fresh local cache. With a short grace the resolver must fall back to
//! the cache within the grace window rather than hang on the dead backend —
//! proving fire-early + short-grace + fresh-cache short-circuit works against a
//! real socket (not just the test seam).

use std::collections::HashMap;
use std::io::Read;
use std::net::TcpListener;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fed::config::{Config, Parameter};
use fed::parameter::Resolver;
use tempfile::TempDir;

#[test]
fn cold_vault_with_fresh_cache_proceeds_within_grace() {
    // A server that accepts connections and then never responds — the socket is
    // alive (so this is a timeout, not a connect error) but the reply never
    // comes, exactly like a scale-to-zero backend that would take ~19s.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            match stream {
                Ok(mut s) => {
                    // Drain the request but never write a response.
                    let mut buf = [0u8; 512];
                    let _ = s.read(&mut buf);
                    held.push(s); // keep the connection open
                }
                Err(_) => break,
            }
        }
    });

    let temp = TempDir::new().unwrap();
    fed::fed_dir::ensure_fed_dir(temp.path()).unwrap();

    // Link this checkout to a project so the vault path is taken.
    std::fs::write(
        temp.path().join(".fed/cloud.yaml"),
        "org: acme\nproject: web\n",
    )
    .unwrap();

    // Seed a FRESH cache entry (stamped now) so the grace fallback can use it.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    std::fs::write(
        temp.path().join(".fed/secrets.cache.env"),
        format!("# fetched-at API_KEY {now}\nAPI_KEY=cached_secret\n"),
    )
    .unwrap();

    // Credentials via env (the CI path), pointed at our dead server, with a
    // tiny grace and a large freshness bound.
    std::env::set_var("FED_TOKEN", "test-token");
    std::env::set_var("FED_CLOUD_URL", format!("http://127.0.0.1:{port}"));
    std::env::set_var("FED_VAULT_GRACE", "1s");
    std::env::set_var("FED_VAULT_MAX_AGE", "24h");
    // Keep the blocking budget short too, so a bug that blocks instead of using
    // the cache fails fast rather than hanging the whole suite.
    std::env::set_var("FED_VAULT_TIMEOUT", "8s");

    let mut config = Config::default();
    config.parameters.insert(
        "API_KEY".to_string(),
        Parameter {
            param_type: Some("secret".to_string()),
            source: Some("manual".to_string()),
            ..Default::default()
        },
    );

    let mut resolver = Resolver::new();
    resolver.set_work_dir(temp.path());

    let start = Instant::now();
    resolver
        .resolve_parameters(&mut config)
        .expect("fresh cache must satisfy the run when the vault is cold");
    let elapsed = start.elapsed();

    // Cleanup env before asserting (so a panic still leaves a clean process is
    // moot here — single test — but tidy regardless).
    for var in [
        "FED_TOKEN",
        "FED_CLOUD_URL",
        "FED_VAULT_GRACE",
        "FED_VAULT_MAX_AGE",
        "FED_VAULT_TIMEOUT",
    ] {
        std::env::remove_var(var);
    }

    let resolved: &HashMap<String, String> = resolver.get_resolved_parameters();
    assert_eq!(
        resolved.get("API_KEY").map(String::as_str),
        Some("cached_secret"),
        "must resolve from the fresh cache"
    );
    // Grace is 1s; the dead server would otherwise force the 8s budget. Give a
    // generous ceiling to stay robust under load while still catching a hang.
    assert!(
        elapsed < Duration::from_secs(6),
        "should fall back to cache within the grace window, took {elapsed:?}"
    );
}
