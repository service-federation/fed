//! DAG-based parameter generation.
//!
//! Resolves `generate` commands in dependency order. Parameters that
//! reference other parameters via `{{PARAM}}` form a DAG. The DAG is
//! topologically sorted and resolved from roots to leaves.
//!
//! For `type: secret` parameters: generated values are persisted.
//! If a root secret is regenerated (was missing), its dependents are
//! invalidated and regenerated regardless of whether they have existing
//! values.

use crate::config::Parameter;
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet, VecDeque};

/// Result of resolving a single parameter's generate command.
#[derive(Debug)]
pub struct GenerateResult {
    pub name: String,
    pub value: String,
    pub was_generated: bool, // true if command was run (vs existing value kept)
    /// Fingerprint of the inputs this generator interpolated, when it is a
    /// secret whose template references other parameters. `None` for
    /// non-secrets, reference-less generators, and random secrets — those have
    /// no rotatable inputs to track. Persisted alongside the derived value so a
    /// later run can detect a rotated input and regenerate.
    pub fingerprint: Option<String>,
}

/// Build a topological ordering of parameters with `generate` commands.
///
/// Returns parameter names in dependency order (roots first).
/// Errors on cycles.
pub fn topological_sort(params: &HashMap<String, Parameter>) -> Result<Vec<String>> {
    // Only consider params with generate commands.
    let generate_params: HashMap<&str, Vec<String>> = params
        .iter()
        .filter(|(_, p)| p.has_generate())
        .map(|(name, p)| (name.as_str(), p.generate_dependencies()))
        .collect();

    if generate_params.is_empty() {
        return Ok(Vec::new());
    }

    // Build in-degree map.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for (name, deps) in &generate_params {
        in_degree.entry(name).or_insert(0);
        for dep in deps {
            // Only count dependencies that are themselves generate params.
            // Dependencies on non-generate params (ports, defaults) are resolved first.
            if generate_params.contains_key(dep.as_str()) {
                *in_degree.entry(name).or_insert(0) += 1;
                dependents.entry(dep.as_str()).or_default().push(name);
            }
        }
    }

    // Kahn's algorithm.
    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&name, _)| name)
        .collect();

    let mut sorted = Vec::new();

    while let Some(name) = queue.pop_front() {
        sorted.push(name.to_string());
        if let Some(deps) = dependents.get(name) {
            for dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep);
                    }
                }
            }
        }
    }

    if sorted.len() != generate_params.len() {
        let unresolved: Vec<&str> = generate_params
            .keys()
            .filter(|name| !sorted.iter().any(|s| s == **name))
            .copied()
            .collect();
        return Err(Error::TemplateResolution(format!(
            "Circular dependency in generate commands: {:?}",
            unresolved
        )));
    }

    Ok(sorted)
}

/// Execute a generate command, interpolating `{{PARAM}}` references
/// from already-resolved values.
pub fn run_generate_command(command: &str, resolved: &HashMap<String, String>) -> Result<String> {
    // Interpolate {{PARAM}} references.
    let interpolated = crate::parameter::Resolver::resolve_template_static(command, resolved)?;

    // Run via sh -c, capture stdout.
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(&interpolated)
        .output()
        .map_err(|e| Error::TemplateResolution(format!("Failed to run generate command: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Report the ORIGINAL template, never `interpolated`: the interpolated
        // command embeds resolved input values (e.g. a manual/vault seed) that
        // must not reach stderr or CI logs. `command` is the pre-interpolation
        // `{{...}}` form, so no resolved secret can leak through this message.
        return Err(Error::TemplateResolution(format!(
            "Generate command failed (exit {}): {}\nCommand: {}",
            output.status.code().unwrap_or(-1),
            stderr.trim(),
            command,
        )));
    }

    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(value)
}

/// Fingerprint a generator's resolved inputs.
///
/// A SHA-256 over the sorted, length-framed `(name, value)` pairs of the
/// parameters the generator's template references (`deps`), read from
/// `resolved`. Sorting makes the digest order-independent; length framing makes
/// it unambiguous — no name or value can forge a boundary. References absent
/// from `resolved` are skipped (deferred/undeclared names are filtered before
/// this point, so a present generator's references are all resolvable).
///
/// Only the hash is ever persisted, never the raw inputs: the derived secret it
/// guards lives in the same 0600 file, so a digest of the inputs adds no
/// marginal exposure while still detecting a rotated input.
pub fn input_fingerprint(deps: &[String], resolved: &HashMap<String, String>) -> String {
    let mut names: Vec<&String> = deps.iter().collect();
    names.sort();
    names.dedup();

    let mut buf: Vec<u8> = Vec::new();
    for name in names {
        if let Some(value) = resolved.get(name) {
            buf.extend_from_slice(&(name.len() as u64).to_be_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
            buf.extend_from_slice(value.as_bytes());
        }
    }
    sha256_hex(&buf)
}

/// Resolve all `generate` parameters in DAG order with invalidation.
///
/// `existing_values`: values already persisted (from .env.secrets).
/// `stored_fingerprints`: the per-name input fingerprints persisted next to
/// those values on the last run (empty for non-persisting callers). A stored
/// fingerprint that no longer matches the generator's current inputs — including
/// a *missing* fingerprint for a generator that DOES reference parameters (a
/// pre-upgrade entry) — invalidates the persisted value and reruns the
/// generator. Reference-less generators and random secrets carry no fingerprint
/// and keep the existing preserve-on-rerun behavior.
///
/// Returns: list of (name, value, was_generated, fingerprint) for all generate
/// params.
pub fn resolve_generate_params(
    params: &HashMap<String, Parameter>,
    existing_values: &HashMap<String, String>,
    resolved_so_far: &HashMap<String, String>,
    stored_fingerprints: &HashMap<String, String>,
) -> Result<Vec<GenerateResult>> {
    let order = topological_sort(params)?;

    let mut resolved = resolved_so_far.clone();
    let mut invalidated: HashSet<String> = HashSet::new();
    let mut results = Vec::new();

    for name in &order {
        let param = match params.get(name) {
            Some(p) => p,
            None => continue,
        };

        let cmd = match &param.generate {
            Some(c) => c,
            None => continue,
        };

        let deps = param.generate_dependencies();
        let is_secret = param.is_secret_type();
        let has_refs = !deps.is_empty();

        // Check if any dependency was just generated (invalidated).
        let any_dep_invalidated = deps.iter().any(|d| invalidated.contains(d));

        // Fingerprint the inputs this generator interpolates. Only meaningful
        // for a secret that references other parameters — non-secrets always
        // recompute, and a reference-less generator has no rotatable input.
        let current_fp = if is_secret && has_refs {
            Some(input_fingerprint(&deps, &resolved))
        } else {
            None
        };

        // Decide whether to generate or keep existing.
        let should_generate = if !is_secret {
            // Non-secret: always recompute.
            true
        } else if any_dep_invalidated {
            // Secret with invalidated dependency: regenerate.
            true
        } else if existing_values.contains_key(name) {
            // Existing secret. Keep it unless one of its template references has
            // rotated since it was persisted. `stored_fingerprints.get(name)`
            // being `None` while `current_fp` is `Some` (a pre-upgrade entry for
            // a generator WITH references) is itself a mismatch → regenerate once
            // and stamp. Reference-less generators (`current_fp == None`) have no
            // inputs to compare and are preserved.
            if has_refs {
                stored_fingerprints.get(name) != current_fp.as_ref()
            } else {
                false
            }
        } else {
            // Secret with no existing value: generate.
            true
        };

        if should_generate {
            let value = run_generate_command(cmd, &resolved)?;
            resolved.insert(name.clone(), value.clone());
            invalidated.insert(name.clone());
            results.push(GenerateResult {
                name: name.clone(),
                value,
                was_generated: true,
                fingerprint: current_fp,
            });
        } else {
            let value = existing_values[name].clone();
            resolved.insert(name.clone(), value.clone());
            results.push(GenerateResult {
                name: name.clone(),
                value,
                was_generated: false,
                fingerprint: current_fp,
            });
        }
    }

    Ok(results)
}

/// Minimal SHA-256 (FIPS 180-4), dependency-free.
///
/// Used only to fingerprint a generator's resolved inputs (see
/// `input_fingerprint`). Pulling in a crypto crate would add an entry to
/// `Cargo.lock`, which the build gate rejects (`--locked`), so the well-known
/// algorithm is inlined here. Not intended as a general-purpose hashing API.
fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;

    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // Pad: append 0x80, then zeros to 56 mod 64, then the 64-bit big-endian bit length.
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            word.clone_from(&u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]));
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = String::with_capacity(64);
    for word in h {
        let _ = write!(out, "{word:08x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param_with_generate(cmd: &str) -> Parameter {
        Parameter {
            generate: Some(cmd.to_string()),
            ..Default::default()
        }
    }

    fn secret_with_generate(cmd: &str) -> Parameter {
        Parameter {
            param_type: Some("secret".to_string()),
            generate: Some(cmd.to_string()),
            ..Default::default()
        }
    }

    // ── Topological Sort ────────────────────────────────────

    #[test]
    fn topo_sort_no_deps() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), param_with_generate("echo a"));
        params.insert("B".to_string(), param_with_generate("echo b"));

        let order = topological_sort(&params).unwrap();
        assert_eq!(order.len(), 2);
        // Both are roots — order doesn't matter, but both must appear.
        assert!(order.contains(&"A".to_string()));
        assert!(order.contains(&"B".to_string()));
    }

    #[test]
    fn topo_sort_linear_chain() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), param_with_generate("echo a"));
        params.insert("B".to_string(), param_with_generate("echo {{A}} | tr a b"));
        params.insert("C".to_string(), param_with_generate("echo {{B}} | tr b c"));

        let order = topological_sort(&params).unwrap();
        assert_eq!(order, vec!["A", "B", "C"]);
    }

    #[test]
    fn topo_sort_detects_cycle() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), param_with_generate("echo {{B}}"));
        params.insert("B".to_string(), param_with_generate("echo {{A}}"));

        let result = topological_sort(&params);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Circular dependency"), "got: {err}");
    }

    #[test]
    fn topo_sort_ignores_non_generate_deps() {
        let mut params = HashMap::new();
        // DB_PORT has no generate command — it's a port param.
        params.insert(
            "DB_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                ..Default::default()
            },
        );
        // DB_URL depends on DB_PORT but DB_PORT isn't a generate param.
        params.insert(
            "DB_URL".to_string(),
            param_with_generate("echo postgres://localhost:{{DB_PORT}}/db"),
        );

        let order = topological_sort(&params).unwrap();
        // Only DB_URL should appear (it's the only generate param).
        assert_eq!(order, vec!["DB_URL"]);
    }

    // ── Generate Command Execution ──────────────────────────

    #[test]
    fn run_simple_command() {
        let resolved = HashMap::new();
        let value = run_generate_command("echo hello", &resolved).unwrap();
        assert_eq!(value, "hello");
    }

    #[test]
    fn run_command_with_interpolation() {
        let mut resolved = HashMap::new();
        resolved.insert("NAME".to_string(), "world".to_string());
        let value = run_generate_command("echo hello-{{NAME}}", &resolved).unwrap();
        assert_eq!(value, "hello-world");
    }

    #[test]
    fn run_failing_command_returns_error() {
        let resolved = HashMap::new();
        let result = run_generate_command("false", &resolved);
        assert!(result.is_err());
    }

    #[test]
    fn run_command_captures_only_stdout() {
        let resolved = HashMap::new();
        let value = run_generate_command("echo stdout; echo stderr >&2", &resolved).unwrap();
        assert_eq!(value, "stdout");
    }

    // ── DAG Resolution with Invalidation ────────────────────

    #[test]
    fn resolve_fresh_install_generates_all() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), secret_with_generate("echo secret-a"));
        params.insert(
            "B".to_string(),
            secret_with_generate("echo derived-from-{{A}}"),
        );

        let existing = HashMap::new(); // Fresh install.
        let resolved = HashMap::new();

        let results =
            resolve_generate_params(&params, &existing, &resolved, &HashMap::new()).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].was_generated);
        assert_eq!(results[0].value, "secret-a");
        assert!(results[1].was_generated);
        assert_eq!(results[1].value, "derived-from-secret-a");
    }

    #[test]
    fn resolve_existing_secrets_kept() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), secret_with_generate("echo new-a"));
        params.insert(
            "B".to_string(),
            secret_with_generate("echo derived-from-{{A}}"),
        );

        let mut existing = HashMap::new();
        existing.insert("A".to_string(), "old-a".to_string());
        existing.insert("B".to_string(), "old-b".to_string());
        let resolved = HashMap::new();

        // B references A; its persisted fingerprint matches A's (kept) value, so
        // an unchanged input preserves B rather than regenerating it. A has no
        // references and is preserved without a fingerprint.
        let a_kept: HashMap<String, String> = [("A".to_string(), "old-a".to_string())]
            .into_iter()
            .collect();
        let stored_fps: HashMap<String, String> = [(
            "B".to_string(),
            input_fingerprint(&["A".to_string()], &a_kept),
        )]
        .into_iter()
        .collect();

        let results = resolve_generate_params(&params, &existing, &resolved, &stored_fps).unwrap();

        assert!(!results[0].was_generated); // A kept.
        assert_eq!(results[0].value, "old-a");
        assert!(!results[1].was_generated); // B kept (fingerprint matched).
        assert_eq!(results[1].value, "old-b");
    }

    #[test]
    fn resolve_missing_root_invalidates_dependents() {
        let mut params = HashMap::new();
        params.insert("A".to_string(), secret_with_generate("echo new-a"));
        params.insert(
            "B".to_string(),
            secret_with_generate("echo derived-from-{{A}}"),
        );

        let mut existing = HashMap::new();
        // A is missing, B has a stale value.
        existing.insert("B".to_string(), "stale-b".to_string());
        let resolved = HashMap::new();

        let results =
            resolve_generate_params(&params, &existing, &resolved, &HashMap::new()).unwrap();

        assert!(results[0].was_generated); // A generated.
        assert_eq!(results[0].value, "new-a");
        assert!(results[1].was_generated); // B invalidated and regenerated.
        assert_eq!(results[1].value, "derived-from-new-a");
    }

    #[test]
    fn resolve_non_secret_always_recomputes() {
        let mut params = HashMap::new();
        params.insert("HASH".to_string(), param_with_generate("echo abc123"));

        let mut existing = HashMap::new();
        existing.insert("HASH".to_string(), "old-hash".to_string());
        let resolved = HashMap::new();

        let results =
            resolve_generate_params(&params, &existing, &resolved, &HashMap::new()).unwrap();

        assert!(results[0].was_generated); // Non-secret always recomputes.
        assert_eq!(results[0].value, "abc123");
    }

    #[test]
    fn resolve_uses_already_resolved_params() {
        // DB_PORT is resolved by the port allocator (not a generate param).
        // DB_URL uses {{DB_PORT}} in its generate command.
        let mut params = HashMap::new();
        params.insert(
            "DB_URL".to_string(),
            param_with_generate("echo postgres://localhost:{{DB_PORT}}/db"),
        );

        let existing = HashMap::new();
        let mut resolved = HashMap::new();
        resolved.insert("DB_PORT".to_string(), "5432".to_string());

        let results =
            resolve_generate_params(&params, &existing, &resolved, &HashMap::new()).unwrap();

        assert_eq!(results[0].value, "postgres://localhost:5432/db");
    }

    // ── SHA-256 ─────────────────────────────────────────────

    #[test]
    fn sha256_known_vectors() {
        // FIPS 180-4 / NIST test vectors.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    // ── Input Fingerprinting ────────────────────────────────

    #[test]
    fn fingerprint_is_order_independent_and_value_sensitive() {
        let a = input_fingerprint(
            &["X".to_string(), "Y".to_string()],
            &[
                ("X".to_string(), "1".to_string()),
                ("Y".to_string(), "2".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        // Same pairs, reversed dep order → identical fingerprint.
        let b = input_fingerprint(
            &["Y".to_string(), "X".to_string()],
            &[
                ("Y".to_string(), "2".to_string()),
                ("X".to_string(), "1".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(a, b, "fingerprint must be order-independent");

        // A changed value → different fingerprint.
        let c = input_fingerprint(
            &["X".to_string(), "Y".to_string()],
            &[
                ("X".to_string(), "1".to_string()),
                ("Y".to_string(), "3".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        assert_ne!(a, c, "a rotated input value must change the fingerprint");
    }

    #[test]
    fn fingerprint_framing_is_unambiguous() {
        // Length framing must stop a `name=value` boundary from being forged by
        // moving characters between the name and the value.
        let ab_c = input_fingerprint(
            &["AB".to_string()],
            &[("AB".to_string(), "C".to_string())].into_iter().collect(),
        );
        let a_bc = input_fingerprint(
            &["A".to_string()],
            &[("A".to_string(), "BC".to_string())].into_iter().collect(),
        );
        assert_ne!(ab_c, a_bc);
    }

    #[test]
    fn resolve_regenerates_on_input_fingerprint_mismatch() {
        // A secret generator over a manual seed: an existing value whose stored
        // fingerprint no longer matches the current seed is regenerated.
        let mut params = HashMap::new();
        params.insert(
            "DERIVED".to_string(),
            secret_with_generate("printf %s {{SEED}}"),
        );

        let existing: HashMap<String, String> = [("DERIVED".to_string(), "old".to_string())]
            .into_iter()
            .collect();
        // Current run resolves SEED=rotated.
        let resolved: HashMap<String, String> = [("SEED".to_string(), "rotated".to_string())]
            .into_iter()
            .collect();
        // Stored fingerprint was computed over the OLD seed value.
        let stored: HashMap<String, String> = [(
            "DERIVED".to_string(),
            input_fingerprint(
                &["SEED".to_string()],
                &[("SEED".to_string(), "first".to_string())]
                    .into_iter()
                    .collect(),
            ),
        )]
        .into_iter()
        .collect();

        let results = resolve_generate_params(&params, &existing, &resolved, &stored).unwrap();
        assert!(results[0].was_generated, "rotated seed must regenerate");
        assert_eq!(results[0].value, "rotated");
        assert_eq!(
            results[0].fingerprint.as_ref(),
            Some(&input_fingerprint(&["SEED".to_string()], &resolved)),
            "the new fingerprint must reflect the current inputs"
        );
    }

    #[test]
    fn resolve_missing_fingerprint_with_refs_regenerates_once() {
        // A pre-upgrade entry: an existing derived secret with NO stored
        // fingerprint, whose generator references a parameter, must regenerate
        // once (and then carries a fingerprint for next time).
        let mut params = HashMap::new();
        params.insert(
            "DERIVED".to_string(),
            secret_with_generate("printf %s {{SEED}}"),
        );

        let existing: HashMap<String, String> = [("DERIVED".to_string(), "stale".to_string())]
            .into_iter()
            .collect();
        let resolved: HashMap<String, String> = [("SEED".to_string(), "seed".to_string())]
            .into_iter()
            .collect();

        let results =
            resolve_generate_params(&params, &existing, &resolved, &HashMap::new()).unwrap();
        assert!(results[0].was_generated, "missing fingerprint regenerates");
        assert_eq!(results[0].value, "seed");
        assert!(results[0].fingerprint.is_some(), "and then stamps one");
    }

    #[test]
    fn resolve_referenceless_secret_without_fingerprint_is_preserved() {
        // A generator that references nothing has no rotatable input; an existing
        // value is preserved even with no stored fingerprint (today's behavior).
        let mut params = HashMap::new();
        params.insert(
            "RAND".to_string(),
            secret_with_generate("echo would-be-new"),
        );

        let existing: HashMap<String, String> = [("RAND".to_string(), "kept".to_string())]
            .into_iter()
            .collect();
        let resolved = HashMap::new();

        let results =
            resolve_generate_params(&params, &existing, &resolved, &HashMap::new()).unwrap();
        assert!(!results[0].was_generated);
        assert_eq!(results[0].value, "kept");
        assert!(results[0].fingerprint.is_none());
    }

    // ── RB-1: failure messages must not leak resolved secrets ───

    #[test]
    fn failing_generate_error_shows_template_not_interpolated_secret() {
        let resolved: HashMap<String, String> =
            [("SEED".to_string(), "round3-supersecret".to_string())]
                .into_iter()
                .collect();
        let err = run_generate_command("false {{SEED}}", &resolved)
            .expect_err("a failing generator must error");
        let msg = err.to_string();
        assert!(
            msg.contains("false {{SEED}}"),
            "error must show the original template, got: {msg}"
        );
        assert!(
            !msg.contains("round3-supersecret"),
            "error must NOT contain the resolved secret value, got: {msg}"
        );
    }
}
