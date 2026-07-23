//! Standing audit: every fed-config YAML fixture under `tests/` must be free
//! of unknown top-level/service/template keys.
//!
//! fed's parser is deliberately permissive — an unrecognized key is a
//! non-fatal "did you mean?" warning (`Config::unknown_key_warnings`), never
//! a parse error, because a typo in a user's config shouldn't brick their
//! stack. That permissiveness has a cost for *our own* test fixtures: a
//! fixture with a typo'd or made-up key silently drops the field into
//! `unknown_fields` and the test ends up exercising the fallback behavior
//! (usually "field absent"), not the feature it claims to cover.
//!
//! This test is the permanent version of a throwaway audit that found three
//! such offenders (`restart_policy` in `force_quit_recovery_test.rs`,
//! top-level `install`/`clean` in `cli_subcommands_test.rs`, and
//! `external_service_parameters` in `reproduction_tests.rs`) — all fixed
//! elsewhere in this repo. This test exists so the next one doesn't need a
//! throwaway script to find it: it fails loudly, with the exact "unknown
//! field" message a real user would see, the moment a new fixture drifts
//! from the schema.
//!
//! For fixtures built inline in new tests, prefer routing them through
//! `support::parse_checked` (`tests/support/mod.rs`) instead of a bare
//! `Parser::parse_config(..).unwrap()` — same check, at the point of
//! authorship rather than at the next full-suite run.

use std::fs;
use std::path::{Path, PathBuf};

/// `file_name:line` pairs that the `services:` + no-`version:` heuristic
/// below cannot itself distinguish from real fed config — recorded here with
/// a reason instead of silently skipped, so a future extractor bug surfaces
/// as a build failure rather than a missed detection. Empty today: every
/// false positive found while building this test (docker-compose YAML
/// embedded in `docker_compose_test.rs`) is handled by the `version:`
/// heuristic itself, not by this allowlist.
const KNOWN_NON_FED_YAML: &[(&str, u32, &str)] = &[];

struct Offender {
    location: String,
    key: String,
    candidates: Vec<String>,
}

fn record_warnings(source: &str, label: &str, offenders: &mut Vec<Offender>) {
    // A parse failure here is expected and not this test's concern: it's
    // either an intentional malformed-YAML negative test, or (for the
    // `tests/*.rs` raw-string scan) a block whose shape our extractor can't
    // turn into valid YAML on its own (e.g. a `format!` placeholder like
    // `compose_file: {}`) — the real fed-config blocks in the same test files
    // parse cleanly and are what this test actually checks.
    let Ok(config) = fed::config::Parser::new().parse_config(source) else {
        return;
    };
    for w in config.unknown_key_warnings() {
        offenders.push(Offender {
            location: format!("{label} ({})", w.location),
            key: w.key,
            candidates: w.candidates.iter().map(|s| s.to_string()).collect(),
        });
    }
    // Legacy-cased spellings (httpGet, gradleTask, composeFile, composeService,
    // !onfailure) still parse via serde aliases, but our own fixtures must use
    // the canonical snake_case forms. Intentional legacy-spelling coverage
    // lives in src/config unit tests, which this audit does not scan.
    for u in &config.legacy_key_usages {
        offenders.push(Offender {
            location: format!("{label} ({})", u.location),
            key: format!("{} (legacy spelling)", u.legacy),
            candidates: vec![u.canonical.to_string()],
        });
    }
}

fn collect_yaml_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_yaml_files(&path, out);
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("yaml") | Some("yml")
        ) {
            out.push(path);
        }
    }
}

/// Extract every single-hash raw-string literal (`r#"..."#`) from `content`,
/// returning `(block, starting_line_number)`. This is the one raw-string
/// form actually used across `tests/*.rs` today (`grep -c 'r#"' tests/*.rs`
/// finds only single-hash raw strings; multi-hash `r##"` is unused) — not a
/// real Rust lexer, just enough to recover the fixtures this audit cares
/// about. `KNOWN_NON_FED_YAML` above absorbs anything it can't handle.
fn extract_raw_strings(content: &str) -> Vec<(String, u32)> {
    let mut out = Vec::new();
    let mut search_from = 0usize;
    while let Some(rel_start) = content[search_from..].find("r#\"") {
        let start = search_from + rel_start + 3; // skip past r#"
        let Some(rel_end) = content[start..].find("\"#") else {
            break;
        };
        let end = start + rel_end;
        let line_number = content[..start].matches('\n').count() as u32 + 1;
        out.push((content[start..end].to_string(), line_number));
        search_from = end + 2;
    }
    out
}

/// A block only counts as "a fed config to check" if it contains a
/// top-level `services:` key **and** doesn't carry the docker-compose
/// version-pinning giveaway (`version:` at column 0 near the top of the
/// block) — the false positive that a bare "contains `services:`" filter
/// can't avoid, found in `docker_compose_test.rs` (raw docker-compose YAML
/// written to disk for the `docker compose` CLI, sitting next to real
/// fed-config blocks in the same file).
fn is_fed_config_block(block: &str) -> bool {
    let lines: Vec<&str> = block.lines().collect();
    let has_services = lines.iter().any(|l| l.trim() == "services:");
    if !has_services {
        return false;
    }
    let has_version_giveaway = lines.iter().take(6).any(|l| l.starts_with("version:"));
    !has_version_giveaway
}

#[test]
fn config_key_audit() {
    let mut offenders = Vec::new();

    // Tier 2.1 — every *.yaml/*.yml under tests/fixtures/, except
    // tests/fixtures/compose/. That directory is, by its name and existing
    // content, raw docker-compose fixtures consumed by the `docker compose`
    // CLI directly — not by fed's Parser. The directory boundary is the
    // scoping rule, not content sniffing.
    let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let compose_dir = fixtures_dir.join("compose");
    let mut fixture_files = Vec::new();
    collect_yaml_files(&fixtures_dir, &mut fixture_files);
    for path in fixture_files {
        if path.starts_with(&compose_dir) {
            continue;
        }
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        record_warnings(&content, &path.display().to_string(), &mut offenders);
    }

    // Tier 2.2 — raw-string YAML blocks embedded directly in tests/*.rs.
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut test_files: Vec<PathBuf> = fs::read_dir(&tests_dir)
        .expect("read tests/ dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("rs"))
        .collect();
    test_files.sort();

    for path in test_files {
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        for (block, line) in extract_raw_strings(&content) {
            if !is_fed_config_block(&block) {
                continue;
            }
            if KNOWN_NON_FED_YAML
                .iter()
                .any(|(f, l, _)| *f == file_name && *l == line)
            {
                continue;
            }
            record_warnings(&block, &format!("{file_name}:{line}"), &mut offenders);
        }
    }

    assert!(
        offenders.is_empty(),
        "{} unknown config key(s) found in test fixtures under tests/ — \
         these tests are not exercising what they think they are:\n{}",
        offenders.len(),
        offenders
            .iter()
            .map(|o| format!(
                "  {}: unknown field '{}' (known: {:?})",
                o.location, o.key, o.candidates
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
