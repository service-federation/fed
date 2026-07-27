# Contributing to Service Federation

## Getting Started

### Prerequisites

- Rust 1.95 or later
- Docker (for running Docker-related tests)
- Git

### Building from Source

```bash
git clone https://github.com/service-federation/fed.git
cd fed
cargo build
```

### Running Tests

```bash
# Run all tests (excluding Docker tests)
cargo test

# Run all tests including Docker tests (requires Docker)
cargo test -- --include-ignored

# Run a specific test
cargo test test_name
```

## Pull Requests

Run `cargo fmt`, `cargo clippy`, and `cargo test` before opening one. Write
tests for new functionality.

### Linting platform-specific code

Some code is behind `#[cfg(target_os = "linux")]` (`/proc` parsing in
`src/error.rs`, the `lsof`/`ss` handling in `src/port/conflict.rs`). On macOS
that code is compiled out, so `cargo clippy` cannot see it — a clean local run
can still fail CI, which lints on Linux.

If you touch `cfg`-gated code, lint the Linux target through Docker:

```bash
scripts/lint-linux.sh
```

CI pins clippy to a specific Rust version so that lint failures are always
caused by a change rather than by a new compiler release. When bumping it,
update both `.github/workflows/ci.yml` and `scripts/lint-linux.sh`.

## Commit Messages

Match `git log`: a plain-sentence summary line, then prose explaining the
symptom, the root cause, and why the fix works — not a bullet list of what
changed. The diff already says what changed; the message is for what the
diff can't say.

## Testing Guidelines

- Docker-related tests should be marked with `#[ignore]` and include `// Requires Docker` comment
- When a new integration test builds a fed-config YAML fixture inline, parse
  it through `support::parse_checked` (`tests/support/mod.rs`) instead of a
  bare `Parser::parse_config(..).unwrap()`. fed's config parser is
  deliberately permissive about unknown keys (a typo is a warning, not a
  parse error), so a typo'd or made-up field in a fixture silently falls
  back to "field absent" instead of failing the test that's supposed to
  exercise it. `parse_checked` turns that into an immediate, loud failure.
  `tests/config_key_audit_test.rs` is the standing gate that catches drift
  in existing fixtures across the whole `tests/` tree either way.

## Architecture Overview

The codebase is organized as follows:

- `src/config/` - Configuration parsing and validation
- `src/service/` - Service managers (Docker, Process, Gradle, etc.)
- `src/orchestrator/` - Service orchestration and lifecycle management
- `src/markers.rs` - Install/migrate lifecycle markers
- `src/state/` - State persistence (SQLite-based)
- `src/tui/` - Terminal UI implementation
- `src/commands/` - CLI command implementations
