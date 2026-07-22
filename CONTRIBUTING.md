# Contributing to Service Federation

Thank you for your interest in contributing to Service Federation! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Rust 1.89 or later
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

## How to Contribute

### Reporting Bugs

Before creating a bug report:
1. Check the [existing issues](https://github.com/service-federation/fed/issues) to avoid duplicates
2. Include steps to reproduce the issue
3. Include your environment details (OS, Rust version, etc.)

### Suggesting Features

Feature requests are welcome! Please:
1. Check existing issues to see if it's already been suggested
2. Describe the use case and why existing features don't meet your needs
3. Be open to discussion about alternative approaches

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Ensure tests pass (`cargo test`)
5. Ensure code is formatted (`cargo fmt`)
6. Ensure clippy is happy (`cargo clippy`)
7. Commit your changes with a descriptive message
8. Push to your fork
9. Open a Pull Request

## Code Style

- Follow standard Rust conventions
- Run `cargo fmt` before committing
- Run `cargo clippy` and address any warnings
- Write tests for new functionality
- Add doc comments for complex logic

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

Write clear, concise commit messages that describe what the change does:

```
Add health check retry with exponential backoff

- Implement retry logic for failed health checks
- Add configurable backoff parameters
- Update documentation
```

## Testing Guidelines

- Write unit tests for new functions
- Write integration tests for new features
- Ensure existing tests continue to pass
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

## Questions?

Feel free to open an issue for any questions about contributing.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
