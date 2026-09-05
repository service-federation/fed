# Cloud-client dependency review — 2026-09-05

The review of Service Federation Cloud included this client's locked dependency
tree. RustSec reported five vulnerability advisories: `RUSTSEC-2026-0258` in
`h2` and `RUSTSEC-2026-0104`, `RUSTSEC-2026-0098`, `RUSTSEC-2026-0099`, and
`RUSTSEC-2026-0049` in `rustls-webpki`. Dependency inclusion does not imply every
affected code path is exercised by fed (in particular CRL parsing).

This update selects patched `h2` and `rustls-webpki` versions and also addresses
unsoundness advisories in `anyhow`, both `rand` versions, and `git2`. The git2
upgrade changes `Reference::shorthand()` from Option to Result; branch detection
continues to fall back when no valid shorthand is available.
HTTPS and SSH transports, which became opt-in in git2 0.21, remain explicitly
enabled and are covered by a regression test alongside named/detached HEADs.

After updating, `cargo audit` reports zero vulnerability advisories. A weekly
and PR dependency audit now prevents known vulnerabilities from silently
remaining in the lockfile.

Three informational warnings remain through ratatui 0.28.1:

- `RUSTSEC-2026-0002`: lru mutable-iterator soundness. Ratatui's only LruCache
  is the private layout cache and does not use its mutable iterator.
- `RUSTSEC-2026-0253`: lru pop panic safety. The layout cache uses `get_or_insert`
  with `(Rect, Layout)` keys, does not call `pop`, and has no custom key Drop
  implementation that can panic. The documented trigger is not present in
  the reviewed use.
- `RUSTSEC-2024-0436`: paste is unmaintained. This is a build-time procedural
  macro dependency, not a reported runtime vulnerability.

These warnings are left visible, without audit ignore rules. They should be
removed when upgrading the ratatui/ansi-to-tui dependency family; a major TUI
migration is separate from these cloud-client fixes. The reachability assessment
must be revisited if the layout-cache implementation or its dependencies change.
