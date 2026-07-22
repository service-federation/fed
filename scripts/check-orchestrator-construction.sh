#!/usr/bin/env bash
#
# CI gate: disallow raw Orchestrator construction inside src/orchestrator/**.
#
# clippy::disallowed_methods (see clippy.toml) catches bin-crate and
# integration-test call sites, but not library-internal ones — a call from
# one src/orchestrator/*.rs file to Orchestrator::new/new_ephemeral defined
# in another file of the *same* crate doesn't cross a crate boundary, so
# clippy doesn't see it (confirmed against clippy 1.97). This script closes
# that gap with a plain text scan.
#
# Allowed exceptions:
#   - builder.rs itself (the one legitimate construction site)
#   - a call preceded (within 10 lines, same justification-comment block) by
#     an explicit `// ALLOW-RAW-ORCHESTRATOR-CONSTRUCTION:<reason>` marker
#     comment (e.g. scripts.rs's child-orchestrator construction — see
#     run_context.rs / the RunContext plan's §4a for why it can't go
#     through the builder)
#   - doc-comment usage examples (`///`/`//!` lines) — these document the
#     still-`pub` constructor for external callers, not an internal bypass
#   - each file's own #[cfg(test)] unit-test module (library-internal unit
#     tests are same-crate, lower-risk, already bounded)
#
# Usage: scripts/check-orchestrator-construction.sh

set -euo pipefail
cd "$(dirname "$0")/.."

status=0
for f in src/orchestrator/*.rs; do
  [ "$f" = "src/orchestrator/builder.rs" ] && continue

  bad=$(awk '
    /#\[cfg\(test\)\]/ { exit }
    {
      lines[NR] = $0
      trimmed = $0
      sub(/^[ \t]+/, "", trimmed)
      is_doc_comment = (trimmed ~ /^\/\/\// || trimmed ~ /^\/\/!/)
      if (!is_doc_comment && $0 ~ /Orchestrator::new(_ephemeral|_with_namespace)?\(/) {
        allowed = 0
        start = (NR - 10 > 1) ? NR - 10 : 1
        for (i = start; i <= NR; i++) {
          if (lines[i] ~ /ALLOW-RAW-ORCHESTRATOR-CONSTRUCTION/) { allowed = 1 }
        }
        if (!allowed) {
          print NR": "$0
        }
      }
    }
  ' "$f")

  if [ -n "$bad" ]; then
    echo "$bad"
    echo "::error file=$f::raw Orchestrator construction outside OrchestratorBuilder — route through the builder or apply_run_context, or mark with an ALLOW-RAW-ORCHESTRATOR-CONSTRUCTION comment and a reason."
    status=1
  fi
done

exit $status
