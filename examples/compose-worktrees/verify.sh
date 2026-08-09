#!/usr/bin/env bash
#
# Checks the claims in README.md against a real Docker daemon.
#
#   ./verify.sh              # uses ../../target/release/fed, falling back to $PATH
#   FED=/path/to/fed ./verify.sh
#
# Everything runs in a throwaway git repo under $TMPDIR and is torn down on exit,
# including on failure. Nothing in your own checkouts is touched.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FED="${FED:-$HERE/../../target/release/fed}"
[ -x "$FED" ] || FED="$(command -v fed)" || { echo "no fed binary; build with 'cargo build --release'"; exit 1; }

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  \033[32mok\033[0m   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; }
is()   { [ "$2" = "$3" ] && ok "$1" || { bad "$1"; printf '         expected %s\n         got      %s\n' "$3" "$2"; }; }
isnt() { [ "$2" != "$3" ] && ok "$1" || { bad "$1"; printf '         both were %s\n' "$2"; }; }

# `pwd -P` matters: on macOS $TMPDIR resolves through /var -> /private/var, and
# compose records the physical path in its working_dir label. Comparing against
# the unresolved path silently matches nothing, and the volumes survive.
ROOT="$(cd "$(mktemp -d "${TMPDIR:-/tmp}/fed-compose-verify-XXXXXX")" && pwd -P)"

# Cleanup works by diffing against a baseline taken before anything starts, and
# removing only Docker resources that (a) did not exist then and (b) belong to a
# compose project named fed-*.
#
# Discovering the project from a container's working_dir label is not enough: a
# compose service that fails to bind leaves a volume behind with no container to
# read the project off. `docker compose down` is no use either — it needs the
# compose file, which is about to be deleted, and silently no-ops without it.
#
# Caveat: if you start an unrelated fed compose project while this runs, it looks
# new and gets torn down with the rest. Don't run this alongside real work.
BASE_C="$(docker ps -aq 2>/dev/null | sort)"
BASE_V="$(docker volume ls -q 2>/dev/null | sort)"
BASE_N="$(docker network ls -q 2>/dev/null | sort)"
is_fed_project() { # $1 = docker object kind, $2 = id
  case "$(docker "$1" inspect "$2" --format \
    '{{index .Labels "com.docker.compose.project"}}' 2>/dev/null)" in fed-*) return 0;; esac
  return 1
}

cleanup() {
  for d in "$ROOT"/hardcoded-wt "$ROOT"/hardcoded "$ROOT"/wt "$ROOT"/repo; do
    [ -d "$d" ] && ( cd "$d" && "$FED" stop >/dev/null 2>&1 )
  done

  for c in $(comm -13 <(echo "$BASE_C") <(docker ps -aq 2>/dev/null | sort)); do
    case "$(docker inspect "$c" --format \
      '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null)" in
      fed-*) docker rm -f "$c" >/dev/null 2>&1 ;;
    esac
  done
  for v in $(comm -13 <(echo "$BASE_V") <(docker volume ls -q 2>/dev/null | sort)); do
    is_fed_project volume "$v" && docker volume rm -f "$v" >/dev/null 2>&1
  done
  for n in $(comm -13 <(echo "$BASE_N") <(docker network ls -q 2>/dev/null | sort)); do
    is_fed_project network "$n" && docker network rm "$n" >/dev/null 2>&1
  done

  rm -rf "$ROOT"
}
trap cleanup EXIT

seed() { # $1 = target dir, $2 = ports line for compose.yaml
  mkdir -p "$1" && cd "$1"
  git init -q . && git config user.email verify@example.com && git config user.name verify
  cp "$HERE/fed.yaml" .
  sed "s|- \"\${CACHE_PORT:-6399}:6379\"|- \"$2\"|" "$HERE/compose.yaml" > compose.yaml
  git add -A && git commit -qm seed
}
port_of()      { ( cd "$1" && "$FED" ports list 2>/dev/null | grep -i cache_port | awk '{print $1}' ); }
containers()   { docker ps --format '{{.Names}}' | grep -- "-cache-1$" | sort | tr '\n' ' '; }
project_of()   { docker inspect "$1" --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null; }

command -v docker >/dev/null || { echo "docker not found"; exit 1; }
docker info >/dev/null 2>&1  || { echo "docker daemon not running"; exit 1; }

echo "fed: $("$FED" --version)"
echo

# ---------------------------------------------------------------------------
echo "1. two worktrees, isolation on — the case the docs promise"
seed "$ROOT/repo" '${CACHE_PORT:-6399}:6379'
"$FED" start >/dev/null 2>&1
MAIN_PORT="$(port_of "$ROOT/repo")"
MAIN_CT="$(containers)"
MAIN_PROJ="$(project_of "$MAIN_CT")"

git worktree add -q -b agent-1 "$ROOT/wt"
cd "$ROOT/wt"
"$FED" isolate enable >/dev/null 2>&1
"$FED" start >/dev/null 2>&1
WT_PORT="$(port_of "$ROOT/wt")"
ALL_CT="$(containers)"
WT_CT="$(echo "$ALL_CT" | tr ' ' '\n' | grep -v "^$MAIN_CT$" | head -1)"
WT_PROJ="$(project_of "$WT_CT")"

isnt "each checkout gets its own host port"      "$MAIN_PORT" "$WT_PORT"
isnt "each checkout gets its own compose project" "$MAIN_PROJ" "$WT_PROJ"
is   "both containers run at the same time"       "$(echo "$ALL_CT" | wc -w | tr -d ' ')" "2"
is   "main checkout kept its default port"        "$MAIN_PORT" "6399"

MAIN_VOL="$(docker volume ls --format '{{.Name}}' | grep "^${MAIN_PROJ}_cache_data$" | head -1)"
WT_VOL="$(docker volume ls  --format '{{.Name}}' | grep "^${WT_PROJ}_cache_data$"  | head -1)"
isnt "named volumes are separate too"             "$MAIN_VOL" "$WT_VOL"
[ -n "$WT_VOL" ] && ok "worktree volume exists ($WT_VOL)" || bad "worktree volume missing"

( cd "$ROOT/wt"      && "$FED" stop >/dev/null 2>&1 )
( cd "$ROOT/repo"    && "$FED" stop >/dev/null 2>&1 )

# ---------------------------------------------------------------------------
echo
echo "2. isolate enable in the SAME directory — does not re-scope compose"
cd "$ROOT/repo"
"$FED" start >/dev/null 2>&1
BEFORE_CT="$(containers)"
"$FED" isolate enable >/dev/null 2>&1
"$FED" start >/dev/null 2>&1
AFTER_CT="$(containers)"
is "the same compose container is reused" "$AFTER_CT" "$BEFORE_CT"
echo "       (the project name is a hash of the compose file path, and the path"
echo "        did not change — use a worktree, not in-place isolation)"
"$FED" isolate disable >/dev/null 2>&1
"$FED" stop >/dev/null 2>&1

# ---------------------------------------------------------------------------
echo
echo "3. a hardcoded host port in compose.yaml — the one real trap"
seed "$ROOT/hardcoded" '6399:6379'
"$FED" start >/dev/null 2>&1
git worktree add -q -b agent-1 "$ROOT/hardcoded-wt"
cd "$ROOT/hardcoded-wt"
"$FED" isolate enable >/dev/null 2>&1
HC_PORT="$(port_of "$ROOT/hardcoded-wt")"
OUT="$("$FED" start 2>&1)"
isnt "fed still allocates a fresh port" "$HC_PORT" "6399"
if echo "$OUT" | grep -qi "already allocated"; then
  ok "second checkout fails loudly, it does not silently share"
  echo "       $(echo "$OUT" | grep -io 'Bind for [^ ]* failed: port is already allocated' | head -1)"
else
  bad "expected a bind failure; got:"; echo "$OUT" | tail -3 | sed 's/^/         /'
fi

echo
echo "-------------------------------------------"
printf '%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
