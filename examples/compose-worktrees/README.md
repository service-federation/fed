# Compose services across git worktrees

Point fed at a `compose.yaml` you already have, then run the same project from two
checkouts at once. This example exists because "does Compose actually isolate?" is
easy to assert and easy to get wrong, so `verify.sh` checks it against a real
Docker daemon instead.

```sh
cargo build --release          # from the repo root
./verify.sh                    # or: FED=$(which fed) ./verify.sh
```

Everything runs in a throwaway git repo under `$TMPDIR` and is torn down on exit,
including on failure. Your own checkouts are not touched.

## What fed does with a compose service

`compose_file` + `compose_service` means fed does not own the container. It shells
out to `docker compose up <service>`, passing two things:

- `-p fed-<hash>`, where the hash is derived from **the path of the compose file**;
- the service's resolved `environment:` as the subprocess environment.

Both halves matter, and between them they explain every result below.

## What is true

**Two worktrees isolate completely.** Different checkout, different compose file
path, so a different project name — and Docker Compose namespaces containers,
networks, and named volumes by project. A worktree gets its own everything and both
checkouts run at the same time:

```
main       fed-1a2b-cache-1   0.0.0.0:6399->6379/tcp    volume fed-1a2b_cache_data
worktree   fed-3c4d-cache-1   0.0.0.0:54746->6379/tcp   volume fed-3c4d_cache_data
```

Ports isolate because `compose.yaml` reads `${CACHE_PORT:-6399}` and fed exports
`CACHE_PORT` into the subprocess. Compose's own substitution does the work; fed
never rewrites your file.

**The main checkout keeps its declared port.** Isolation is opt-in per checkout.
Enabling it in a worktree does not disturb anything already running.

## What is not true

**`fed isolate enable` in the same directory does not re-scope a compose service.**
The project name comes from the compose file path, and running `isolate enable`
where you stand does not change that path. Native and direct image-backed services
get new names; the compose container is reused as-is. Isolation for compose services
is per-directory. Use a worktree.

**fed cannot rewrite a host port hardcoded inside `compose.yaml`.** Write
`"6399:6379"` instead of `"${CACHE_PORT:-6399}:6379"` and the second checkout gets a
freshly allocated `CACHE_PORT` that nothing reads, then dies on the bind:

```
Error response from daemon: driver failed programming external connectivity on
endpoint fed-54a6-cache-1: Bind for 0.0.0.0:6399 failed: port is already allocated
```

This is the one real trap, and it fails loudly rather than silently sharing a
container. Every host port in a compose file that you want isolated has to go
through `${VAR}` substitution and be declared as a `type: port` parameter.

## Files

| File | Purpose |
|---|---|
| `compose.yaml` | The file you already had. Uses `${CACHE_PORT:-6399}` so it still works standalone. |
| `fed.yaml` | Declares `CACHE_PORT` as `type: port`, one compose service, one native process that reads the same parameter. |
| `verify.sh` | Nine assertions across the three scenarios above. Exits non-zero if any fails. |
