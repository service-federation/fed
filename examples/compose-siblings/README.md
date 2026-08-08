# Postgres and Redis siblings in one Compose project

This is a small, usable local data stack: durable Postgres for application data
and Redis for disposable caching. Both images are pinned by version and manifest
digest, and both expose Compose healthchecks.

Fed includes the Compose file once and expands both children into its service graph.
Starting, restarting, or stopping Redis must not recreate or remove Postgres—the
exact shape used by applications that want to operate infrastructure independently.
Fed-allocated ports are passed once at import scope and consumed through normal
Compose variable interpolation.

```sh
cargo build
target/debug/fed -w examples/compose-siblings start database
target/debug/fed -w examples/compose-siblings start cache
target/debug/fed -w examples/compose-siblings stop cache
target/debug/fed -w examples/compose-siblings stop database
```

The Docker integration suite copies and starts this exact project, creates a real
Postgres table and row, exercises Redis, and proves both the Postgres container
identity and SQL data remain unchanged. It then stops Postgres and verifies final
project cleanup.
