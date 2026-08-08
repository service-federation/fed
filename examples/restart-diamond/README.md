# Targeted restart through a diamond graph

This project models a common application bootstrap: a datastore starts, schema and
stream provisioning run, derived application/worker setup completes, and only then
does the web process boot. The commands use local marker files so the example stays
portable and makes lifecycle ordering directly observable.

`datastore` increments a generation on every start. Every hook-only node records
that generation and refuses to complete unless its prerequisites ran in the same
generation. It is a compact template for migrations, topic creation, tenant setup,
generated assets, and other idempotent `migrate` hooks.

The graph deliberately contains shared and direct edges:

```text
datastore -> schema-ready -> worker-ready ------> web
    |             |
    |             +----> application-ready ----> web
    +-> stream-ready ---> application-ready
    +-------------------------------------------> web
```

Exercise it with a locally built Fed binary:

```sh
cargo build
target/debug/fed -w examples/restart-diamond start
target/debug/fed -w examples/restart-diamond restart datastore
cat examples/restart-diamond/.runtime/events
target/debug/fed -w examples/restart-diamond stop
```

Each generation must contain exactly one line for every service, and every hook
must precede its consumers. The integration suite copies and drives this exact
example through separate Fed processes.
