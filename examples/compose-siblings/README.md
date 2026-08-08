# Sibling services in one Compose project

Both Fed services select a different child from the same Compose file. Starting,
restarting, or stopping one child must not recreate or remove the other child.

```sh
cargo build
target/debug/fed -w examples/compose-siblings start database
target/debug/fed -w examples/compose-siblings start cache
target/debug/fed -w examples/compose-siblings stop cache
target/debug/fed -w examples/compose-siblings stop database
```

The Docker integration suite copies this exact project, writes durable data to
`database`, exercises `cache`, and proves both the database container identity and
its data remain unchanged.
