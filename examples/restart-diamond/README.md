# Targeted restart through a diamond graph

This project makes lifecycle ordering observable. `root` increments a generation
on every start, the hook-only nodes record that generation, and every downstream
node refuses to complete or run unless its prerequisites ran in the same generation.

The graph deliberately contains shared and direct edges:

```text
root -> hook-a -> worker -> web
  |       |
  |       +----> shared -> web
  +-> hook-b ---> shared
  +---------------------> web
```

Exercise it with a locally built Fed binary:

```sh
cargo build
target/debug/fed -w examples/restart-diamond start
target/debug/fed -w examples/restart-diamond restart root
cat examples/restart-diamond/.runtime/events
target/debug/fed -w examples/restart-diamond stop
```

Each generation must contain exactly one line for every service, and every hook
must precede its consumers. The integration suite copies and drives this exact
example through separate Fed processes.
