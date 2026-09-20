# A real stack with interchangeable implementations

A tiny catalog you can run with **Python 3.10+ and fed**, with no packages,
Docker, or cloud account. The frontend calls a catalog API. The `live` variant
reads inventory from a storage HTTP service; `fixture` serves a small local
sample instead. Both use the same catalog URL and response shape.

The optional terminal console needs macOS or Linux. All HTTP listeners bind to
`127.0.0.1`. The storage service holds a fixed in-memory dataset, not a database.

## Start and look at the result

From this directory, with the PR's fed binary on your `PATH`:

```sh
fed validate
fed start
fed status
fed ports list
```

Open the frontend URL printed by `fed start` (normally
<http://127.0.0.1:18743>). You should see **live**, Notebook with 12 in stock,
and Pencil with 24. The JSON link goes through the same frontend → catalog →
storage request chain. Ports are parameters, so use the printed URL or
`fed ports list` if a default port is occupied.

`entrypoint: '*'` starts the complete enabled graph, just like `fed start --all`.
It leaves the `console` profile off. The `stack` service is a dependency-only
group: it becomes `completed` after frontend starts and has no process.

## Switch implementation without changing consumers

```sh
fed variant set fixture
fed variant list
fed status
fed restart --all
```

Setting a preference changes **future starts**, not running processes. Before
restart, status still says `catalog (live)`. After restart, reload the same
browser URL: it now says **fixture**, Sample notebook has 3 in stock, and Sample
pencil has 0. The frontend configuration has not changed.

A command-line pin overrides the stored preference for that invocation:

```sh
fed --variant catalog:live restart catalog
fed status
```

The running service is now `live`, while the saved preference remains `fixture`.
`fed variant list` reports what the next start would choose; `fed status`
reports what is running. To pin persistently, use `fed variant set catalog:live`;
`fed variant unset catalog` removes that pin, retaining the preference list.
`fed variant clear` removes all saved choices.

## Start only the fixture, with no storage

```sh
fed stop
fed variant set fixture
fed start catalog
fed status
```

Only catalog runs. Its variant's `depends_on: []` opts out of the shared storage
dependency. `fed start --all` still starts storage because it explicitly selects
all enabled services, independently of dependencies.

The `defaults` block supplies the shared environment, health start period,
startup timeout, and dependency. Storage itself is excluded from **all** of
those defaults because it is named in `defaults.depends_on`. The live variant
uses `extends: live-catalog` to get its command and storage URL from a template.

## Attach to a console and exercise health recovery

```sh
fed --profile console start --all
fed --profile console status
fed attach console
```

Type `catalog` and Enter to see the current response. Type `fail` to make catalog
return HTTP 503, including on its health endpoint. In a second terminal in this
directory, run `fed status`: catalog becomes `failing` after three failed probes
at 200 ms intervals. Its PID stays the same. The frontend also eventually
reports failing because its readiness check follows the real dependency.
Frontend and console use `on_failure: ignore` for catalog so they remain alive
to show errors and accept the recovery command; the default dependency policy
would stop them.

Type `recover` in the attached console. Catalog returns to `healthy`, with the
same PID. Type `catalog` again to confirm real requests work. Press **Ctrl+P,
then Ctrl+Q** to detach without stopping the console. `fed attach console` can
reconnect; `fed --profile console logs console` shows its transcript.

The same health switch works without a console:

```sh
touch .fed/catalog-unhealthy
fed status                 # poll until catalog is failing
rm .fed/catalog-unhealthy
fed status                 # poll until catalog is healthy
```

Recovery of frontend may take longer: it uses the default probe interval of
500 ms rather than catalog's 200 ms. The catalog probe timeout is 500 ms;
startup may wait up to the shared 5-second health start period. A startup
health timeout produces a warning rather than pretending readiness succeeded.

```sh
fed stop
rm -f .fed/catalog-unhealthy
fed variant clear
```

Stop works without the profile flag and cleans up the console, its terminal
host/socket, and the supervisor as well as the HTTP services. The failure
marker deliberately persists across restarts until removed.

## Repeat the checks in a disposable copy

From the repository root:

```sh
cargo build
python3 examples/variant-stack/verify.py --fed target/debug/fed
```

The runner copies this example and the binary into a temporary directory. It
checks real requests, wildcard startup, profiles, variant precedence, a rejected
pin, running-versus-selected status, full restart, dependency opt-out, health
failure/recovery without process restart, and cleanup. Commands, stdout/stderr,
and HTTP responses are retained in the printed `evidence.json`, including a
failed attempt. Services are stopped in a `finally` block. The scratch directory
is retained for inspection and can be deleted afterwards.

Browser rendering and interactive attach/detach are manual parts of the
walkthrough; the runner checks that the console is attachable but does not
simulate an interactive terminal.
