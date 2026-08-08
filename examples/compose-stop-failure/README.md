# Recovering from a failed Compose stop

This project includes a container-runtime wrapper that can inject a failure into
`docker compose rm`. It demonstrates the important failure contract: Fed returns
non-zero and retains its state row while the container is still alive, allowing a
later stop to retry the cleanup.

The Docker integration test copies this project, starts `cache`, injects the stop
failure in a fresh Fed process, inspects both SQLite and Docker, and finally retries
without the fault.
