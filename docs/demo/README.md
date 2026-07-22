# fed demo stack

This small stack backs the README recording. It runs PostgreSQL in Docker,
applies an idempotent schema migration, and starts a native Python API only
after both earlier graph nodes are ready.

```sh
fed start
curl http://localhost:18480/health
fed stop
fed clean
```
