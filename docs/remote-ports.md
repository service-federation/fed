# Decision: the same port on the laptop and on the remote environment

Status: accepted, 2026-10-12. Changed the same day from pinning to refusing, see "Why not pinning".

## Problem

`fed remote connect` forwards remote port P from local port P + 10000. A stack that builds URLs from its own ports breaks:

```yaml
NEXTAUTH_URL: 'http://localhost:{{NEXT_PORT}}'          # the VM says localhost:3000
NEXT_PUBLIC_SOCKET_URL: 'http://localhost:{{SOCKET_PORT}}'
```

The browser runs on the laptop at `localhost:13000`. Login callbacks redirect to `localhost:3000`, cookies are set for the wrong origin, and the socket client connects to whatever runs on the laptop's 3001, or to nothing. Plenora has about twenty `localhost:{{...}}` values, so this is not a corner case.

## Options

1. **The same port when it is free, else +10000.** Simple, and right in the common case. When a port is taken on the laptop, the URLs break silently. That is the worst kind of failure.
2. **Override the URL parameters on the remote side.** Each project would need to list which parameters are URLs and rewrite them. That is fragile, and it leaks into every fed.yaml.
3. **Resolve every port so it is free on both machines, and forward it to the same number.** Before `fed start` on the VM, the client works out the stack's ports there, and pins each one the laptop uses to a port the laptop has free.
4. **The same port, and refuse loudly when a laptop port is taken.** `fed remote connect` forwards remote port P to local port P. When the laptop uses one of the ports, it forwards nothing and says which port, which parameter and which program.

## Decision

Option 4. Option 3 was accepted first and built, then dropped.

- `fed remote start` runs plain `fed start` on the VM.
- `fed remote connect` forwards each port to the same number. Before it forwards anything, it checks every port on the laptop. When one is taken, it forwards nothing and lists each taken port with its parameter and, when fed can find it, the program. It says to stop that program and run `fed remote connect` again.
- A program can take a port between that check and the forward. Then that forward fails, and `connect` ends every other forward and fails too. A partial connection would send some requests to the VM and others to the program on the laptop.

`PORT_OFFSET` goes away.

## Why not pinning

Pinning (option 3) needed a new command on the VM to work out ports without starting anything, and one to move a port and restart only the services that use it. Each review found new edge cases: fixed values from `.env`, secrets and generators that the dry run must not touch, Compose imports, which services own a port, and two runs at once. All of that served one rare case: a port the laptop already uses.

That case comes mostly from running the same stack locally and remotely at once, and that is now unsupported. Stop the local stack before `fed remote connect`.

## What it costs

- Running the same project locally and remotely at the same time no longer works. `connect` refuses and names the ports.
- Another program on a laptop port of the stack must stop before `connect`.
- The VM needs no new fed command, so any fed with `fed ports list --json` works there.

## Not covered

- URLs that hard-code a port instead of using `{{PORT}}`. fed cannot see those.
- A browser on another machine than the one running `fed remote connect`.
