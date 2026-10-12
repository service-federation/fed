# Decision: the same port on the laptop and on the remote environment

Status: accepted, 2026-10-12. Not built yet.

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
3. **Resolve every port so it is free on both machines, and forward it to the same number.** The VM is fresh, so almost any port is free there. The laptop is the constraint. If the remote stack only uses ports that are free on the laptop, every `{{PORT}}` value is correct on both sides, and no URL needs rewriting.

## Decision

Option 3.

1. Before the remote stack starts, `fed remote start` asks the VM which ports the stack will use. The VM resolves them and saves them, without starting anything.
2. The client tries to bind each of those ports on the laptop's loopback.
3. For each port that is taken on the laptop, the client picks one that is free on the laptop and pins it on the VM. A pin is an exact value, stored like any resolved port, so later `fed remote start` runs keep it.
4. `fed remote start` then starts the stack. Every resolved port is one the laptop can bind.
5. `fed remote connect` forwards each port to the same number. If a port became taken on the laptop since `start`, `connect` stops with a message that names the port and, when it can, the process. It says to stop that process or to run `fed remote start` again, which pins a new port and restarts the services that use it.

`PORT_OFFSET` goes away.

## What it costs

- Two ssh round trips before `start` (resolve, then pin), about a second.
- A port the laptop takes between `start` and `connect` means a restart of the services that use it. That is rare, and the message says what to do.
- Running the same project locally and remotely at the same time still works. The local stack holds its ports, so the remote stack gets other ones, and URLs on each side are correct for that side.

## Not covered

- URLs that hard-code a port instead of using `{{PORT}}`. fed cannot see those.
- A browser on another machine than the one running `fed remote connect`.
