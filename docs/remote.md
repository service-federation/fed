# Remote environments

With `fed remote`, fed runs your stack on a fresh machine in Service Federation
Cloud instead of on your laptop. You copy your checkout there, start it with
`fed start` as usual, and reach its ports on `localhost`. The machine deletes
itself when you stop using it.

Use it when your laptop is short on memory or CPU, or when you want a clean
machine to try something on.

## What you need

- **macOS or Linux.** On Windows, run fed in WSL.
- **`ssh`, `ssh-keygen` and `rsync`.** OpenSSH comes with macOS and most Linux
  systems. `fed remote up` checks for all three before it creates anything.
- **A login and a linked checkout.** Run `fed login`, then `fed link org/project`
  in the checkout.
- **Remote environments turned on for your org.** If they are not, `fed remote up`
  says so. Ask a Service Federation admin to turn them on.

## Quick start

Run these from the linked checkout:

```sh
fed remote up box         # create a machine called box, wait until it is ready
fed remote start box      # copy this checkout to it and run fed start there
fed remote connect box    # reach its ports on localhost, Ctrl-C to stop
fed remote down box       # delete it when you are done
```

`connect` prints where each port went, for example
`localhost:15432 -> box:5432  (DB_PORT)`.

## Commands

### `fed remote up [NAME] [--type TYPE]`

Creates a machine for the linked project and waits until it answers SSH and
has finished setting up. Without a name, it is called `env-HHMMSS`. Names use
1 to 31 lowercase letters, digits and `-`. The default type is `DEV1-S`.

The create can take a few minutes. If it takes more than 5, or you press Ctrl-C,
fed stops waiting and tells you whether the machine exists.

### `fed remote ls`

Lists your machines in the linked project, with their address and the time
they have left. Org admins see everyone's machines, with an OWNER column.

`ls` also forgets this machine's keys for every environment the cloud has
deleted, in any project.

### `fed remote push NAME [--as WORKSPACE]`

Copies this checkout to `/srv/<workspace>` on the machine. The workspace is
the checkout's folder name unless you pass `--as`.

- **What goes:** the files `git ls-files` lists, so tracked files and untracked
  files that are not ignored. `.git` never goes. From `.fed/` only
  `.fed/cloud.yaml` goes, the link to the team vault.
- **What is deleted:** a file an earlier push copied, and that this checkout no
  longer has, is deleted on the machine. Nothing else is. Build output,
  `node_modules` and fed's own state on the machine stay.
- **Submodules** are skipped, with a warning.

### `fed remote start NAME [--as WORKSPACE]`

Pushes, then runs `fed start` in the workspace. Before that it does two things:

- If the machine has an older fed than yours, it installs your version from
  its GitHub release. A version with no release, such as a build from source,
  gets a warning instead.
- If the checkout uses the team vault, it gives the workspace read access to
  it. See [Secrets](#secrets).

### `fed remote connect NAME [--as WORKSPACE]`

Forwards every port the workspace's stack uses to this machine until you press
Ctrl-C. Remote port P is on local port P + 10000, so the same stack can also
run on your laptop. Ports above 55535 keep their number.

A port that is taken on your laptop gets a warning, and the other ports still
work.

### `fed remote ssh NAME [--as WORKSPACE] [-- COMMAND...]`

Opens a shell in the workspace on the machine. With a command after `--`, it
runs that command in the workspace instead and exits with its exit code:

```sh
fed remote ssh box -- fed status
fed remote ssh box -- fed logs api
```

Outside a checkout, the shell opens in root's home folder.

### `fed remote down NAME`

Deletes the machine now, revokes its vault tokens and forgets its keys. Run
from a checkout linked to the machine's project, it also deletes a machine you
created on another computer.

## How long a machine lives

- **5 minutes after the last SSH session ends**, the machine deletes itself.
  Every fed remote command and an open `fed remote connect` count as a session.
- **6 hours after it was created**, it is deleted in any case. `fed remote up`
  prints the time.

After that, a command on the machine says it was deleted. Create a new one with
`fed remote up NAME`. Nothing on the old machine is kept.

## Limits

Your org decides how many machines it may have at once, which types it may
use, and how many machine hours it may use each month. Each person may also
have only a few machines at once. When a limit is reached, `fed remote up` says
which one.

## Secrets

`fed remote start` gives the workspace a vault token: read access to the linked
project's secrets, and nothing else.

- The token is made for that workspace and lasts until the machine's deadline.
  It is revoked when the machine is deleted, and by `fed remote down`.
- It travels to the machine over SSH, never in a command line. On the machine
  it is in `/run/fedenv/tokens/<workspace>`, readable only by root, in memory.
- fed on the machine uses it for commands run inside `/srv/<workspace>`.

## Files on your laptop

fed keeps each machine's SSH keys and address in `~/.fed/remote/<id>/`,
readable only by you. The keys are made on your laptop and work only for that
machine. They are deleted with the machine.

fed connects with its own SSH settings and ignores `~/.ssh/config`.

## Troubleshooting

**"box was deleted (it was idle for 5 minutes or reached its 6-hour limit)"**
The machine is gone, and fed has forgotten its keys. Run `fed remote up box`.

**"Remote environments are not enabled for acme"**
Your org cannot create machines yet. Ask a Service Federation admin to turn
them on.

**"the rsync on PATH is openrsync"**
openrsync cannot do the copy fed needs. Install rsync with `brew install rsync`
and make sure it comes first in your PATH. The `/usr/bin/rsync` that comes with
macOS works.

**"cannot connect to box over SSH"**
The machine exists but does not answer. Check your network. A firewall that
blocks outgoing SSH on high ports also causes this. The error ends with what
ssh said.

**A port is taken.** `connect` warns about each forward that stopped. Free the
local port, or stop the local stack, and run `connect` again.

**"fed X has no release to install"**
You run a fed built from source that is newer than the machine's. Commands on
the machine use the older fed.

## FAQ

**Can two checkouts share one machine?** Yes. Each gets its own workspace,
named after its folder. Pass `--as` to pick another name.

**Can I keep a machine running overnight?** No. A machine lives at most 6
hours. Create a new one when you need it.

**Are my files kept after the machine is deleted?** No. Push again to a new
machine.

**Can I use my own SSH key?** No. fed makes a key pair for each machine, and
only that key gets in.

## Building with remote enabled

For now, `fed remote` is only in builds with the `remote-beta` cargo feature.
Release builds and Homebrew do not have it yet. To build it from source:

```sh
cargo install --git https://github.com/service-federation/fed --features remote-beta
```
