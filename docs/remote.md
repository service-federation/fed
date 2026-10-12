# Remote environments

**Develop locally, run remotely.** Keep your editor, checkout and language server
on your machine. Run your services in a disposable cloud VM and reach them on
`localhost` through an authenticated tunnel.

Remote environments are currently a limited beta. Public prepaid access and the
launch-credit offer are not open yet.

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
`localhost:5432 -> box:5432  (DB_PORT)`. Each port keeps its number, so a
`localhost:{{DB_PORT}}` value in fed.yaml works in your browser too.

## Local development and syncing

Your editor and language server stay local. Run local installation and codegen
with `fed run TASK`. `fed start` still runs locally; remote operations use the
explicit `fed remote` prefix.

Edits are not continuously synced. Run `fed remote push NAME` after editing, or
`fed remote start NAME` to push and start the stack. Copy any output you need from
the VM before it is deleted; remote-only files are temporary.

## Commands

### `fed remote up [NAME] [--type TYPE] [--disk GB]`

Creates a machine for the linked project and waits until it answers SSH and
has finished setting up. Without a name, it is called `env-HHMMSS`. Names use
1 to 31 lowercase letters, digits and `-`. The default type is `DEV1-S`.

`--disk` sets the size of the machine's disk, a whole number of GB from 10 to
200. Use it when the stack's images and data need more room:
`fed remote up box --disk 60`. Without it, the machine gets the type's default
disk.

The create can take a few minutes. If it takes more than 5, or you press Ctrl-C,
fed stops waiting and tells you whether the machine exists.

### `fed remote ls`

Lists your machines in the linked project, with their address and the time
they have left. When the cloud reports disk sizes, a DISK column shows them. Org admins see everyone's machines, with an OWNER column.

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

Pushes, then runs `fed start` in the workspace. Before that it does three things:

- If the machine has an older fed than yours, it installs your version from
  its GitHub release. A version with no release, such as a build from source,
  gets a warning instead.
- If the checkout uses the team vault, it gives the workspace read access to
  it. See [Secrets](#secrets).
- It picks the stack's ports. The machine works out which ports the stack
  uses, without starting anything. When a program on your laptop uses one of
  them, fed moves that parameter to a port your laptop has free, and says so:
  `Port 3000 is taken on this machine, so NEXT_PORT uses port 52114 on box.`
  The machine keeps the new port for later starts. When a port moves, the
  stack restarts so that every service uses it.

### `fed remote connect NAME [--as WORKSPACE]`

Forwards every port the workspace's stack uses to the same port on this
machine until you press Ctrl-C. Values such as `http://localhost:{{WEB_PORT}}`
are then right on both machines.

When a program on your laptop has taken one of the ports since
`fed remote start`, `connect` forwards nothing. It lists each such port with
its parameter and, when fed can find it, the program. Stop that program, or
run `fed remote start NAME` again to move the port.

### `fed remote ssh NAME [--as WORKSPACE] [-- COMMAND...]`

Opens a shell in the workspace on the machine. With a command after `--`, it
runs that command in the workspace instead and exits with its exit code:

```sh
fed remote ssh box -- fed status
fed remote ssh box -- fed logs api
```

Outside a checkout, the shell opens in root's home folder.

When this computer has keys for a machine by that name in another project, but
not in the linked one, commands use that machine and say so.

### `fed remote down NAME`

Deletes the machine now, revokes its vault tokens and forgets its keys. Run
from a checkout linked to the machine's project, it also deletes a machine you
created on another computer.

## How long a machine lives

- **5 minutes after the last SSH session ends**, the machine powers off. Cloud
  cleanup then deletes it and its resources.
  Every fed remote command and an open `fed remote connect` count as a session.
- **6 hours after it was created**, cloud cleanup enforces its deadline
  independently of software running inside it. `fed remote up` prints the time.

Deletion is asynchronous. The cleanup schedule and provider operations add time
after shutdown or expiry; provider failures can delay it further.

After that, a command on the machine says it no longer exists. Create a new one with
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

## Docker services on the machine

The machine runs Linux. There, fed starts each Docker container with
`--add-host host.docker.internal:host-gateway`, so `host.docker.internal`
reaches the machine as it does on Docker Desktop. A container can use
`postgresql://...@host.docker.internal:{{DB_PORT}}/app` to reach another
service's published port. The same applies when you run fed with Docker on a
Linux laptop. Podman provides the name itself, so fed adds nothing there.

Ports are published as Docker publishes them by default. On the machine, the
cloud's firewall blocks them from outside. You reach them through
`fed remote connect`.

## Files on your laptop

fed keeps each machine's SSH keys and address in `~/.fed/remote/<id>/`,
readable only by you. The keys are made on your laptop and work only for that
machine. They are deleted with the machine.

fed connects with its own SSH settings and ignores `~/.ssh/config`.

## Troubleshooting

**"box no longer exists"**
Someone deleted the machine, or it expired. fed has forgotten its keys. Run
`fed remote up box`.

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

**"fed remote connect cannot forward every port of box"**
A program on your laptop uses a port of the remote stack. Stop it, or run
`fed remote start box` to move the stack to free ports.

**"fed remote start needs a newer fed on box"**
The machine's fed cannot work out the stack's ports. This happens when your
fed is a build from source with no release to install. Use a released fed on
your laptop, so `fed remote start` can install the same release on the machine.

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

## Installing the remote beta

The `v8.4.0-beta.1` prerelease includes remote commands for Linux and macOS.
Download the archive for your platform from the
[beta release](https://github.com/service-federation/fed/releases/tag/v8.4.0-beta.1),
verify its published checksum, and run the extracted `fed` binary. Stable
Homebrew installations remain on the stable release.

Cloud access is still a limited beta: your organisation must be enabled by a
platform admin. Installing the CLI does not enable compute access or allocate a VM.

To build the same version from source:

```sh
cargo install --git https://github.com/service-federation/fed --tag v8.4.0-beta.1 --features remote-beta
```
