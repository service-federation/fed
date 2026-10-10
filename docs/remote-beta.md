# Remote environments (closed beta)

With `fed remote`, fed runs your stack on a disposable machine in Service
Federation Cloud instead of on your laptop. The commands exist only in builds
with the `remote-beta` cargo feature. Release builds and Homebrew do not have
them.

## Install

To get the beta build, install fed from source with the feature:

```sh
cargo install --git https://github.com/service-federation/fed --features remote-beta
```

You also need `ssh`, `ssh-keygen` and `rsync` on your machine, a `fed login`,
and a checkout linked with `fed link org/project`. Your org must be in the beta.
If it is not, `fed remote up` says so.

## Use

Run these from the linked checkout:

```sh
fed remote up box             # create the machine, wait until SSH answers
fed remote start box          # copy this checkout to /srv/<folder>, run fed start there
fed remote connect box        # forward the stack's ports to this machine, Ctrl-C to stop
fed remote ssh box -- fed status
fed remote ls                 # your machines in this project, with time left
fed remote down box           # delete it now
```

- **`--as NAME`** picks the folder under `/srv/`. It defaults to the checkout's
  folder name, so two checkouts (or two people) can share one machine.
- **`push`** copies without starting. It sends the files `git ls-files` lists
  (tracked, plus untracked files that are not ignored). It never sends `.git`.
  From `.fed/` it sends only `cloud.yaml`.
- **`connect`** forwards remote port P from local port P + 10000, so the same
  stack can also run locally. Ports above 55535 keep their number.
- **`start`** gives the workspace read access to the project's team vault, with
  a token that expires at the machine's deadline. `down` revokes it.

## When a machine goes away

A machine deletes itself **5 minutes after the last SSH session ends**, and at
its deadline in any case. `fed remote up` prints the deadline. An open
`fed remote connect` counts as a session.

## Local files

fed keeps each machine's SSH keys and address in `~/.fed/remote/<id>/`, readable
only by you. The keys are made on your machine and are used only for that
machine. `fed remote ls` and `fed remote down` delete the folder when the
machine is gone.
