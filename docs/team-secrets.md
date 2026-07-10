# Team Secrets

Share development secrets across your team through [Service Federation Cloud](https://www.service-federation.com). One person sets a secret once; everyone else's `fed start` just works.

**Scope, stated plainly:** this is for *development* secrets — the API keys and OAuth credentials your dev stack needs. It is a dev tool, not a production vault. Values are encrypted at rest and never logged, listings return names only, and removing an org member revokes their access on their next request — but don't put production credentials here.

## Setup (once per team)

1. Sign in at [app.service-federation.com](https://app.service-federation.com) with GitHub.
2. Create an org and a project (a project maps to a repo).
3. Add your teammates by email — existing users are added directly; others get an invite link to share.

## Setup (once per repo)

```bash
fed link acme/web        # writes .fed/cloud.yaml
git add .fed/cloud.yaml && git commit -m "link to acme/web"
```

`.fed/cloud.yaml` is meant to be committed — it's how teammates inherit the link. If your `.gitignore` covers `.fed/`, add `!.fed/cloud.yaml`.

## Setup (once per machine)

```bash
fed login                # browser sign-in; token stored in ~/.fed/credentials (0600)
```

On a box with no browser: `fed login --no-browser`. In CI: set `FED_TOKEN` in the environment instead.

## Daily use

There isn't any. Declare the secret in `service-federation.yaml`:

```yaml
parameters:
  STRIPE_SECRET_KEY:
    type: secret
    source: manual
    description: "From https://dashboard.stripe.com/apikeys"
```

Put the value in the vault (either the dashboard, or):

```bash
fed secrets set STRIPE_SECRET_KEY    # value read from stdin, never argv
```

Every linked, logged-in teammate's `fed start` now resolves it automatically.

## How resolution works

Missing `source: manual` secrets resolve in this order:

1. **Team vault** — fetched over HTTPS, only the names your config declares.
2. **Local cache** — vault values are written to `generated_secrets_file` (mode 0600, must be gitignored), so later runs — including `fed start --offline` — don't need the network.
3. **Error** — anything still missing stops startup with a list of exactly what's needed.

Notes:

- The cache means a revoked member can still read values already on their disk — revocation stops *new* fetches. That's the honest semantics of any local cache.
- `fed secrets ls` shows names and who last updated them, never values.
- Environments: secrets are scoped per environment (`development` by default); `fed -e staging start` fetches the `staging` set.

## Pricing

Free during early access.
