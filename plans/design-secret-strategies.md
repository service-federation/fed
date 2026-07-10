# Secret Generation Strategies — Design

## Problem

Fed generates secrets as 32-char random alphanumeric strings. This works
for passwords and API keys but doesn't cover:

1. **Asymmetric keypairs** — JWT signing (Ed25519), SSH host keys, TLS
   certificates. One generation produces two values (public + private).
2. **Derived secrets** — A public key derived from a private key. The
   user configures the private key, the public key follows automatically.
3. **Format-specific secrets** — Hex-encoded keys, base64-encoded keys,
   PEM-formatted keys. The generated value must be in the right format.

## Current State

```yaml
parameters:
  DB_PASSWORD:
    type: secret           # → 32-char alphanumeric
  API_KEY:
    type: secret
    source: manual         # → user-provided, no generation
```

All auto-generated secrets use the same strategy: random alphanumeric.

## Proposed API

### Strategy field

Add a `strategy` field to secret parameters:

```yaml
parameters:
  # Default: random alphanumeric (backward compatible).
  DB_PASSWORD:
    type: secret

  # Explicit random with custom length.
  SESSION_SECRET:
    type: secret
    strategy: random
    length: 64

  # Hex-encoded random bytes.
  ENCRYPTION_KEY:
    type: secret
    strategy: hex
    length: 32              # 32 bytes = 64 hex chars

  # Ed25519 keypair — generates private key, derives public key.
  JWT_PRIVATE_KEY:
    type: secret
    strategy: ed25519

  JWT_PUBLIC_KEY:
    type: secret
    derived_from: JWT_PRIVATE_KEY    # automatically derived
```

### Strategies

| Strategy | Output | Use case |
|----------|--------|----------|
| `random` | Alphanumeric string | Passwords, API keys, session secrets |
| `hex` | Hex-encoded random bytes | Encryption keys, HMAC secrets |
| `base64` | Base64-encoded random bytes | Encryption keys (base64 format) |
| `ed25519` | PEM-encoded Ed25519 private key | JWT signing, SSH host keys |
| `rsa` | PEM-encoded RSA private key | Legacy JWT, TLS (if needed) |

Default (no strategy specified): `random` with length 32. Fully backward
compatible.

### Derived secrets

`derived_from` creates a parameter whose value is computed from another:

```yaml
JWT_PRIVATE_KEY:
    type: secret
    strategy: ed25519

JWT_PUBLIC_KEY:
    type: secret
    derived_from: JWT_PRIVATE_KEY
```

The derivation is determined by the source parameter's strategy:
- `ed25519` → public key is extracted from the private key
- `rsa` → public key is extracted from the private key

Fed resolves these in dependency order: generate the source first,
then derive. Derived parameters are **not stored** in the secrets file
— they're computed at startup from the stored source.

Wait — that's a problem. If the derived value isn't stored, every
`fed start` must recompute it, which requires the derivation logic
at runtime. That's fine for keypairs (deterministic), but adds complexity.

**Alternative: store both.** Generate both values and write both to
`.env.secrets`. Simpler, no derivation logic at runtime:

```
# .env.secrets
JWT_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\nMC4CAQ...
JWT_PUBLIC_KEY=-----BEGIN PUBLIC KEY-----\nMCow...
```

**Decision: store both.** The `derived_from` field tells Fed which
parameter to generate first and how to compute the other. Both values
end up in the secrets file. Services read whichever one they need.

### Resolution order for keypairs

1. If both values exist in `.env.secrets`, use them (no regeneration).
2. If only the private key exists, derive the public key and append it.
3. If neither exists, generate the keypair and write both.
4. If only the public key exists, that's an error (can't derive private
   from public).

### Multi-line values in .env files

PEM keys are multi-line. `.env` files traditionally don't handle multi-
line values well. Options:

**A. Single-line with \n escapes:**
```
JWT_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\nMC4C...
```
Most `.env` parsers handle `\n` escaping (dotenvy does).

**B. Base64-encoded PEM:**
```
JWT_PRIVATE_KEY=LS0tLS1CRUdJTi...
```
Service decodes base64 before use. Unambiguous single-line.

**C. Separate file per key:**
```yaml
JWT_PRIVATE_KEY:
    type: secret
    strategy: ed25519
    file: .secrets/jwt-private.pem     # written to a file, not .env
```
Cleanest for PEM keys but introduces a new output mechanism.

**Decision: Option A (escaped newlines).** It's the standard for `.env`
files, dotenvy handles it, and it keeps the single-file model. If a
service needs the raw PEM, it unescapes `\n`.

## Implementation plan

### Phase 1: Strategy + length (no keypairs)

Add `strategy` and `length` fields to `Parameter`:
```rust
pub struct Parameter {
    // ... existing fields ...
    pub strategy: Option<String>,   // "random", "hex", "base64"
    pub length: Option<u32>,        // bytes (hex/base64) or chars (random)
}
```

Update `generate_secret()` to dispatch on strategy:
```rust
pub fn generate_secret(strategy: &str, length: u32) -> String {
    match strategy {
        "hex" => generate_hex(length),
        "base64" => generate_base64(length),
        _ => generate_random_alphanumeric(length),
    }
}
```

Backward compatible: no `strategy` → `random`, no `length` → 32.

### Phase 2: Keypair generation + derived_from

Add `derived_from` field to `Parameter`:
```rust
pub struct Parameter {
    // ... existing fields ...
    pub derived_from: Option<String>,
}
```

Add keypair generation:
```rust
fn generate_ed25519_keypair() -> (String, String) {
    // Returns (private_pem, public_pem) with \n-escaped newlines
}
```

Update `analyze_secrets` to:
1. Sort secrets by dependency (derived_from sources first).
2. Generate sources, then derive dependents.
3. Write both to `.env.secrets`.

Add validation:
- `derived_from` must reference an existing secret parameter.
- Circular `derived_from` is an error.
- Only keypair strategies support derivation.

## Example: Filament config with keypairs

```yaml
generated_secrets_file: .env.secrets

parameters:
  DB_PASSWORD:
    type: secret

  JWT_PRIVATE_KEY:
    type: secret
    strategy: ed25519

  JWT_PUBLIC_KEY:
    type: secret
    derived_from: JWT_PRIVATE_KEY

services:
  identity-svc:
    environment:
      FIL_JWT_PRIVATE_KEY: "{{JWT_PRIVATE_KEY}}"

  gateway:
    environment:
      FIL_JWT_PUBLIC_KEY: "{{JWT_PUBLIC_KEY}}"
```

Identity-svc signs tokens with the private key. Gateway verifies with
the public key. A compromised gateway can't forge tokens.

## Open questions

1. **Should `strategy: ed25519` be a parameter type or a strategy?**
   Could argue it should be `type: ed25519-keypair` instead of
   `type: secret, strategy: ed25519`. The `type` field already has
   `port` and `secret` — adding more types keeps the API consistent.
   But `type: secret` with `strategy` is more extensible.
   **Lean: strategy field.** Keep `type: secret` as the marker.

2. **RSA key size?** If we support RSA, what default size? 2048 is
   minimum, 4096 is recommended. Probably just default to 4096 and
   don't make it configurable initially.

3. **TLS certificate generation?** Self-signed TLS certs are useful
   for local dev (HTTPS without mkcert). Would be `strategy: tls-cert`
   with a `derived_from` for the key. Defer this — it's useful but
   not blocking.
