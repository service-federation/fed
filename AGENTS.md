# Notes for coding agents

Repo-specific facts that are easy to get wrong. For build/test/PR mechanics see
CONTRIBUTING.md.

## Cloud protocol: client version and 426 Upgrade Required

All HTTP to Service Federation Cloud (the vault) lives in `src/cloud/mod.rs`,
and both clients there are built via `client_builder()`. That builder attaches
two things the server relies on:

- `x-fed-version: <CARGO_PKG_VERSION>` (`VERSION_HEADER`) on every request
- a `fed/<version>` user agent

The server may answer any cloud request with **426 Upgrade Required** when the
client is too old for the protocol it requires. fed maps 426 to an error telling
the user to upgrade (`api_error`, plus a dedicated arm in
`revoke_current_token`). Clients ≤ 7.2.0 predate all of this: they send no
version header and print only a generic HTTP error on 426, so the server must
treat a missing `x-fed-version` as "too old to say".

When touching this code, keep the contract intact:

- New cloud requests must go through `client_builder()` (never a bare
  `reqwest::Client::builder()`), so they carry the version header.
- 426 handling must stay in the shared `api_error` mapping so every vault
  request gets the upgrade hint.
- The server side lives in the `service-federation-cloud` repo; its agent docs
  (`app/AGENTS.md` there) describe the same contract from the server's view.
  Change the two together.
