# Portfolio demo (local, isolated, real execution paths on a test token)

One shell entry point runs a complete local demo of TxHarbor:

1. **Deposit (stage A)** — a real `transfer` to a watched address on a fresh Anvil chain is
   observed as `pending`, stays `pending` at confirmation depth `N-1` and becomes `confirmed`
   once depth `N=3` is reached (threshold, confirmation count and policy sequence are asserted).
2. **Withdrawal (stage B)** — a real `POST /withdrawals` (201), execution admission (201), a real
   `signer-serve` + `withdrawal-worker` execution with a real on-chain payment, then replays with
   the *same caller + idempotency key* (create → 200/one `request_id`, admission → 200/one
   `intent_id`) and a bounded 30 s observation window proving nothing doubles.

Everything is local and disposable: one compose project `txharbor-demo-<run-id>`, one fresh
Postgres, one fresh Anvil, one evidence root. The default `txharbor` stack, its volume and its
ports are never touched, and **nothing is ever deleted** — see “Retention”.

> **Not for production.** The demo uses a fixture ERC-20, an Anvil development key for the
> signer, localhost-only bindings and a disposable Postgres. It proves wiring and real execution
> paths; it is not a hardened deployment.

## Prerequisites

- `bash` 5, `curl`, `go` (builds the two demo binaries), GNU coreutils `timeout` (required for every
  bounded call), Docker CLI with Compose v2 (**≥ 2.24.4** — the port overlay uses the `!override`
  merge key).
- A **local** docker context: `docker context inspect <ctx> --format '{{.Endpoints.docker.Host}}'`
  must be `unix://…`. `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH` must be unset and
  the caller must not export `TXHARBOR_RPC_URL` (preflight refuses both before any connection).
- The token fixture must be compiled once (below).
- Host ports `55436`, `58547`, `18081`, `18091` must be free.

## One-time fixture preparation

```bash
bash scripts/portfolio-demo/compile_token.sh --fetch     # once, needs network: pinned solc 0.8.30
bash scripts/portfolio-demo/compile_token.sh --compile   # writes ABI/creation/runtime + PROVENANCE.md
bash scripts/portfolio-demo/compile_token.sh --verify    # recomputes every hash vs PROVENANCE.md
```

The compiler is fetched into the isolated tools directory
(`${TXHARBOR_DEMO_TOOLS_DIR:-/tmp/opencode/txharbor-demo-tools}`), never installed system-wide.
`run_demo.sh` refuses to start without `scripts/portfolio-demo/fixtures/MinimalERC20.creation.bin`.

## Run

```bash
bash scripts/portfolio-demo/run_demo.sh                  # mode: run_all (preflight → setup → A → B → verify)
bash scripts/portfolio-demo/run_demo.sh --preflight-only # guards only; prints "PREFLIGHT PASS (no resources)"
bash scripts/portfolio-demo/run_demo.sh --help
```

A successful run prints `EVIDENCE: .evidence/portfolio-demo/<run-id>` when the stages finish and
`RESULT: PASS` only after cleanup (process stop + container stop) also succeeded, so a stop failure
can never read as a pass. Every failure prints `FAIL: …` (or `RESULT: FAIL exit=<rc>`) and exits
non-zero; polling always has a deadline, so a hung step fails instead of hanging.

**Status: verified.** The last real run — `run04w-20261011130031` on 2026-10-11, at source
HEAD `11d698c75edf14e28e7075300ca082fe6dc9a308` — completed all stages (A, B, N5 refusals, final
verify), passed the public secrets scan, and exited `run_rc=0` / `exit_code=0` / `cleanup_rc=0`
(`RESULT: PASS`; wall clock 132 s on an already-prepared host — not a cold-start promise). A
curated, tracked excerpt of its public evidence (raw byte copies + provenance + `SHA256SUMS`)
lives in
[`docs/evidence/portfolio-demo/run04w-20261011130031/`](../../docs/evidence/portfolio-demo/run04w-20261011130031/README.md);
the full original tree stays in the local git-ignored `.evidence/portfolio-demo/run04w-20261011130031/`.
The talk track that walks through those files is §2.2 of
[`docs/portfolio.md`](../../docs/portfolio.md). The negative-case drills (N1–N4) below were **not**
re-run in that round. Runs are still per-id and disposable: this status describes one local
execution, not a guarantee that other machines have reproduced it.

Optional environment inputs: `PORTFOLIO_RUN_ID` (default `UTC-timestamp-PID`) and
`TXHARBOR_DEMO_TOOLS_DIR`. Host ports are fixed by the script (`55436`, `58547`, `18081`, `18091`);
the overlay knobs `PORTFOLIO_PG_PORT` / `PORTFOLIO_ANVIL_PORT` exist for hand-driven compose use and
must stay at those defaults for `run_demo.sh`.

## Reading the evidence

```
.evidence/portfolio-demo/<run-id>/          (0700)
  public/   0700 (files 0600)  summary.txt, resources.txt, run-metadata.txt, run-exit.txt
                  chain/ (deposit-receipt.json, signing-receipt.json, transfer-log-query.json,
                          transfer-log-query.filter.txt, final-state.txt)
                  http/  (create/admission/replay/negative HTTP bodies + statuses)
                  sql/   (whitelisted read-only query snapshots)
  private/  0700  keys/ (0600 API keys, signer credential, dev signer key)
                  env/  (0600 serve.env / signer.env / worker.env; contains the dev DSN)
                  curl/(0600 request bodies + curl configs carrying the Bearer token)
                  logs/ (service logs, CLI paper trail)
```

- `public/summary.txt` is the human-readable run record; `public/run-metadata.txt` records the git
  HEAD/dirty count and the sha256 of every script and fixture used; `public/run-exit.txt` records
  the final exit/cleanup codes; `public/chain/` holds the raw receipts (deposit and signing) plus
  the `eth_getLogs` uniqueness query output, `public/http/` and `public/sql/` hold the status and
  count snapshots the assertions are made from.
- **The demo proves the evidence itself is clean**: it writes `private/scan-patterns.txt` (0600)
  with the exact secret values of this run and fails the run if any of them — or the marker
  `signed_tx_bytes` — appears anywhere under `public/`. Matched content is never printed.
- **The `eth_getLogs` uniqueness query uses raw 32-byte topics**, never an event signature:
  foundry v1.8.1 `cast logs` builds topics only from *indexed* event inputs
  (`crates/cast/src/cmd/logs.rs`: `build_filter` → `build_filter_event_sig`), so
  `Transfer(address,address,uint256)` yields topic0 only and leaves topic1/topic2 as wildcards.
  The query passes `TRANSFER_SIG` plus the zero-padded signer/recipient as topics 0/1/2; the
  filter metadata and the raw stdout are written to this run's **private** 0600 logs before any
  assertion, and only copied into `public/chain/` after every field check passed (the final
  secrets scan still governs what may be shared). Regression without a chain:
  `checks/log-filter/run_checks.sh` (pure mocks).
- **Historical status of that check**: the pre-fix run `run04v-20261011120444` passed stage A and
  stage B through B6 (completed intent, stable replays, conserved balances) but failed the final
  verify on `getLogs ... count at head 9: want [1] got [4]`; its raw `getLogs` JSON was never
  retained (the old code wrote it only after the count assertion), so the contents of those four
  matched logs are unknown. The post-fix real rerun **passed** (`run04w-20261011130031`, see
  `docs/changes/portfolio-demo.md` §19–§20) — the pre-fix failure record is kept, not rewritten.
- `private/` is sensitive (keys, credentials, DSN password, full logs). Never commit, paste or
  package it; share `public/` only.
- The `.evidence/` tree is already git-ignored (`.git/info/exclude`).
- Honest boundaries: `outbox_events` rows are all `publish_state=pending` — this demonstrates the
  **transactional outbox write**, not event delivery (no events pipeline runs). Exactly one
  configuration row is written by SQL (the `withdrawal_authorization_scopes` scope/fee ceilings,
  because `request_id` is minted server-side and no CLI path can bind it); callers, API keys,
  permissions, nonce registry and the upstream grant are configured through the existing admin
  CLI, and every business fact (request, intent, binding, signing, broadcast, chain effect) comes
  from the real production code paths.

## Negative cases (safety drills)

Each drill is a `--preflight-only` invocation, i.e. a guards-only run: no container, no volume, no
network and no stack is created, and no chain or database write happens. A refusal exits non-zero
with a `FAIL: refused: …` line.

```bash
# N1 — occupied host port (run one of these in another shell first):
python3 -m http.server 55436 --bind 127.0.0.1     # or: nc -l 127.0.0.1 55436
PORTFOLIO_RUN_ID=n1 bash scripts/portfolio-demo/run_demo.sh --preflight-only
#   → FAIL: refused: 127.0.0.1:55436 is already in use

# N2a — evidence root already exists (pre-created by the caller):
mkdir -p .evidence/portfolio-demo/n2
PORTFOLIO_RUN_ID=n2 bash scripts/portfolio-demo/run_demo.sh --preflight-only
#   → FAIL: refused: evidence root already exists: .evidence/portfolio-demo/n2

# N2b — the run's project resources already exist (inspect before you run):
docker ps -aq --filter label=com.docker.compose.project=txharbor-demo-n2
docker network ls -q --filter 'name=^txharbor-demo-n2_default$'
docker volume ls -q --filter 'name=^txharbor-demo-n2_pgdata$'
#   any non-empty answer → the same invocation refuses before any side effect

# N3 — caller-supplied RPC override input (refused with **no connection at all**):
TXHARBOR_RPC_URL=http://127.0.0.1:8545 bash scripts/portfolio-demo/run_demo.sh --preflight-only
#   → FAIL: refused: TXHARBOR_RPC_URL is set …

# N4 — remote / non-unix docker context (refused before touching a daemon):
DOCKER_HOST=tcp://127.0.0.1:2375 bash scripts/portfolio-demo/run_demo.sh --preflight-only
#   → FAIL: refused: DOCKER_HOST is set …
```

Preflight order is mandatory and each step is refuse-only: N3/N4 create nothing at all (not even
an evidence directory); N1/N2a/N2b may leave an empty `.evidence/portfolio-demo/<run-id>/` behind
from the earlier evidence-root step — still no containers, volumes, networks or stack.

## Retention

- **Nothing is deleted.** On exit (success or failure) the script stops *its own* processes with
  `TERM` (bounded wait; a timeout is reported as a failure and the process is left running, never
  `KILL`ed) and stops only the containers that carry both its compose project label and its
  per-run `txharbor.demo.run_token` label — never `compose stop`/`down`, never by project name
  alone. It never runs `down`, `down -v`, `rm`, `prune` or any volume/network deletion.
- Containers, the `txharbor-demo-<run-id>_pgdata` volume and the `txharbor-demo-<run-id>_default`
  network stay in place, stopped, with the full Postgres volume state. Anvil has **no state
  persistence** in the base compose file (in-memory chain): once the container stops the chain is
  not resumable; only the exported chain evidence (`public/chain/` receipts and the log query
  output) is retained, and no recovery feature is added.
- Cleanup of a finished run (containers/volume/network/evidence) requires a **separate explicit
  authorization**; because runs are per-id, a new run never reuses or mutates an old one.
- Evidence: keep `public/` for review, treat `private/` as secret material, delete a whole run
  directory only when its data is no longer needed.
- Re-running is safe: a new `PORTFOLIO_RUN_ID` (timestamp + PID by default) means a new project,
  a new volume, a new evidence root and no interference with earlier runs. Host ports are fixed
  (`55436`, `58547`, `18081`, `18091`): a stopped earlier stack releases them, and if anything
  still holds one, preflight refuses instead of retargeting.

## Identities and credentials

- One logical caller (`CALLER_ID=7001`) owns the withdrawal request and its `withdrawal_authorizations`
  row **and** is the principal the 009 signer credential authenticates as. 009 scopes every grant read
  to the authenticated `caller_id` and refuses a foreign-owned grant as `authorization_invalid` (403),
  so a separate signer principal would stall stage B with the intent stuck in `executing`.
- That is **one owner, not one secret**: the API key (`private/keys/caller.key`), the issuer key
  (`private/keys/issuer.key`) and the signer credential (`private/keys/signer.cred`) are three
  independently issued 0600 secrets; the worker is started with the signer credential only, and the
  issuer stays its own identity (`ISSUER_CALLER_ID=9001`).
- Preflight refuses a mismatched signer principal before any daemon/CLI/evidence action, and setup
  reads the durable grant owner back (read-only `SELECT`) to refuse a foreign or missing `caller_id`
  at setup time instead of 240s into stage B. Mock regression:
  `checks/principal/run_checks.sh` (no daemon, no database).

## The “about 5 minutes” claim

Five minutes describes the **walkthrough after preparation**: with images, tools and the fixture
already built and the stack warm, narrating stage A (pending → N-1 → confirmed), stage B (create →
admission → real execution → replays) and the verify output fits in a short talk. It is **not** a
cold-start promise: the first run also pays for image pulls, the one-time `solc` fetch/compile,
`go build`, migrations and the first indexer passes. Measured evidence lives in the run's own
`public/summary.txt`; do not quote a duration the run did not measure.
