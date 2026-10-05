# Protected-entry negatives (2026-10-04 implement round; zero-write class)

Disposable lane: container r2lab-pg2, socket /var/run/postgresql ident map
`pg_map postgres recovery_r`, HBA order writer-reject / recovery-peer /
tcp-writer-reject / tcp-recovery-reject BEFORE all-trust/scram lines.
Zero-object observation on the target DB after the protected-lane negatives
(public.rels = 0, confirmation_policy_history absent).

| # | negative | observed refusal |
|---|---|---|
| P1 | writer_owner over local socket (peer-lane entry) | `pg_hba.conf rejects connection for host "[local]", user "writer_owner"` (rule 1) |
| P2 | writer_owner over loopback TCP | `pg_hba.conf rejects connection for host "127.0.0.1", user "writer_owner"` (rule 3) |
| P3 | recovery_r over loopback TCP | `pg_hba.conf rejects connection ... user "recovery_r"` (rule 5) — no TCP ingress for the recovery role (R3) |
| P4 | recovery_r password attempts (TCP) | same server-side rejection (client-side refused earlier by recovery-route validation, code unit-tested in TestValidateRecoveryRouteRefusals) |
| P5 | wrong OS identity (root, no map) → recovery_r peer | `Peer authentication failed for user "recovery_r"` — no fabricated identity route |
| P6 | wrong_lane_role (equivalent grants, NOT the recovery identity) — real restore of the FULL TxHarbor-scheme archive on lane_neg per PROTECTED_NEG_lane | exit 0 but WRONG-OWNERSHIP objects land under wrong_lane_role; comparator-level refusal before any guard/acceptance; zero-write companion: the admission/comparator path refuses a non-recovery role BEFORE any guard prepare (unit-level) and the wrong-lane databases are outside the trusted-target comparator, so a recovery admission can never mint for them |

Commands were executed manually inside the container and are literal in this
README (human-organized record; not a scripted CI artifact). No production
database touched; container destroyed afterwards. Original TSV: r2_grant_matrix.tsv.

## Raw retention status (audited 2026-10-05)

Audit scope: `/tmp/r2lab` in full (top-level logs, every `run_*/scratch/` incl. its empty
`tmp/`, keyword sweep for protected-entry / P1..P6 / lane_neg / `neg_*` / ident / pg_map /
r2lab), host-side copies, this repository's index/history for `raw/protected-entry/`, and
the harness session store for the executing round (the source of `session-extract.md`).

Result: no container-local or host-side raw stderr/output file survives for P1–P6. The
round ran in the one-off container `r2lab-pg2` (disposable, removed after the round); its
in-container `/tmp/r2lab/` files vanished with the container and no host-side copy landed
for them. The harness session trace of that round is preserved as `session-extract.md`
(verbatim tool calls and outputs, passwords redacted); each case below cites its section.
`r2lab_toc.txt` / `r2lab_plaintext.sql` exist only as the committed copies under this
inventory directory; their `/tmp` originals are gone.

- `raw/neg_lane` (referenced by the TSV row `PROTECTED_NEG_lane`): no artifact. On disk:
  absent under `docs/evidence/015/`. In git: `git ls-files -- 'docs/evidence/015/r2-grant-inventory/raw/neg_lane*'`
  is empty and `git log --all -- '*neg_lane*'` is empty. At audit time the only tracked file
  under `raw/protected-entry/` was this README (`git ls-files`); `session-extract.md` is the
  new harness-trace extract. Its P6 section preserves the restore attempt 1 (exit=2) and
  attempt 2 (exit=0) commands and outputs verbatim; the `neg_lane.err` file itself still
  does not exist.

| # | raw retention | verifiable basis (without raw) |
|---|---|---|
| P1 | missing (originals); harness extract preserved | this README P1 row (literal record); `session-extract.md` §P1 |
| P2 | missing (originals); harness extract preserved | this README P2 row; `session-extract.md` §P2 |
| P3 | missing (originals); harness extract preserved | this README P3 row; `session-extract.md` §P3 |
| P4 | missing (originals); harness extract preserved | this README P4 row; `session-extract.md` §P4; client-side leg anchored by unit test `TestValidateRecoveryRouteRefusals` (`internal/recovery/targetwriter_recoveryroute_linux_test.go:20`) exercising `validateRecoveryRoute` (`internal/recovery/targetwriter_linux.go:1107`) |
| P5 | missing (originals); harness extract preserved | this README P5 row; `session-extract.md` §P5 |
| P6 | missing (originals); harness extract preserved | this README P6 row + `r2_grant_matrix.tsv` row `PROTECTED_NEG_lane` (its `PROTECTED_ENTRY` / `raw/protected-entry/*` cross-reference has no further artifact in this repository); `session-extract.md` §P6 |

No per-case P1–P5 rows exist in `r2_grant_matrix.tsv`; its only protected-entry
cross-reference is the `PROTECTED_NEG_lane` disposition (P6).

Zero-write assertions:
- Header zero-object observation (public.rels = 0, confirmation_policy_history absent):
  verbatim in `session-extract.md` (`zero-object check on lane_target`) — now verifiable
  via session-extract.md.
- P6 zero-write companion (comparator/admission refusal of a non-recovery role before any
  guard prepare): no verbatim run in the extract.
bounded re-verification pending (needs separate authorization)
