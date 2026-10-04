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
