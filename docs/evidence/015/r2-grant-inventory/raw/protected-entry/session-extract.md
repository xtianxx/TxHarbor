# Protected-entry session extract (P1–P6)

extracted from OMP harness session store; verbatim except password redaction

- session file: /home/dream/.omp/agent/sessions/-product_env-TxHarbor/2026-10-04T01-33-38-483Z_01a1048b-9333-7696-aee2-e252d40df319.jsonl
- line numbers below are 1-based file lines in that JSONL: `command (line N)` is the stored tool-call message, `output (line M)` its stored tool-result message.
- round: 2026-10-04 implement round, one-off container `r2lab-pg2` (removed 2026-10-04T05:17:21Z — see Aftermath).
- redaction: password literals masked as `<REDACTED>`; every other byte is verbatim from the stored tool call / tool result text.
- these are harness-recorded commands and outputs. The container-local raw files (e.g. `/tmp/r2lab/neg_lane.err`) never landed on the host and do not exist; see README `## Raw retention status`.

## P1 — writer_owner over local socket (rule 1)

source: lines 1853→1855, 1856→1858, 1886→1888

### HBA rules read — 2026-10-04T04:43:30.343Z
command (line 1853):
```sh
docker exec r2lab-pg2 psql -U postgres -d postgres -t -c "SELECT rule_number, type, database, user_name, auth_method, error FROM pg_hba_file_rules LIMIT 6;"
```
output (line 1855):
```text
           1 | local | {all}         | {writer_owner} | reject      | 
           2 | local | {all}         | {recovery_r}   | peer        | 
           3 | local | {all}         | {all}          | trust       | 
           4 | host  | {all}         | {all}          | trust       | 
           5 | host  | {all}         | {all}          | trust       | 
           6 | local | {replication} | {all}          | trust       | 



Wall time: 0.52 seconds
```

### writer_owner local socket refused — 2026-10-04T04:43:42.042Z
command (line 1856):
```sh
docker exec r2lab-pg2 sh -c 'psql "host=/var/run/postgresql port=5432 dbname=lane_target user=writer_owner" -c "SELECT 1;"' 2>&1 | head -3
```
output (line 1858):
```text
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  pg_hba.conf rejects connection for host "[local]", user "writer_owner", database "lane_target", no encryption


Wall time: 0.52 seconds
```

### re-confirmed with rule table — 2026-10-04T04:45:59.498Z
command (line 1886):
```sh
docker exec r2lab-pg2 sh -c 'HBA=$(psql -U postgres -d postgres -t -A -c "SHOW hba_file"); printf "%s\n" "local all writer_owner reject" "local all recovery_r peer map=pg_map" "$(grep -v "recovery_r" /tmp/hba.bak)" > /tmp/hba.new; cp /tmp/hba.new "$HBA"; psql -U postgres -d postgres -c "SELECT pg_reload_conf();" >/dev/null'; docker exec r2lab-pg2 psql -U postgres -d postgres -t -c "SELECT rule_number, user_name, auth_method FROM pg_hba_file_rules LIMIT 3;"; docker exec -u postgres r2lab-pg2 sh -c 'psql "host=/var/run/postgresql port=5432 dbname=lane_target user=writer_owner" -c "SELECT 1;"' 2>&1 | head -2
```
output (line 1888):
```text
           1 | {writer_owner} | reject
           2 | {recovery_r}   | peer
           3 | {all}          | trust

psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  pg_hba.conf rejects connection for host "[local]", user "writer_owner", database "lane_target", no encryption


Wall time: 1.41 seconds
```

## P2 — writer_owner over loopback TCP (rules 3–4)

source: lines 1898→1900, 1901→1903, 1913→1915, 1916→1918

### TCP reject rules appended — 2026-10-04T04:46:56.864Z
command (line 1898):
```sh
docker exec r2lab-pg2 sh -c 'HBA=$(psql -U postgres -d postgres -t -A -c "SHOW hba_file"); printf "%s\n" "$(cat /tmp/hba.new)" "host all writer_owner 127.0.0.1/32 reject" "host all writer_owner ::1/128 reject" "host all recovery_r 127.0.0.1/32 reject" "host all recovery_r ::1/128 reject" > "$HBA"; psql -U postgres -d postgres -c "SELECT pg_reload_conf();" >/dev/null; psql -U postgres -d postgres -t -c "SELECT rule_number, type, user_name, auth_method FROM pg_hba_file_rules ORDER BY rule_number LIMIT 6;"'
```
output (line 1900):
```text
           1 | local | {writer_owner} | reject
           2 | local | {recovery_r}   | peer
           3 | local | {all}          | trust
           4 | host  | {all}          | trust
           5 | host  | {all}          | trust
           6 | local | {all}          | trust



Wall time: 0.56 seconds
```

### rule order after append (reject rules after trust/scram) — 2026-10-04T04:47:11.410Z
command (line 1901):
```sh
docker exec r2lab-pg2 sh -c 'psql -U postgres -d postgres -t -c "SELECT rule_number, user_name, auth_method FROM pg_hba_file_rules ORDER BY rule_number;"' | head -12
```
output (line 1903):
```text
           1 | {writer_owner} | reject
           2 | {recovery_r}   | peer
           3 | {all}          | trust
           4 | {all}          | trust
           5 | {all}          | trust
           6 | {all}          | trust
           7 | {all}          | trust
           8 | {all}          | trust
           9 | {all}          | scram-sha-256
          10 | {writer_owner} | reject
          11 | {writer_owner} | reject
          12 | {recovery_r}   | reject


Wall time: 0.46 seconds
```

### rebuilt HBA with TCP reject rules first — 2026-10-04T04:48:20.411Z
command (line 1913):
```sh
docker exec r2lab-pg2 sh -c 'HBA=$(psql -U postgres -d postgres -t -A -c "SHOW hba_file"); printf "%s\n" "local all writer_owner reject" "local all recovery_r peer map=pg_map" "host all writer_owner 127.0.0.1/32 reject" "host all writer_owner ::1/128 reject" "host all recovery_r 127.0.0.1/32 reject" "host all recovery_r ::1/128 reject" "local   all             all                                     trust" "host    all             all             127.0.0.1/32            trust" "host    all             all             ::1/128                 trust" "local   replication     all                                     trust" "host    replication     all             127.0.0.1/32            trust" "host    replication     all             ::1/128                 trust" "host all all all scram-sha-256" > "$HBA"; psql -U postgres -d postgres -c "SELECT pg_reload_conf();" >/dev/null; psql -U postgres -d postgres -t -c "SELECT rule_number, type, user_name, auth_method FROM pg_hba_file_rules ORDER BY rule_number LIMIT 8;"'
```
output (line 1915):
```text
           1 | local | {writer_owner} | reject
           2 | local | {recovery_r}   | peer
           3 | host  | {writer_owner} | reject
           4 | host  | {writer_owner} | reject
           5 | host  | {recovery_r}   | reject
           6 | host  | {recovery_r}   | reject
           7 | local | {all}          | trust
           8 | host  | {all}          | trust



Wall time: 0.56 seconds
```

### writer_owner loopback TCP refused — 2026-10-04T04:48:36.930Z
_(shared trace lines 1916→1918; same call also covers P3/P4)_
command (line 1916):
```sh
docker exec r2lab-pg2 sh -c 'psql "host=127.0.0.1 port=5432 dbname=lane_target user=writer_owner" -c "SELECT 1;"' 2>&1 | head -2; echo; docker exec r2lab-pg2 sh -c 'psql "host=127.0.0.1 port=5432 dbname=lane_target user=recovery_r password=<REDACTED>" -c "SELECT 1;"' 2>&1 | head -2
```
output (line 1918):
```text
psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  pg_hba.conf rejects connection for host "127.0.0.1", user "writer_owner", database "lane_target", no encryption

psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  pg_hba.conf rejects connection for host "127.0.0.1", user "recovery_r", database "lane_target", no encryption


Wall time: 0.90 seconds
```

## P3 — recovery_r over loopback TCP (rules 5–6)

source: lines 1916→1918

### recovery_r loopback TCP refused (second psql in shared command) — 2026-10-04T04:48:36.930Z
_(shared trace lines 1916→1918; shared with P2/P4)_
command (line 1916):
```sh
docker exec r2lab-pg2 sh -c 'psql "host=127.0.0.1 port=5432 dbname=lane_target user=writer_owner" -c "SELECT 1;"' 2>&1 | head -2; echo; docker exec r2lab-pg2 sh -c 'psql "host=127.0.0.1 port=5432 dbname=lane_target user=recovery_r password=<REDACTED>" -c "SELECT 1;"' 2>&1 | head -2
```
output (line 1918):
```text
psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  pg_hba.conf rejects connection for host "127.0.0.1", user "writer_owner", database "lane_target", no encryption

psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  pg_hba.conf rejects connection for host "127.0.0.1", user "recovery_r", database "lane_target", no encryption


Wall time: 0.90 seconds
```

## P4 — recovery_r password attempts (TCP) + client-side route validation

source: lines 1804→1806, 1807→1809, 1819→1821, 1916→1918

### unit test file created (write tool) — 2026-10-04T04:37:37.578Z
command (line 1804, tool `write`):
```text
path: internal/recovery/targetwriter_recoveryroute_linux_test.go

//go:build linux

package recovery

// targetwriter_recoveryroute_test.go (2026-10-04 implement round, T019 R2/R3
// closed acceptance): unit negatives for the protected peer-entry discipline —
// passwords, TCP/IP hosts, TLS params, identity overrides and the original
// writer role itself are refused BEFORE any guard, marker or child work; the
// wrong_lane role (equivalent grants, not the recovery identity) is the
// protected-entry zero-write negative companion (full integration evidence:
// docs/evidence/015/r2-grant-inventory/.

import (
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestValidateRecoveryRouteRefusals(t *testing.T) {
	trusted := controlstore.DSNTarget{
		Host: "/var/run/postgresql", Port: 5432, Database: "txharbor", Role: "writer_owner",
	}
	cases := []struct {
		name  string
		route string
		want  string
	}{
		{"empty", "", "recovery route is empty"},
		{"password_uri", "postgres://recovery_r:<REDACTED>@/txharbor", "not parseable"},
		{"keyword_with_password_field", "host=/var/run/postgresql port=5432 dbname=txharbor user=recovery_r password=<REDACTED>", "not parseable"},
		{"tcp_loopback_host", "host=127.0.0.1 port=5432 dbname=txharbor user=recovery_r", "socket directory"},
		{"ip_host", "host=10.0.0.1 port=5432 dbname=txharbor user=recovery_r", "socket directory"},
		{"wrong_database", "host=/var/run/postgresql port=5432 dbname=otherdb user=recovery_r", "does not match the trusted target"},
		{"original_writer_role", "host=/var/run/postgresql port=5432 dbname=txharbor user=writer_owner", "original writer role"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRecoveryRoute(tc.route, trusted)
			if err == nil {
				t.Fatalf("route %q was accepted", tc.route)
			}
			if !strings.Contains(err.Error(), tc.want) && !(tc.want == "not parseable" && strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("refusal %q does not match expected %q", err.Error(), tc.want)
			}
		})
	}
	t.Run("valid_peer_route_accepted", func(t *testing.T) {
		if err := validateRecoveryRoute("host=/var/run/postgresql port=5432 dbname=txharbor user=recovery_r", trusted); err != nil {
			t.Fatalf("valid peer route refused: %v", err)
		}
	})
}

```
output (line 1806):
```text
Successfully wrote 2295 bytes to internal/recovery/targetwriter_recoveryroute_linux_test.go
```

_(creation-time body: 2295 bytes, byte-identical to commit `27e533d`'s version of this file; the committed file was later extended with the T068 secrecy subtest `refusals_never_echo_route_material` by commit `147bea8`, so the current in-tree file is longer.)_

### unit test run 1 (pre-fix) — 2026-10-04T04:37:52.754Z
command (line 1807):
```sh
go test -count=1 -run 'TestValidateRecoveryRouteRefusals' ./internal/recovery/ 2>&1 | tail -4
```
output (line 1809):
```text
        targetwriter_recoveryroute_linux_test.go:44: refusal "recovery route must not negotiate TLS over the local socket path" does not match expected "socket directory"
FAIL
FAIL	github.com/xtianxx/txharbor/internal/recovery	0.007s
FAIL


Wall time: 4.90 seconds
```

### unit test run 2 (post-fix) — 2026-10-04T04:39:00.548Z
command (line 1819):
```sh
go test -count=1 -run 'TestValidateRecoveryRouteRefusals' ./internal/recovery/ 2>&1 | tail -6
```
output (line 1821):
```text
ok  	github.com/xtianxx/txharbor/internal/recovery	0.007s


Wall time: 4.41 seconds
```

### recovery_r TCP password attempt refused server-side (second psql in shared command) — 2026-10-04T04:48:36.930Z
_(shared trace lines 1916→1918; shared with P2/P3)_
command (line 1916):
```sh
docker exec r2lab-pg2 sh -c 'psql "host=127.0.0.1 port=5432 dbname=lane_target user=writer_owner" -c "SELECT 1;"' 2>&1 | head -2; echo; docker exec r2lab-pg2 sh -c 'psql "host=127.0.0.1 port=5432 dbname=lane_target user=recovery_r password=<REDACTED>" -c "SELECT 1;"' 2>&1 | head -2
```
output (line 1918):
```text
psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  pg_hba.conf rejects connection for host "127.0.0.1", user "writer_owner", database "lane_target", no encryption

psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  pg_hba.conf rejects connection for host "127.0.0.1", user "recovery_r", database "lane_target", no encryption


Wall time: 0.90 seconds
```

## P5 — wrong OS identity → recovery_r peer (ident map `pg_map postgres recovery_r`)

source: lines 1862→1864, 1865→1867, 1868→1870, 1871→1873, 1874→1876, 1877→1879, 1880→1882

### root OS identity -> recovery_r peer refused — 2026-10-04T04:44:04.997Z
command (line 1862):
```sh
docker exec r2lab-pg2 sh -c 'psql "host=/var/run/postgresql port=5432 dbname=lane_target user=recovery_r"' 2>&1 | head -2
```
output (line 1864):
```text
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  Peer authentication failed for user "recovery_r"


Wall time: 0.44 seconds
```

### container OS users (root/postgres) — 2026-10-04T04:44:24.027Z
command (line 1865):
```sh
docker exec r2lab-pg2 sh -c 'id; id postgres 2>/dev/null; ls /var/run/postgresql/ | head'
```
output (line 1867):
```text
uid=0(root) gid=0(root) groups=0(root)
uid=999(postgres) gid=999(postgres) groups=999(postgres),101(ssl-cert)


Wall time: 0.44 seconds
```

### postgres OS identity before ident map -> refused — 2026-10-04T04:44:36.353Z
command (line 1868):
```sh
docker exec -u postgres r2lab-pg2 sh -c 'psql "host=/var/run/postgresql port=5432 dbname=lane_target user=recovery_r" -c "SELECT current_user;"' 2>&1 | head -3
```
output (line 1870):
```text
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  Peer authentication failed for user "recovery_r"


Wall time: 0.49 seconds
```

### ident map absent (0 rows) — 2026-10-04T04:44:43.416Z
command (line 1871):
```sh
docker exec r2lab-pg2 sh -c 'psql -U postgres -d postgres -c "SELECT pg_ident_file_mappings();"' 2>&1 | head -6; docker exec r2lab-pg2 sh -c 'grep peer $(psql -U postgres -d postgres -t -A -c "SHOW hba_file")'
```
output (line 1873):
```text
 pg_ident_file_mappings 
------------------------
(0 rows)

local all recovery_r peer
# "gss", "sspi", "ident", "peer", "pam", "oauth", "ldap", "radius" or


Wall time: 0.93 seconds
```

### map via ALTER SYSTEM pg_ident_file (not effective, 0 rows) — 2026-10-04T04:45:06.391Z
command (line 1874):
```sh
docker exec r2lab-pg2 sh -c 'HBA=$(psql -U postgres -d postgres -t -A -c "SHOW hba_file"); printf "%s\n" "local all recovery_r peer map=pg_map" "$(grep -v "recovery_r" /tmp/hba.bak)" > /tmp/hba.new; cp /tmp/hba.new "$HBA"; printf "%s\n" "pg_map postgres recovery_r" > /tmp/pg_ident.conf; psql -U postgres -d postgres -c "ALTER SYSTEM SET pg_ident_file = '"'"'/tmp/pg_ident.conf'"'"';" -c "SELECT pg_reload_conf();"' 2>&1 | tail -2; docker exec r2lab-pg2 sh -c 'psql -U postgres -d postgres -t -c "SELECT count(*) FROM pg_ident_file_mappings;"'
```
output (line 1876):
```text
(1 row)

     0



Wall time: 0.89 seconds
```

### map via data_directory/pg_ident.conf (1 row) — 2026-10-04T04:45:23.537Z
command (line 1877):
```sh
docker exec r2lab-pg2 sh -c 'psql -U postgres -d postgres -c "SHOW data_directory;"; psql -U postgres -d postgres -c "ALTER SYSTEM RESET pg_ident_file"; HBA=$(psql -U postgres -d postgres -t -A -c "SHOW hba_file"); D=$(psql -U postgres -d postgres -t -A -c "SHOW data_directory"); printf "pg_map postgres recovery_r\n" > "$D/pg_ident.conf"; printf "%s\n" "local all recovery_r peer map=pg_map" "$(grep -v "recovery_r" /tmp/hba.bak)" > "$HBA"; psql -U postgres -d postgres -c "SELECT pg_reload_conf();"; psql -U postgres -d postgres -t -c "SELECT count(*) FROM pg_ident_file_mappings;"' 2>&1 | tail -4
```
output (line 1879):
```text
(1 row)

     1



Wall time: 0.72 seconds
```

### postgres OS identity after map -> peer accepted — 2026-10-04T04:45:33.323Z
command (line 1880):
```sh
docker exec -u postgres r2lab-pg2 sh -c 'psql "host=/var/run/postgresql port=5432 dbname=lane_target user=recovery_r" -c "SELECT current_user;"' 2>&1 | head -3
```
output (line 1882):
```text
 current_user 
--------------
 recovery_r


Wall time: 0.45 seconds
```

## P6 — wrong_lane_role restore on lane_neg (PROTECTED_NEG_lane) + lane_target companion

source: lines 1780→1782, 1783→1785, 1786→1788, 1789→1791, 1792→1794, 1828→1830, 1831→1833, 1837→1839, 1922→1924

### wrong_lane_role created — 2026-10-04T04:35:24.210Z
command (line 1780):
```sh
docker exec r2lab-pg2 psql -U postgres -d postgres -c "CREATE ROLE wrong_lane_role LOGIN PASSWORD '<REDACTED>';" 2>&1 | tail -1
```
output (line 1782):
```text
CREATE ROLE


Wall time: 0.94 seconds
```

### lane_neg setup + restore attempt 1 (err file path missing) — 2026-10-04T04:35:37.996Z
command (line 1783):
```sh
docker exec r2lab-pg2 psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS lane_neg;" -c "CREATE DATABASE lane_neg OWNER writer_owner;" >/dev/null 2>&1; docker exec r2lab-pg2 psql -U postgres -d lane_neg -c "GRANT CONNECT ON DATABASE lane_neg TO wrong_lane_role;" -c "GRANT USAGE, CREATE ON SCHEMA public TO wrong_lane_role;" >/dev/null 2>&1; docker exec r2lab-pg2 sh -c 'pg_restore --clean --if-exists --no-owner --no-privileges --dbname=postgres://wrong_lane_role:<REDACTED>@127.0.0.1:5432/lane_neg?sslmode=disable /tmp/datadb_full.dump 2>/tmp/r2lab/neg_lane.err >/dev/null; echo exit=$?; grep -c error /tmp/r2lab/neg_lane.err' && docker cp r2lab-pg2:/tmp/r2lab/neg_lane.err /tmp/r2lab/neg_lane.err && head -3 /tmp/r2lab/neg_lane.err
```
output (line 1785):
```text
exit=2
sh: 1: cannot create /tmp/r2lab/neg_lane.err: Directory nonexistent
grep: /tmp/r2lab/neg_lane.err: No such file or directory


Wall time: 1.76 seconds

Command exited with code 2
```

### lane_neg restore attempt 2 (exit=0) — 2026-10-04T04:35:58.799Z
command (line 1786):
```sh
docker exec r2lab-pg2 mkdir -p /tmp/r2lab && docker exec r2lab-pg2 sh -c 'pg_restore --clean --if-exists --no-owner --no-privileges --dbname=postgres://wrong_lane_role:<REDACTED>@127.0.0.1:5432/lane_neg?sslmode=disable /tmp/datadb_full.dump 2>/tmp/r2lab/neg_lane.err >/dev/null; echo exit=$?; grep -c error /tmp/r2lab/neg_lane.err' && docker exec r2lab-pg2 sh -c 'head -3 /tmp/r2lab/neg_lane.err'
```
output (line 1788):
```text
exit=0
0


Wall time: 1.67 seconds

Command exited with code 1
```

### ownership check after restore (69 rels, owner wrong_lane_role) — 2026-10-04T04:36:10.216Z
command (line 1789):
```sh
docker exec r2lab-pg2 psql -U postgres -d lane_neg -t -c "SELECT DISTINCT relkind, pg_get_userbyid(relowner) FROM pg_class c WHERE c.relname IN ('events_015','confirmation_policy_history','snapshot_probe_015','wallet_sequences_015');" 2>/dev/null | head -4; docker exec r2lab-pg2 psql -U postgres -d lane_neg -t -c "SELECT count(*) FROM pg_tables WHERE schemaname='public';"
```
output (line 1791):
```text
 r       | wrong_lane_role

    69



Wall time: 1.04 seconds
```

### lane_neg cleanup — 2026-10-04T04:36:22.780Z
command (line 1792):
```sh
docker exec r2lab-pg2 psql -U postgres -d lane_neg -c "DROP DATABASE lane_neg;" 2>/dev/null; docker exec r2lab-pg2 psql -U postgres -d postgres -c "DROP DATABASE lane_neg;" 2>&1 | tail -1; docker exec r2lab-pg2 psql -U postgres -d postgres -c "DROP ROLE wrong_lane_role;" 2>&1 | tail -1
```
output (line 1794):
```text
DROP DATABASE
DROP ROLE


Wall time: 1.78 seconds
```

### lane_target created + CONNECT grant attempt — 2026-10-04T04:39:49.174Z
command (line 1828):
```sh
docker exec r2lab-pg2 psql -U postgres -d postgres -c "CREATE DATABASE lane_target OWNER writer_owner;" 2>&1 | tail -1 && docker exec r2lab-pg2 psql -U postgres -d lane_target -c "GRANT CONNECT ON DATABASE lane_target TO wrong_lane_role;" 2>&1 | tail -1
```
output (line 1830):
```text
CREATE DATABASE
ERROR:  role "wrong_lane_role" does not exist


Wall time: 0.99 seconds
```

### wrong_lane_role created + CONNECT granted — 2026-10-04T04:40:22.986Z
command (line 1831):
```sh
docker exec r2lab-pg2 psql -U postgres -d postgres -c "CREATE ROLE wrong_lane_role LOGIN PASSWORD '<REDACTED>';" >/dev/null 2>&1; docker exec r2lab-pg2 psql -U postgres -d lane_target -c "GRANT CONNECT ON DATABASE lane_target TO wrong_lane_role;" >/dev/null 2>&1; echo lets_go
```
output (line 1833):
```text
lets_go


Wall time: 1.03 seconds
```

### context: wrong_lane_role local entry (before fence) — 2026-10-04T04:41:16.192Z
command (line 1837):
```sh
docker exec r2lab-pg2 sh -c 'psql "host=/var/run/postgresql port=5432 dbname=lane_target user=wrong_lane_role" -c "SELECT current_user, pg_backend_pid();"' 2>&1 | head -4
```
output (line 1839):
```text
  current_user   | pg_backend_pid 
-----------------+----------------
 wrong_lane_role |           1138
(1 row)


Wall time: 0.46 seconds
```

### lane_target cleanup — 2026-10-04T04:49:01.819Z
command (line 1922):
```sh
docker exec r2lab-pg2 psql -U postgres -d postgres -c "DROP DATABASE lane_target;" >/dev/null 2>&1; docker exec r2lab-pg2 psql -U postgres -d postgres -c "DROP ROLE wrong_lane_role;" >/dev/null 2>&1; echo cleaned_lane
```
output (line 1924):
```text
cleaned_lane


Wall time: 0.93 seconds
```

## Zero-object observation (protected-lane target)

source: lines 1919→1921

### zero-object check on lane_target — 2026-10-04T04:48:52.401Z
command (line 1919):
```sh
docker exec r2lab-pg2 psql -U postgres -d lane_target -t -c "SELECT count(*) FROM pg_tables WHERE schemaname='public';" -c "SELECT count(*) FROM public.confirmation_policy_history;" 2>&1 | head -3
```
output (line 1921):
```text
     0

ERROR:  relation "public.confirmation_policy_history" does not exist


Wall time: 0.48 seconds
```

## Aftermath

source: lines 2253→2255

### container removed after the round — 2026-10-04T05:17:21.085Z
command (line 2253):
```sh
docker rm -f r2lab-pg2 >/dev/null 2>&1; echo container_cleaned; git status --porcelain | wc -l
```
output (line 2255):
```text
container_cleaned
233


Wall time: 0.84 seconds
```
