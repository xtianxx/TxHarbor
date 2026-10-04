package recovery

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPreparePGChildConnectionMovesKeywordPassword(t *testing.T) {
	dsn := `host=db.example port=5544 dbname=ledger user=backup password='pa:ss\\word' sslmode=verify-full sslrootcert='/path/to/root cert' application_name=backup-worker`
	safe, env, cleanup, err := PreparePGChildConnection(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if strings.Contains(safe, "pa:ss") {
		t.Fatalf("safe DSN contains password: %q", safe)
	}
	if !strings.Contains(safe, "sslmode='verify-full'") || !strings.Contains(safe, "sslrootcert='/path/to/root cert'") || !strings.Contains(safe, "application_name='backup-worker'") {
		t.Fatalf("connection options were not preserved: %q", safe)
	}
	if len(env) != 1 || !strings.HasPrefix(env[0], "PGPASSFILE=") {
		t.Fatalf("passfile env = %#v", env)
	}
	passfile := strings.TrimPrefix(env[0], "PGPASSFILE=")
	if !strings.HasPrefix(passfile, "/proc/self/fd/") {
		t.Fatalf("passfile is not descriptor-backed: %q", passfile)
	}
	data, err := os.ReadFile(passfile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `pa\:ss\\word`) {
		t.Fatalf("password missing/incorrectly escaped in passfile: %q", data)
	}
	cleanup()
	if _, err := os.Stat(passfile); err == nil {
		t.Fatalf("closed credential descriptor remains readable: %v", err)
	}
}

func TestPreparePGChildConnectionRemovesURIPassword(t *testing.T) {
	dsn := "postgresql://backup:p%40ss%3Aword@db.example:5544/ledger?sslmode=verify-full&sslrootcert=%2Fetc%2Froot.crt&application_name=backup-worker"
	safe, env, cleanup, err := PreparePGChildConnection(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if strings.Contains(safe, "p%40ss") || strings.Contains(safe, "p@ss") {
		t.Fatalf("safe URI contains password: %q", safe)
	}
	for _, option := range []string{"sslmode=verify-full", "sslrootcert=%2Fetc%2Froot.crt", "application_name=backup-worker"} {
		if !strings.Contains(safe, option) {
			t.Fatalf("URI option %q lost: %q", option, safe)
		}
	}
	if len(env) != 1 {
		t.Fatalf("env = %#v", env)
	}
}

func TestPreparePGChildConnectionPreservesParentTargetAndApplicationName(t *testing.T) {
	for _, dsn := range []string{
		`host=db.example port=5544 dbname=ledger user=backup password=secret application_name=backup-worker sslmode=verify-full`,
		`postgresql://backup:secret@db.example:5544/ledger?application_name=backup-worker&sslmode=verify-full`,
	} {
		parent, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		safe, _, cleanup, err := PreparePGChildConnection(dsn)
		if err != nil {
			t.Fatal(err)
		}
		child, parseErr := pgx.ParseConfig(safe)
		cleanup()
		if parseErr != nil {
			t.Fatalf("parse sanitized config: %v", parseErr)
		}
		if parent.Host != child.Host || parent.Port != child.Port || parent.Database != child.Database || parent.User != child.User {
			t.Fatalf("target identity changed: parent=%s:%d/%s@%s child=%s:%d/%s@%s", parent.Host, parent.Port, parent.Database, parent.User, child.Host, child.Port, child.Database, child.User)
		}
		if parent.RuntimeParams["application_name"] != child.RuntimeParams["application_name"] || child.RuntimeParams["application_name"] != "backup-worker" {
			t.Fatalf("application_name changed: parent=%q child=%q", parent.RuntimeParams["application_name"], child.RuntimeParams["application_name"])
		}
	}
}

func TestLocalPGCommandChildArgvIsCredentialFree(t *testing.T) {
	const password = "not-in-argv-very-secret"
	child := writePGChildScript(t, `#!/bin/sh
test -r "$PGPASSFILE" || exit 8
printf 'PASSFILE='
cat "$PGPASSFILE"
printf '\nARGV\n'
printf '%s\n' "$@"
printf 'PGHOST=%s PGPASSWORD=%s\n' "${PGHOST-unset}" "${PGPASSWORD-unset}"
`)
	var output bytes.Buffer
	err := (LocalPGCommand{}).Run(context.Background(), child,
		[]string{"--dbname=host=db port=5432 dbname=ledger user=backup password=" + password + " sslmode=require"}, nil, &output, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	got := output.String()
	argv := strings.SplitN(got, "\nARGV\n", 2)
	if len(argv) != 2 {
		t.Fatalf("unexpected child output: %q", got)
	}
	if strings.Contains(argv[1], password) {
		t.Fatalf("password appeared in child argv: %q", argv[1])
	}
	if !strings.Contains(argv[0], password) {
		t.Fatalf("password was not made available through passfile: %q", got)
	}
	if !strings.Contains(argv[1], "PGHOST=unset PGPASSWORD=unset") {
		t.Fatalf("inherited PG environment was not scrubbed: %q", got)
	}
}

func TestLocalPGCommandCleansPassfileWhenChildFailsAndDoesNotLeakError(t *testing.T) {
	const password = "failure-secret"
	child := writePGChildScript(t, "#!/bin/sh\nprintf '%s' \"$PGPASSFILE\"\nexit 7\n")
	var output bytes.Buffer
	// The child reports the temporary path before failing; it must be removed
	// even on a non-zero exit.
	err := (LocalPGCommand{}).Run(context.Background(), child,
		[]string{"--dbname=host=db port=5432 dbname=ledger user=backup password=" + password}, nil, &output, &bytes.Buffer{})
	if err == nil || strings.Contains(err.Error(), password) {
		t.Fatalf("unexpected/leaky error: %v", err)
	}
	path := output.String()
	if path == "" {
		t.Fatal("child did not receive a passfile")
	}
	if !strings.HasPrefix(path, "/proc/self/fd/") {
		t.Fatalf("child did not receive an anonymous descriptor path: %q", path)
	}
}

func writePGChildScript(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pg-child")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPreparePGChildConnectionFailsClosedOnUnsafeOptions(t *testing.T) {
	for _, dsn := range []string{
		`host=db port=5432 dbname=ledger user=backup password=secret passfile=/tmp/custom`,
		`postgres://user:secret@db:5432/ledger?passfile=/tmp/custom`,
		`host=db port=5432 dbname=ledger user=backup sslpassword=secret`,
		`postgres://user:secret@db:5432/ledger?sslpassword=secret`,
		`host=db port=5432 dbname=ledger user=backup service=production`,
		`postgres://user:secret@db:5432/ledger?service=production`,
		`host=db port=5432 dbname=ledger user=backup passfile=/tmp/custom`,
		`postgres://user:secret@db:5432/ledger?passfile=/tmp/custom`,
		`host=db port=5432 dbname='postgres://user:secret@db/ledger' user=backup password=secret`,
		`host=db password='secret`,
	} {
		_, _, cleanup, err := PreparePGChildConnection(dsn)
		cleanup()
		if err == nil {
			t.Fatal("accepted an unsupported credential-bearing DSN")
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error leaked credential: %v", err)
		}
	}
}

func TestPGChildEnvironmentRejectsAnyPGOverride(t *testing.T) {
	for _, env := range [][]string{
		{"PATH=/bin", "PGPASSWORD=secret"},
		{"PATH=/bin", "pgservicefile=/tmp/x"},
		{"PGHOST=db"},
	} {
		if err := validatePGChildEnvironment(env); err == nil {
			t.Fatalf("accepted ambient PostgreSQL environment %#v", env)
		}
	}
	if err := validatePGChildEnvironment([]string{"PATH=/bin", "OTHER=yes"}); err != nil {
		t.Fatalf("rejected non-PG environment: %v", err)
	}
	cmd := exec.Command("/bin/true")
	_, err := protectPGChildArgsWithEnvironment(cmd, "pg_dump", []string{"--dbname=host=db"}, []string{"PGHOST=other"})
	if err == nil {
		t.Fatal("LocalPGCommand argument preparation accepted an ambient PG override")
	}
}

func TestPGChildEnvironmentForwardsOnlyRuntimeAllowlist(t *testing.T) {
	environ := []string{
		"PATH=/usr/bin", "HOME=/home/operator", "TMPDIR=/tmp", "LANG=C.UTF-8", "LC_ALL=C",
		"SYSTEMROOT=C:\\Windows", "TXHARBOR_RECOVERY_TARGET_DSN=postgres://u:target-secret@db/target",
		"TXHARBOR_RECOVERY_CONTROL_DSN=postgres://u:control-secret@db/control",
		"TXHARBOR_PG_DSN=postgres://u:data-secret@db/data", "TXHARBOR_RPC_URL=https://rpc.invalid/key-secret",
		"TXHARBOR_SIGNER_PRIVATE_KEY=signer-secret", "VAULT_TOKEN=vault-secret", "TESTSECRET=custom-secret",
		"LD_PRELOAD=/tmp/injected.so", "BROKER_DSN=postgres://u:broker-secret@db/broker", "broken-entry",
	}
	got := pgChildEnvironment(environ)
	want := []string{"PATH=/usr/bin", "HOME=/home/operator", "TMPDIR=/tmp", "LANG=C.UTF-8", "LC_ALL=C", "SYSTEMROOT=C:\\Windows"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("child environment = %#v, want runtime allowlist %#v", got, want)
	}
	cmd := exec.Command("/bin/true")
	cleanup, err := protectPGChildArgsWithEnvironment(cmd, "pg_dump", []string{"--dbname=host=db port=5432 dbname=ledger user=backup password=secret"}, environ)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	for _, entry := range cmd.Env {
		if strings.HasPrefix(entry, "PGPASSFILE=") {
			continue
		}
		if strings.Contains(entry, "secret") || strings.HasPrefix(entry, "TESTSECRET=") || strings.HasPrefix(entry, "LD_PRELOAD=") {
			t.Fatalf("sensitive/unapproved environment forwarded: %q", entry)
		}
	}
}

func TestProtectPGChildArgsRejectsAlternateAndDuplicateDatabaseArguments(t *testing.T) {
	for _, args := range [][]string{
		{"--dbname", "host=db"},
		{"-d", "host=db"},
		{"-dhost=db"},
		{"--dbname=host=db", "--dbname=host=other"},
		{"--dbname=host=db", "-dhost=other"},
		{"--dbname=host=db", "--host=other"},
		{"--dbname=host=db", "-p5433"},
		{"host=db"}, // positional pg_dump database name
	} {
		cmd := exec.Command("/bin/true")
		_, err := protectPGChildArgs(cmd, "pg_dump", args)
		if err == nil {
			t.Fatalf("accepted alternative/duplicate target args %#v", args)
		}
	}
}
