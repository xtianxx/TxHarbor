package recovery

// PostgreSQL client child-process connection preparation. Never place a DSN
// password in argv: process listings and diagnostic tooling commonly expose
// argv. The password is instead written to a private, invocation-scoped
// libpq passfile. Normal return/error paths remove the private directory; an
// uncatchable SIGKILL or machine crash can leave it behind in the OS temp
// directory (whose child directory is mode 0700). This helper is exported within the internal package for
// later supervised-command reuse. It supports only a documented, explicit
// target and option allowlist; a target must explicitly specify host/hostaddr,
// port, database, and user. Allowed optional settings are application_name,
// SSL certificate/mode/protocol settings, gssencmode, channel_binding,
// connect_timeout, client_encoding, target_session_attrs, load_balance_hosts,
// keepalive settings, and tcp_user_timeout. Service/passfile indirection,
// ambient PG* configuration, unknown options, and options with secret payloads
// (including sslpassword) are refused because they could change target
// identity or leak credentials.

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
)

// PreparePGChildConnection converts one libpq DSN to a credential-free DSN,
// plus private environment entries and a cleanup function. URI and keyword
// conninfo syntax are preserved for the supported option allowlist; malformed
// and unsupported forms fail closed. The caller must reject ambient PG*
// environment and use the returned entries only after that check. When the
// DSN has no embedded password, env is empty and cleanup is a no-op.
func PreparePGChildConnection(dsn string) (safeDSN string, env []string, cleanup func(), err error) {
	cleanup = func() {}
	if strings.HasPrefix(strings.ToLower(dsn), "postgres://") || strings.HasPrefix(strings.ToLower(dsn), "postgresql://") {
		return preparePGURI(dsn)
	}
	return preparePGKeywordDSN(dsn)
}

// validatePGChildEnvironment refuses all inherited libpq variables rather
// than silently removing settings which may have selected a different server
// for the parent connection. The private PGPASSFILE is added only after this
// check, and only when the DSN itself contains a password.
func validatePGChildEnvironment(environ []string) error {
	for _, entry := range environ {
		key, _, ok := strings.Cut(entry, "=")
		if ok && len(key) >= 2 && strings.EqualFold(key[:2], "PG") {
			return fmt.Errorf("ambient PostgreSQL environment is unsupported")
		}
	}
	return nil
}

func pgChildEnvironment(environ []string) []string {
	filtered := make([]string, 0, len(environ))
	for _, entry := range environ {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || len(key) < 2 || !strings.EqualFold(key[:2], "PG") {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func protectPGChildArgs(cmd *exec.Cmd, name string, args []string) (func(), error) {
	return protectPGChildArgsWithEnvironment(cmd, name, args, os.Environ())
}

func protectPGChildArgsWithEnvironment(cmd *exec.Cmd, name string, args, environ []string) (func(), error) {
	if err := validatePGChildEnvironment(environ); err != nil {
		return func() {}, err
	}
	cmd.Env = pgChildEnvironment(environ)
	args = append([]string(nil), args...)
	dbIndex, dbCount := -1, 0
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "--dbname=") {
			dbCount++
			dbIndex = i
		} else if args[i] == "--dbname" || args[i] == "--d" || strings.HasPrefix(args[i], "--d=") || strings.HasPrefix(args[i], "--db") || args[i] == "-d" || strings.HasPrefix(args[i], "-d") && len(args[i]) > 2 {
			return func() {}, fmt.Errorf("unsupported database option form")
		} else if isPGTargetOverrideArg(args[i]) {
			return func() {}, fmt.Errorf("connection target override outside --dbname is unsupported")
		} else if !strings.HasPrefix(args[i], "-") {
			// Every currently supported invocation uses option=value or flags.
			// Bare operands can be a positional pg_dump dbname or pg_restore
			// alternate target and therefore are never forwarded.
			return func() {}, fmt.Errorf("positional PostgreSQL arguments are unsupported")
		}
	}
	if dbCount > 1 {
		return func() {}, fmt.Errorf("multiple database options are unsupported")
	}
	if dbCount == 0 {
		if name == "pg_dump" && (len(args) == 1 && (args[0] == "--version" || args[0] == "--help")) || name == "pg_restore" && len(args) == 1 && args[0] == "--list" {
			return func() {}, nil
		}
		return func() {}, fmt.Errorf("PostgreSQL command requires one canonical --dbname= option")
	}
	dsn := strings.TrimPrefix(args[dbIndex], "--dbname=")
	if dsn == "" {
		return func() {}, fmt.Errorf("database option has no value")
	}
	safe, env, cleanup, err := PreparePGChildConnection(dsn)
	if err != nil {
		return func() {}, err
	}
	args[dbIndex] = "--dbname=" + safe
	cmd.Env = append(cmd.Env, env...)
	cmd.Args = append([]string{cmd.Args[0]}, args...)
	return cleanup, nil
}

func isPGTargetOverrideArg(arg string) bool {
	for _, long := range []string{"--host", "--port", "--username", "--maintenance-db"} {
		if arg == long || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return arg == "-h" || arg == "-p" || arg == "-U" || strings.HasPrefix(arg, "-h") && len(arg) > 2 || strings.HasPrefix(arg, "-p") && len(arg) > 2 || strings.HasPrefix(arg, "-U") && len(arg) > 2
}

func preparePGURI(dsn string) (string, []string, func(), error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" || u.Opaque != "" || u.Fragment != "" {
		return "", nil, func() {}, fmt.Errorf("unsupported PostgreSQL URI")
	}
	if u.Hostname() == "" || u.Port() == "" || u.User == nil || u.User.Username() == "" || strings.Trim(u.Path, "/") == "" {
		return "", nil, func() {}, fmt.Errorf("PostgreSQL URI requires explicit host, port, database, and user")
	}
	if strings.ContainsAny(strings.Trim(u.Path, "/"), " =\t\r\n") {
		return "", nil, func() {}, fmt.Errorf("unsupported PostgreSQL database name")
	}
	password, hasPassword := "", false
	if u.User != nil {
		password, hasPassword = u.User.Password()
		if hasPassword {
			u.User = url.User(u.User.Username())
		}
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("unsupported PostgreSQL URI query")
	}
	if values, exists := query["password"]; exists {
		if len(values) != 1 || hasPassword {
			return "", nil, func() {}, fmt.Errorf("ambiguous password in PostgreSQL URI")
		}
		password, hasPassword = values[0], true
		delete(query, "password")
	}
	for key, values := range query {
		if key == "service" || key == "passfile" || key == "sslpassword" || !supportedPGOption(key) {
			return "", nil, func() {}, fmt.Errorf("unsupported PostgreSQL URI option")
		}
		if len(values) != 1 {
			return "", nil, func() {}, fmt.Errorf("duplicate PostgreSQL URI option")
		}
		if key == "host" || key == "hostaddr" || key == "port" || key == "dbname" || key == "user" {
			return "", nil, func() {}, fmt.Errorf("duplicate PostgreSQL target setting")
		}
	}
	if !hasPassword {
		return u.String(), nil, func() {}, nil
	}
	if _, explicit := query["passfile"]; explicit {
		return "", nil, func() {}, fmt.Errorf("password with explicit passfile is unsupported")
	}
	if err := validatePassfilePassword(password); err != nil {
		return "", nil, func() {}, err
	}
	u.RawQuery = query.Encode()
	return writePrivatePGPassfile(u.String(), password)
}

type pgConnOption struct{ key, value string }

// supportedPGOption is the complete non-secret libpq option allowlist for a
// child dump/restore command. Notably omitted: service, passfile, sslpassword,
// options (which can carry arbitrary settings), and unknown future options.
// Additions must be checked for credential-bearing values before inclusion.
func supportedPGOption(key string) bool {
	switch key {
	case "host", "hostaddr", "port", "dbname", "user", "password",
		"application_name", "sslmode", "sslcompression", "sslcert", "sslkey",
		"sslrootcert", "sslcrl", "sslcrldir", "ssl_min_protocol_version",
		"ssl_max_protocol_version", "gssencmode", "channel_binding",
		"connect_timeout", "client_encoding", "target_session_attrs",
		"load_balance_hosts", "keepalives", "keepalives_idle",
		"keepalives_interval", "keepalives_count", "tcp_user_timeout":
		return true
	default:
		return false
	}
}

func preparePGKeywordDSN(dsn string) (string, []string, func(), error) {
	options, err := parsePGKeywordDSN(dsn)
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("unsupported PostgreSQL keyword DSN")
	}
	password, hasPassword := "", false
	filtered := make([]pgConnOption, 0, len(options))
	seen := make(map[string]bool, len(options))
	identity := make(map[string]string, len(options))
	for _, option := range options {
		if seen[option.key] {
			return "", nil, func() {}, fmt.Errorf("duplicate PostgreSQL connection option")
		}
		seen[option.key] = true
		switch option.key {
		case "password":
			password, hasPassword = option.value, true
		default:
			if option.key == "service" || option.key == "passfile" || option.key == "sslpassword" || !supportedPGOption(option.key) {
				return "", nil, func() {}, fmt.Errorf("unsupported PostgreSQL connection option")
			}
			if option.key == "host" || option.key == "hostaddr" || option.key == "port" || option.key == "dbname" || option.key == "user" {
				identity[option.key] = option.value
			}
			filtered = append(filtered, option)
		}
	}
	if identity["host"] == "" && identity["hostaddr"] == "" || identity["port"] == "" || identity["dbname"] == "" || identity["user"] == "" {
		return "", nil, func() {}, fmt.Errorf("keyword DSN requires explicit host, port, database, and user")
	}
	if strings.ContainsAny(identity["dbname"], " =?\t\r\n") || strings.Contains(identity["dbname"], "://") {
		return "", nil, func() {}, fmt.Errorf("unsupported PostgreSQL database name")
	}
	if hasPassword {
		if err := validatePassfilePassword(password); err != nil {
			return "", nil, func() {}, err
		}
	}
	safe := formatPGKeywordDSN(filtered)
	if !hasPassword {
		return safe, nil, func() {}, nil
	}
	return writePrivatePGPassfile(safe, password)
}

func parsePGKeywordDSN(s string) ([]pgConnOption, error) {
	var out []pgConnOption
	for i := 0; ; {
		for i < len(s) && isPGSpace(s[i]) {
			i++
		}
		if i == len(s) {
			return out, nil
		}
		start := i
		for i < len(s) && s[i] != '=' && !isPGSpace(s[i]) {
			i++
		}
		if start == i || i == len(s) || s[i] != '=' {
			return nil, fmt.Errorf("invalid option")
		}
		key := s[start:i]
		i++
		var value strings.Builder
		if i < len(s) && s[i] == '\'' {
			i++
			closed := false
			for i < len(s) {
				if s[i] == '\\' {
					i++
					if i == len(s) {
						return nil, fmt.Errorf("invalid escape")
					}
					value.WriteByte(s[i])
					i++
					continue
				}
				if s[i] == '\'' {
					i++
					closed = true
					break
				}
				value.WriteByte(s[i])
				i++
			}
			if !closed || i < len(s) && !isPGSpace(s[i]) {
				return nil, fmt.Errorf("invalid quoted value")
			}
		} else {
			for i < len(s) && !isPGSpace(s[i]) {
				if s[i] == '\\' {
					i++
					if i == len(s) {
						return nil, fmt.Errorf("invalid escape")
					}
				}
				value.WriteByte(s[i])
				i++
			}
		}
		out = append(out, pgConnOption{key, value.String()})
	}
}

func formatPGKeywordDSN(options []pgConnOption) string {
	var out strings.Builder
	for i, option := range options {
		if i > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(option.key)
		out.WriteString("='")
		for _, r := range option.value {
			if r == '\\' || r == '\'' {
				out.WriteByte('\\')
			}
			out.WriteRune(r)
		}
		out.WriteByte('\'')
	}
	return out.String()
}

func isPGSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

func validatePassfilePassword(password string) error {
	if strings.ContainsAny(password, "\r\n") {
		return fmt.Errorf("password cannot be represented safely in a passfile")
	}
	return nil
}

func writePrivatePGPassfile(safeDSN, password string) (string, []string, func(), error) {
	dir, err := os.MkdirTemp("", "txharbor-pg-*")
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("create private PostgreSQL credential directory")
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	path := dir + string(os.PathSeparator) + "pgpass"
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		cleanup()
		return "", nil, func() {}, fmt.Errorf("create private PostgreSQL passfile")
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		cleanup()
		return "", nil, func() {}, fmt.Errorf("protect private PostgreSQL passfile")
	}
	escape := func(s string) string {
		s = strings.ReplaceAll(s, `\`, `\\`)
		return strings.ReplaceAll(s, ":", `\:`)
	}
	_, writeErr := fmt.Fprintf(f, "*:*:*:*:%s\n", escape(password))
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		cleanup()
		return "", nil, func() {}, fmt.Errorf("write private PostgreSQL passfile")
	}
	return safeDSN, []string{"PGPASSFILE=" + path}, cleanup, nil
}
