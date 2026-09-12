// Package config loads and validates every runtime setting from environment
// variables. Configuration is env-only (spec clarification, data-model §1):
// there is no file support and no third-party config library.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/logx"
)

// Environment variable names. TXHARBOR_MUST be the only configuration carrier
// in 001 (FR-001).
const (
	EnvPGDSN              = "TXHARBOR_PG_DSN"
	EnvRPCURL             = "TXHARBOR_RPC_URL"
	EnvChainID            = "TXHARBOR_CHAIN_ID"
	EnvStartHeight        = "TXHARBOR_START_HEIGHT"
	EnvHTTPAddr           = "TXHARBOR_HTTP_ADDR"
	EnvStartupTimeout     = "TXHARBOR_STARTUP_TIMEOUT"
	EnvProbeInterval      = "TXHARBOR_PROBE_INTERVAL"
	EnvProbeTimeout       = "TXHARBOR_PROBE_TIMEOUT"
	EnvShutdownTimeout    = "TXHARBOR_SHUTDOWN_TIMEOUT"
	EnvMigrateLockTimeout = "TXHARBOR_MIGRATE_LOCK_TIMEOUT"
	EnvIndexRPCTimeout    = "TXHARBOR_INDEX_RPC_TIMEOUT"
	EnvIndexPollInterval  = "TXHARBOR_INDEX_POLL_INTERVAL"
	EnvIndexRetryInitial  = "TXHARBOR_INDEX_RETRY_INITIAL"
	EnvIndexRetryMax      = "TXHARBOR_INDEX_RETRY_MAX"
)

// Defaults from data-model §1. Acceptance runs use these values (FR-013).
const (
	DefaultHTTPAddr           = "127.0.0.1:8080"
	DefaultStartupTimeout     = 30 * time.Second
	DefaultProbeInterval      = 2 * time.Second
	DefaultProbeTimeout       = 5 * time.Second
	DefaultShutdownTimeout    = 15 * time.Second
	DefaultMigrateLockTimeout = 30 * time.Second
	DefaultIndexRPCTimeout    = 5 * time.Second
	DefaultIndexPollInterval  = 1 * time.Second
	DefaultIndexRetryInitial  = 200 * time.Millisecond
	DefaultIndexRetryMax      = 30 * time.Second

	// probeBudget is the hard ceiling for interval+timeout so that an
	// outage is observed/recovered well inside the 10s acceptance bound.
	probeBudget = 10 * time.Second
)

// Config is the validated application configuration.
type Config struct {
	PGDSN              string
	RPCURL             string
	ChainID            uint64
	StartHeight        uint64
	HTTPAddr           string
	StartupTimeout     time.Duration
	ProbeInterval      time.Duration
	ProbeTimeout       time.Duration
	ShutdownTimeout    time.Duration
	MigrateLockTimeout time.Duration
	IndexRPCTimeout    time.Duration
	IndexPollInterval  time.Duration
	IndexRetryInitial  time.Duration
	IndexRetryMax      time.Duration
}

// Getenv looks up an environment variable (os.LookupEnv compatible).
type Getenv func(string) (string, bool)

// Load reads, validates and defaults the configuration. It returns an error
// naming every offending variable so operators can fix all issues at once.
func Load(getenv Getenv) (*Config, error) {
	var errs []error
	c := &Config{
		HTTPAddr:           DefaultHTTPAddr,
		StartupTimeout:     DefaultStartupTimeout,
		ProbeInterval:      DefaultProbeInterval,
		ProbeTimeout:       DefaultProbeTimeout,
		ShutdownTimeout:    DefaultShutdownTimeout,
		MigrateLockTimeout: DefaultMigrateLockTimeout,
		IndexRPCTimeout:    DefaultIndexRPCTimeout,
		IndexPollInterval:  DefaultIndexPollInterval,
		IndexRetryInitial:  DefaultIndexRetryInitial,
		IndexRetryMax:      DefaultIndexRetryMax,
	}

	// Required values: presence first, then format.
	if dsn, err := require(getenv, EnvPGDSN); err != nil {
		errs = append(errs, err)
	} else if err := validateDSN(dsn); err != nil {
		errs = append(errs, invalid(EnvPGDSN, "%v", err))
	} else {
		c.PGDSN = dsn
	}

	if raw, err := require(getenv, EnvRPCURL); err != nil {
		errs = append(errs, err)
	} else if err := validateHTTPURL(raw); err != nil {
		errs = append(errs, invalid(EnvRPCURL, "%v", err))
	} else {
		c.RPCURL = raw
	}

	if raw, err := require(getenv, EnvChainID); err != nil {
		errs = append(errs, err)
	} else if id, err := parseChainID(raw); err != nil {
		errs = append(errs, invalid(EnvChainID, "%v", err))
	} else {
		c.ChainID = id
	}

	if raw, err := require(getenv, EnvStartHeight); err != nil {
		errs = append(errs, err)
	} else if h, err := parseStartHeight(raw); err != nil {
		errs = append(errs, invalid(EnvStartHeight, "%v", err))
	} else {
		c.StartHeight = h
	}

	if raw, ok := getenv(EnvHTTPAddr); ok && raw != "" {
		if err := validateHTTPAddr(raw); err != nil {
			errs = append(errs, invalid(EnvHTTPAddr, "%v", err))
		} else {
			c.HTTPAddr = raw
		}
	}

	c.StartupTimeout = duration(getenv, EnvStartupTimeout, c.StartupTimeout, &errs)
	c.ProbeInterval = duration(getenv, EnvProbeInterval, c.ProbeInterval, &errs)
	c.ProbeTimeout = duration(getenv, EnvProbeTimeout, c.ProbeTimeout, &errs)
	c.ShutdownTimeout = duration(getenv, EnvShutdownTimeout, c.ShutdownTimeout, &errs)
	c.MigrateLockTimeout = duration(getenv, EnvMigrateLockTimeout, c.MigrateLockTimeout, &errs)
	c.IndexRPCTimeout = duration(getenv, EnvIndexRPCTimeout, c.IndexRPCTimeout, &errs)
	c.IndexPollInterval = duration(getenv, EnvIndexPollInterval, c.IndexPollInterval, &errs)
	c.IndexRetryInitial = duration(getenv, EnvIndexRetryInitial, c.IndexRetryInitial, &errs)
	c.IndexRetryMax = duration(getenv, EnvIndexRetryMax, c.IndexRetryMax, &errs)

	// FR-013: interval+timeout must stay under the 10s perception bound.
	if c.ProbeInterval+c.ProbeTimeout >= probeBudget {
		errs = append(errs, invalid(EnvProbeInterval,
			"interval %s + probe timeout %s must be < %s (worst-case perception bound)",
			c.ProbeInterval, c.ProbeTimeout, probeBudget))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return c, nil
}

// Summary renders the effective configuration with credentials redacted, for
// one startup echo line (FR-003).
func (c *Config) Summary() string {
	return fmt.Sprintf(
		"pg=%s rpc=%s chain_id=%d start_height=%d http_addr=%s startup_timeout=%s probe_interval=%s probe_timeout=%s shutdown_timeout=%s migrate_lock_timeout=%s index_rpc_timeout=%s index_poll_interval=%s index_retry_initial=%s index_retry_max=%s",
		logx.Redact(c.PGDSN), logx.Redact(c.RPCURL), c.ChainID, c.StartHeight, c.HTTPAddr,
		c.StartupTimeout, c.ProbeInterval, c.ProbeTimeout, c.ShutdownTimeout, c.MigrateLockTimeout,
		c.IndexRPCTimeout, c.IndexPollInterval, c.IndexRetryInitial, c.IndexRetryMax,
	)
}

func require(getenv Getenv, name string) (string, error) {
	raw, ok := getenv(name)
	if !ok || raw == "" {
		return "", fmt.Errorf("missing required environment variable %s", name)
	}
	return raw, nil
}

func invalid(name, format string, args ...any) error {
	return fmt.Errorf("invalid %s: %s", name, fmt.Sprintf(format, args...))
}

func duration(getenv Getenv, name string, def time.Duration, errs *[]error) time.Duration {
	raw, ok := getenv(name)
	if !ok || raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, invalid(name, "%q is not a duration (want e.g. %s)", raw, def))
		return def
	}
	if d <= 0 {
		*errs = append(*errs, invalid(name, "%s must be > 0", d))
		return def
	}
	return d
}

// validateDSN accepts any DSN pgx itself accepts (URL or keyword/value form).
func validateDSN(dsn string) error {
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return fmt.Errorf("not a parseable Postgres connection string: %v", err)
	}
	return nil
}

func validateHTTPURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("not a parseable URL: %v", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme %q is not http or https", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("missing host")
	}
	return nil
}

// parseStartHeight accepts any uint64 including genesis 0; negative and
// non-decimal input is rejected.
func parseStartHeight(raw string) (uint64, error) {
	h, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a non-negative decimal integer", raw)
	}
	return h, nil
}

func parseChainID(raw string) (uint64, error) {
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a decimal integer", raw)
	}
	if id == 0 {
		return 0, errors.New("must be a positive decimal integer (> 0)")
	}
	return id, nil
}

func validateHTTPAddr(addr string) error {
	// An empty host is allowed (listen on all interfaces).
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("not in host:port form: %v", err)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 0 || p > 65535 {
		return fmt.Errorf("port %q is not in 0..65535", port)
	}
	return nil
}
