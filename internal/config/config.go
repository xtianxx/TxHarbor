// Package config loads and validates every runtime setting from environment
// variables. Configuration is env-only (spec clarification, data-model §1):
// there is no file support and no third-party config library.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/signer"

	"github.com/xtianxx/txharbor/internal/logx"
)

// Environment variable names. TXHARBOR_MUST be the only configuration carrier
// in 001 (FR-001).
const (
	EnvPGDSN                 = "TXHARBOR_PG_DSN"
	EnvRPCURL                = "TXHARBOR_RPC_URL"
	EnvChainID               = "TXHARBOR_CHAIN_ID"
	EnvStartHeight           = "TXHARBOR_START_HEIGHT"
	EnvHTTPAddr              = "TXHARBOR_HTTP_ADDR"
	EnvStartupTimeout        = "TXHARBOR_STARTUP_TIMEOUT"
	EnvProbeInterval         = "TXHARBOR_PROBE_INTERVAL"
	EnvProbeTimeout          = "TXHARBOR_PROBE_TIMEOUT"
	EnvShutdownTimeout       = "TXHARBOR_SHUTDOWN_TIMEOUT"
	EnvMigrateLockTimeout    = "TXHARBOR_MIGRATE_LOCK_TIMEOUT"
	EnvIndexRPCTimeout       = "TXHARBOR_INDEX_RPC_TIMEOUT"
	EnvIndexPollInterval     = "TXHARBOR_INDEX_POLL_INTERVAL"
	EnvIndexRetryInitial     = "TXHARBOR_INDEX_RETRY_INITIAL"
	EnvIndexRetryMax         = "TXHARBOR_INDEX_RETRY_MAX"
	EnvLogStartHeight        = "TXHARBOR_LOG_START_HEIGHT"
	EnvLogContracts          = "TXHARBOR_LOG_CONTRACTS"
	EnvLogBatchBlocks        = "TXHARBOR_LOG_BATCH_BLOCKS"
	EnvDepositStartHeight    = "TXHARBOR_DEPOSIT_START_HEIGHT"
	EnvDepositContracts      = "TXHARBOR_DEPOSIT_CONTRACTS"
	EnvDepositWatchAddresses = "TXHARBOR_DEPOSIT_WATCH_ADDRESSES"
	EnvDepositBatchBlocks    = "TXHARBOR_DEPOSIT_BATCH_BLOCKS"
	EnvConfirmationDepth     = "TXHARBOR_CONFIRMATION_DEPTH"
	// Reorg recovery (006 FR-03/Q1): required raw max-depth string (no
	// default; parsed + refused at startup via ParseReorgMaxDepth) plus the
	// optional replay-batch cap. Poll/retry timing reuses the INDEX knobs
	// (research R5 timing table — no new knob names).
	EnvReorgMaxDepth    = "TXHARBOR_REORG_MAX_DEPTH"
	EnvReorgReplayBatch = "TXHARBOR_REORG_REPLAY_BATCH"
	// Nonce read API (008 FR-19/FR-21): the bearer credential for the read
	// endpoints mounted by the serve carrier. Reconcile/observation timing
	// reuses the INDEX knobs above — no 008 timing knob exists.
	EnvNonceReadToken = "TXHARBOR_NONCE_READ_TOKEN"
	// Signer service (009 T013): listener, backend mode, key file, signing
	// deadline, and the policy allowlists/caps. Parsed when present;
	// required-ness is enforced by SignerPolicyConfig for the signer paths
	// only, so the shared Load stays green for serve/migrate flows.
	EnvSignerHTTPAddr       = "TXHARBOR_SIGNER_HTTP_ADDR"
	EnvSignerMode           = "TXHARBOR_SIGNER_MODE"
	EnvSignerKeyFile        = "TXHARBOR_SIGNER_KEY_FILE"
	EnvSignerKeyTimeout     = "TXHARBOR_SIGNER_KEY_TIMEOUT"
	EnvSignerChains         = "TXHARBOR_SIGNER_CHAINS"
	EnvSignerSenders        = "TXHARBOR_SIGNER_SENDERS"
	EnvSignerAssets         = "TXHARBOR_SIGNER_ASSETS"
	EnvSignerRecipients     = "TXHARBOR_SIGNER_RECIPIENTS"
	EnvSignerMaxAmount      = "TXHARBOR_SIGNER_MAX_AMOUNT"
	EnvSignerMaxGasLimit    = "TXHARBOR_SIGNER_MAX_GAS_LIMIT"
	EnvSignerMaxFeePerGas   = "TXHARBOR_SIGNER_MAX_FEE_PER_GAS"
	EnvSignerMaxPriorityFee = "TXHARBOR_SIGNER_MAX_PRIORITY_FEE_PER_GAS"
	EnvSignerMaxGasPrice    = "TXHARBOR_SIGNER_MAX_GAS_PRICE"
	// 010 transaction-lifecycle caller knobs (T001/R-010-12): the 009 base URL,
	// the bearer credential (secret, never logged), and the bounded dispatch
	// timeout. No other timing knob exists — reconcile cadence and RPC timeout
	// reuse TXHARBOR_INDEX_POLL_INTERVAL / TXHARBOR_INDEX_RPC_TIMEOUT.
	EnvTxSignerURL        = "TXHARBOR_TX_SIGNER_URL"
	EnvTxSignerCredential = "TXHARBOR_TX_SIGNER_CREDENTIAL"
	EnvTxSendTimeout      = "TXHARBOR_TX_SEND_TIMEOUT"
	// 011 withdrawal execution worker (T001). Cadence/backoff are technical
	// values; lease TTL, heartbeat and stall window are the approved initial
	// configuration (2026-09-17, research R14): heartbeat < TTL < stall
	// enforced fail-closed at Load. The stall window is an independent value,
	// never derived from the TTL.
	EnvWorkerTTLSeconds       = "TXHARBOR_WORKER_TTL_SECONDS"
	EnvWorkerHeartbeatSeconds = "TXHARBOR_WORKER_HEARTBEAT_SECONDS"
	EnvWorkerStallSeconds     = "TXHARBOR_WORKER_STALL_SECONDS"
	EnvWorkerBackoffBaseMS    = "TXHARBOR_WORKER_BACKOFF_BASE_MS"
	EnvWorkerBackoffMaxMS     = "TXHARBOR_WORKER_BACKOFF_MAX_MS"
	EnvWorkerScanIntervalMS   = "TXHARBOR_WORKER_SCAN_INTERVAL_MS"
	EnvWorkerLabel            = "TXHARBOR_WORKER_LABEL"
)
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

	// DefaultLogBatchBlocks bounds one log scan interval (research R6: Anvil
	// local sensitivity vs round-trip cost; production values are re-checked
	// against the provider annex without changing history semantics).
	DefaultLogBatchBlocks = uint64(500)

	// DefaultDepositBatchBlocks bounds one deposit scan interval (research
	// R7: same round-trip trade-off as the 003 log stream; tuning it never
	// changes history semantics and it is not part of the config identity).
	DefaultDepositBatchBlocks = uint64(500)

	// DefaultReorgReplayBatch caps one recovery replay range (research R5
	// timing table: one frontier-advance txn per stream range). It mirrors
	// the executor's internal default so an unset knob behaves identically
	// to a directly constructed executor.
	DefaultReorgReplayBatch = uint64(500)

	// Signer defaults (009): loopback-only listener, fail-closed production
	// mode (a local test key is never constructed in production), and the
	// 5s signing deadline (research R2).
	DefaultSignerHTTPAddr   = "127.0.0.1:8091"
	DefaultSignerMode       = "production"
	DefaultSignerKeyTimeout = 5 * time.Second

	// 010 dispatch timeout (T001): the bounded raw-transaction broadcast window.
	// 15s is the frozen default; zero/negative is refused at Load (fail-closed).
	DefaultTxSendTimeout = 15 * time.Second
	// 011 worker defaults (T001; approved initial configuration 2026-09-17,
	// research R14). Heartbeat is TTL/3 with ±10% jitter applied at runtime;
	// the stall window is independent of the TTL. Backoff and scan cadence are
	// technical values, never business limits.
	DefaultWorkerTTL          = 30 * time.Second
	DefaultWorkerHeartbeat    = 10 * time.Second
	DefaultWorkerStall        = 300 * time.Second
	DefaultWorkerBackoffBase  = 1 * time.Second
	DefaultWorkerBackoffMax   = 30 * time.Second
	DefaultWorkerScanInterval = 1 * time.Second

	// logConfigVersion prefixes the config identity encoding (clarification
	// A1). The version is part of the hashed input so future encodings never
	// collide with this one.
	logConfigVersion = "erc20-transfer:v1"

	// depositConfigVersion is the domain separator of the deposit config
	// identity (research R3); the following newline is part of the encoding.
	depositConfigVersion = "deposit:v1"

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
	// Log scanning (003-event-indexing): independent start bound, normalized
	// whitelist, its config identity, and the per-interval block cap.
	LogStartHeight uint64
	LogContracts   []string
	LogConfigHash  string
	LogBatchBlocks uint64
	// Deposit detection (004): receive-side start bound, normalized
	// contract/watch entries with effective heights, its versioned config
	// identity, and the per-interval block cap. Batch/timeout knobs are
	// deliberately excluded from the identity (research R3/R7).
	DepositStartHeight    uint64
	DepositContracts      []DepositEntry
	DepositWatchAddresses []DepositEntry
	DepositConfigHash     string
	DepositBatchBlocks    uint64
	// Confirmation tracking (005 FR-03/Q1): required positive threshold N
	// in [1, MaxInt64] (BIGINT system range, no business cap, no default).
	ConfirmationDepth uint64
	// Reorg recovery (006 FR-03/Q1): raw max-depth string, carried through
	// unparsed — startup parses it via ParseReorgMaxDepth and refuses via
	// the serve fail() path, so a missing/zero/negative/non-integer/
	// out-of-representation value never reaches the executor.
	ReorgMaxDepthRaw string
	// Replay batch cap for the recovery executor (research R5 timing
	// table); poll/retry timing reuses IndexPollInterval/IndexRetryInitial/
	// IndexRetryMax, so no new timing knobs exist.
	ReorgReplayBatch uint64
	// Nonce read API bearer token (008 FR-19). Optional at Load; while unset
	// the read endpoints stay fail-closed. Never echoed raw: Summary() renders
	// presence as the redaction placeholder only.
	NonceReadToken string
	// Signer service (009 T013): parsed when present; SignerPolicyConfig
	// enforces required-ness for the signer paths.
	SignerHTTPAddr       string
	SignerMode           string
	SignerKeyFile        string
	SignerKeyTimeout     time.Duration
	SignerChains         []int64
	SignerSenders        []string
	SignerAssets         []string
	SignerRecipients     []string
	SignerMaxAmount      *big.Int
	SignerMaxGasLimit    uint64
	SignerMaxFeePerGas   *big.Int
	SignerMaxPriorityFee *big.Int
	SignerMaxGasPrice    *big.Int
	// 010 transaction-lifecycle caller knobs (T001). SignerURL/Credential are
	// parsed when present; required-ness is enforced fail-closed at client
	// construction so serve/migrate stay green without them.
	TxSignerURL        string
	TxSignerCredential string
	TxSendTimeout      time.Duration
	// 011 withdrawal execution worker (T001). All validity/expiry decisions
	// use the DB clock; these durations never become an application-clock
	// expiry. WorkerLabel is evidence-only free text.
	WorkerTTL          time.Duration
	WorkerHeartbeat    time.Duration
	WorkerStall        time.Duration
	WorkerBackoffBase  time.Duration
	WorkerBackoffMax   time.Duration
	WorkerScanInterval time.Duration
	WorkerLabel        string
}

// DepositEntry is one normalized `address[:effective]` configuration item: a
// lowercase 0x-prefixed 20-byte EVM address plus the first block at which the
// asset or watched address is eligible (explicit suffix, or the global
// deposit start height when omitted; FR-04/FR-05). The same address with
// different effective heights denotes distinct entries and is never merged
// (research R3).
type DepositEntry struct {
	Address   string
	Effective uint64
}

// String renders the canonical history-snapshot line `address:effective`
// (data-model Table 4: assets/watches snapshot, lexicographic, no trailing
// newline).
func (e DepositEntry) String() string {
	return e.Address + ":" + strconv.FormatUint(e.Effective, 10)
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

	if raw, err := require(getenv, EnvLogStartHeight); err != nil {
		errs = append(errs, err)
	} else if h, err := parseStartHeight(raw); err != nil {
		errs = append(errs, invalid(EnvLogStartHeight, "%v", err))
	} else {
		c.LogStartHeight = h
	}

	if raw, err := require(getenv, EnvLogContracts); err != nil {
		errs = append(errs, err)
	} else if contracts, hash, err := NormalizeWhitelist(raw); err != nil {
		errs = append(errs, invalid(EnvLogContracts, "%v", err))
	} else {
		c.LogContracts = contracts
		c.LogConfigHash = hash
	}

	c.LogBatchBlocks = DefaultLogBatchBlocks
	if raw, ok := getenv(EnvLogBatchBlocks); ok && raw != "" {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || n == 0 {
			errs = append(errs, invalid(EnvLogBatchBlocks, "%q is not a positive decimal integer", raw))
		} else {
			c.LogBatchBlocks = n
		}
	}

	// Deposit detection (004): the start bound and both collections are
	// required — a blank collection is a configuration error and never
	// degrades into all-address monitoring (FR-04, research R7). The
	// collection entries default their effective height to the global start.
	if raw, err := require(getenv, EnvDepositStartHeight); err != nil {
		errs = append(errs, err)
	} else if h, err := parseStartHeight(raw); err != nil {
		errs = append(errs, invalid(EnvDepositStartHeight, "%v", err))
	} else {
		c.DepositStartHeight = h
	}

	var contracts, watches []DepositEntry
	if raw, err := require(getenv, EnvDepositContracts); err != nil {
		errs = append(errs, err)
	} else if entries, err := parseDepositEntries(raw, c.DepositStartHeight); err != nil {
		errs = append(errs, invalid(EnvDepositContracts, "%v", err))
	} else {
		contracts = entries
	}
	if raw, err := require(getenv, EnvDepositWatchAddresses); err != nil {
		errs = append(errs, err)
	} else if entries, err := parseDepositEntries(raw, c.DepositStartHeight); err != nil {
		errs = append(errs, invalid(EnvDepositWatchAddresses, "%v", err))
	} else {
		watches = entries
	}
	if contracts != nil && watches != nil {
		c.DepositContracts = contracts
		c.DepositWatchAddresses = watches
		c.DepositConfigHash = depositIdentity(c.DepositStartHeight, contracts, watches)
	}

	c.DepositBatchBlocks = DefaultDepositBatchBlocks
	if raw, ok := getenv(EnvDepositBatchBlocks); ok && raw != "" {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || n == 0 {
			errs = append(errs, invalid(EnvDepositBatchBlocks, "%q is not a positive decimal integer", raw))
		} else {
			c.DepositBatchBlocks = n
		}
	}

	// Confirmation tracking (005 FR-03/Q1): required positive threshold,
	// no default; missing/non-integer/<1/out-of-system-range refuses startup.
	if raw, err := require(getenv, EnvConfirmationDepth); err != nil {
		errs = append(errs, err)
	} else if n, err := parseConfirmationDepth(raw); err != nil {
		errs = append(errs, invalid(EnvConfirmationDepth, "%v", err))
	} else {
		c.ConfirmationDepth = n
	}

	// Reorg recovery (006): the raw max-depth string passes through
	// unvalidated here — startup (serve.go) parses it and refuses via the
	// fail() path, keeping one refusal site next to the executor that
	// binds it. Only the replay-batch cap validates at Load, like the
	// other batch knobs above.
	if raw, ok := getenv(EnvReorgMaxDepth); ok {
		c.ReorgMaxDepthRaw = raw
	}

	c.ReorgReplayBatch = DefaultReorgReplayBatch
	if raw, ok := getenv(EnvReorgReplayBatch); ok && raw != "" {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || n == 0 {
			errs = append(errs, invalid(EnvReorgReplayBatch, "%q is not a positive decimal integer", raw))
		} else {
			c.ReorgReplayBatch = n
		}
	}

	c.loadSigner(getenv, &errs)
	c.loadTxLifecycle(getenv, &errs)
	c.loadWorker(getenv, &errs)
	// Nonce read API (008 FR-19): the bearer token passes through verbatim
	// and is never formatted into an error. Unset or empty leaves the read
	// endpoints fail-closed (the read provider authenticates against it).
	if raw, ok := getenv(EnvNonceReadToken); ok && raw != "" {
		c.NonceReadToken = raw
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
// one startup echo line (FR-003). The nonce read token is represented by
// presence + the redaction placeholder, never by its value.
func (c *Config) Summary() string {
	nonceReadToken := ""
	if c.NonceReadToken != "" {
		nonceReadToken = logx.Redacted
	}
	txSignerCredential := ""
	if c.TxSignerCredential != "" {
		txSignerCredential = logx.Redacted
	}
	return fmt.Sprintf(
		"pg=%s rpc=%s chain_id=%d start_height=%d http_addr=%s startup_timeout=%s probe_interval=%s probe_timeout=%s shutdown_timeout=%s migrate_lock_timeout=%s index_rpc_timeout=%s index_poll_interval=%s index_retry_initial=%s index_retry_max=%s log_start_height=%d log_contracts=%d log_config_hash=%s log_batch_blocks=%d deposit_start_height=%d deposit_contracts=%d deposit_watch_addresses=%d deposit_config_hash=%s deposit_batch_blocks=%d confirmation_depth=%d reorg_max_depth=%s reorg_replay_batch=%d nonce_read_token=%s signer_http_addr=%s signer_mode=%s signer_key_timeout=%s signer_chains=%d signer_senders=%d signer_assets=%d signer_recipients=%d signer_max_gas_limit=%d tx_signer_url=%s tx_signer_credential=%s tx_send_timeout=%s worker_ttl=%s worker_heartbeat=%s worker_stall=%s worker_backoff_base=%s worker_backoff_max=%s worker_scan_interval=%s worker_label=%q",
		logx.Redact(c.PGDSN), logx.Redact(c.RPCURL), c.ChainID, c.StartHeight, c.HTTPAddr,
		c.StartupTimeout, c.ProbeInterval, c.ProbeTimeout, c.ShutdownTimeout, c.MigrateLockTimeout,
		c.IndexRPCTimeout, c.IndexPollInterval, c.IndexRetryInitial, c.IndexRetryMax,
		c.LogStartHeight, len(c.LogContracts), c.LogConfigHash, c.LogBatchBlocks,
		c.DepositStartHeight, len(c.DepositContracts), len(c.DepositWatchAddresses),
		c.DepositConfigHash, c.DepositBatchBlocks, c.ConfirmationDepth, c.ReorgMaxDepthRaw,
		c.ReorgReplayBatch, nonceReadToken, c.SignerHTTPAddr, c.SignerMode, c.SignerKeyTimeout,
		len(c.SignerChains), len(c.SignerSenders), len(c.SignerAssets),
		len(c.SignerRecipients), c.SignerMaxGasLimit,
		logx.Redact(c.TxSignerURL), txSignerCredential, c.TxSendTimeout,
		c.WorkerTTL, c.WorkerHeartbeat, c.WorkerStall, c.WorkerBackoffBase,
		c.WorkerBackoffMax, c.WorkerScanInterval, c.WorkerLabel,
	)
}

// NormalizeWhitelist validates a comma-separated allowlist of EVM contract
// addresses and returns the canonical form: lowercase 0x-prefixed hex, sorted
// and deduplicated, plus the versioned config identity
// SHA-256("erc20-transfer:v1\n" + join("\n")) as lowercase hex
// (spec clarification A1, research R3). A blank allowlist is a configuration
// error and never degrades into a full-chain query (FR-04).
func NormalizeWhitelist(raw string) ([]string, string, error) {
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, "", errors.New("allowlist contains a blank entry")
		}
		if !common.IsHexAddress(p) {
			return nil, "", fmt.Errorf("%q is not a 20-byte EVM address", p)
		}
		norm := strings.ToLower(p)
		if _, dup := seen[norm]; dup {
			continue
		}
		seen[norm] = struct{}{}
		out = append(out, norm)
	}
	if len(out) == 0 {
		return nil, "", errors.New("allowlist is empty")
	}
	sort.Strings(out)
	sum := sha256.Sum256([]byte(logConfigVersion + "\n" + strings.Join(out, "\n")))
	return out, hex.EncodeToString(sum[:]), nil
}

// parseDepositEntries validates a comma-separated collection of
// `0x…[:effective]` items and returns the canonical form: lowercase
// 0x-prefixed addresses with their effective height (explicit decimal suffix,
// or defaultEffective when omitted), sorted by canonical line and
// deduplicated. The same address with different effective heights stays as
// distinct entries (research R3); a blank item or an empty collection is a
// configuration error (FR-04/FR-05).
func parseDepositEntries(raw string, defaultEffective uint64) ([]DepositEntry, error) {
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]DepositEntry, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, errors.New("address list contains a blank entry")
		}
		addr, effRaw, hasEff := strings.Cut(p, ":")
		if !common.IsHexAddress(addr) {
			return nil, fmt.Errorf("%q is not a 20-byte EVM address", addr)
		}
		e := DepositEntry{Address: strings.ToLower(common.HexToAddress(addr).Hex()), Effective: defaultEffective}
		if hasEff {
			h, err := strconv.ParseUint(effRaw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("effective height %q is not a non-negative decimal integer", effRaw)
			}
			e.Effective = h
		}
		if _, dup := seen[e.String()]; dup {
			continue
		}
		seen[e.String()] = struct{}{}
		out = append(out, e)
	}
	if len(out) == 0 {
		return nil, errors.New("address list is empty")
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, nil
}

// depositIdentity computes SHA-256 over the versioned, deterministic encoding
// `deposit:v1\nstart:<S>\n` + `asset:<contract>:<effective>` lines +
// `watch:<address>:<effective>` lines, each collection in lexicographic order
// with no trailing newline (research R3, data-model config identity section).
func depositIdentity(start uint64, contracts, watches []DepositEntry) string {
	lines := make([]string, 0, len(contracts)+len(watches))
	for _, e := range contracts {
		lines = append(lines, "asset:"+e.String())
	}
	for _, e := range watches {
		lines = append(lines, "watch:"+e.String())
	}
	sum := sha256.Sum256([]byte(depositIdentityInput(start, lines)))
	return hex.EncodeToString(sum[:])
}

// depositIdentityInput builds the exact bytes hashed by depositIdentity; kept
// separate so tests can pin the encoding (no BOM, no trailing newline).
func depositIdentityInput(start uint64, lines []string) string {
	return depositConfigVersion + "\nstart:" + strconv.FormatUint(start, 10) + "\n" + strings.Join(lines, "\n")
}

// DepositSnapshot encodes entries as `<address>:<effective>` lines in
// lexicographic order with no trailing newline — the canonical `assets` /
// `watches` history snapshot text (data-model Table 4, research R11).
func DepositSnapshot(entries []DepositEntry) string {
	lines := make([]string, 0, len(entries))
	for _, e := range entries {
		lines = append(lines, e.String())
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
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

// parseConfirmationDepth accepts N in [1, MaxInt64]: the BIGINT storage
// system range, not a business cap. Zero is refused as non-positive;
// larger values are refused as out of system range.
func parseConfirmationDepth(raw string) (uint64, error) {
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a decimal integer", raw)
	}
	if n == 0 {
		return 0, errors.New("must be a positive decimal integer (> 0)")
	}
	if n > 1<<63-1 {
		return 0, fmt.Errorf("%q exceeds system-supported integer range (max 9223372036854775807)", raw)
	}
	return n, nil
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

// loadSigner parses the 009 signer knobs when present. Malformed present
// values are Load errors; absent values stay unset and SignerPolicyConfig
// refuses the signer paths until they are provided.
func (c *Config) loadSigner(getenv Getenv, errs *[]error) {
	c.SignerHTTPAddr = DefaultSignerHTTPAddr
	if raw, ok := getenv(EnvSignerHTTPAddr); ok && raw != "" {
		if err := validateHTTPAddr(raw); err != nil {
			*errs = append(*errs, invalid(EnvSignerHTTPAddr, "%v", err))
		} else {
			c.SignerHTTPAddr = raw
		}
	}

	c.SignerMode = DefaultSignerMode
	if raw, ok := getenv(EnvSignerMode); ok && raw != "" {
		if raw != "development" && raw != "production" {
			*errs = append(*errs, invalid(EnvSignerMode, "%q must be development or production", raw))
		} else {
			c.SignerMode = raw
		}
	}

	if raw, ok := getenv(EnvSignerKeyFile); ok {
		c.SignerKeyFile = raw
	}
	c.SignerKeyTimeout = duration(getenv, EnvSignerKeyTimeout, DefaultSignerKeyTimeout, errs)

	splitList := func(name string) []string {
		raw, ok := getenv(name)
		if !ok || strings.TrimSpace(raw) == "" {
			return nil
		}
		var out []string
		for _, part := range strings.Split(raw, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	}
	if raw, ok := getenv(EnvSignerChains); ok && strings.TrimSpace(raw) != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			n, err := strconv.ParseUint(part, 10, 64)
			if err != nil || n == 0 || n > 1<<63-1 {
				*errs = append(*errs, invalid(EnvSignerChains, "%q is not a positive chain id", part))
				continue
			}
			c.SignerChains = append(c.SignerChains, int64(n))
		}
	}
	c.SignerSenders = splitList(EnvSignerSenders)
	c.SignerAssets = splitList(EnvSignerAssets)
	c.SignerRecipients = splitList(EnvSignerRecipients)

	positiveBig := func(name string) *big.Int {
		raw, ok := getenv(name)
		if !ok || strings.TrimSpace(raw) == "" {
			return nil
		}
		raw = strings.TrimSpace(raw)
		v, ok := new(big.Int).SetString(raw, 10)
		if !ok || v.Sign() <= 0 {
			*errs = append(*errs, invalid(name, "%q is not a positive decimal integer", raw))
			return nil
		}
		return v
	}
	c.SignerMaxAmount = positiveBig(EnvSignerMaxAmount)
	c.SignerMaxFeePerGas = positiveBig(EnvSignerMaxFeePerGas)
	c.SignerMaxPriorityFee = positiveBig(EnvSignerMaxPriorityFee)
	c.SignerMaxGasPrice = positiveBig(EnvSignerMaxGasPrice)
	if raw, ok := getenv(EnvSignerMaxGasLimit); ok && strings.TrimSpace(raw) != "" {
		n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil || n == 0 {
			*errs = append(*errs, invalid(EnvSignerMaxGasLimit, "%q is not a positive decimal integer", raw))
		} else {
			c.SignerMaxGasLimit = n
		}
	}
}

// loadTxLifecycle parses the 010 caller knobs (T001). The URL is validated as
// an http(s) endpoint when present; the credential passes through verbatim and
// is never formatted into an error; the dispatch timeout defaults to 15s and
// refuses zero/negative values via duration (fail-closed).
func (c *Config) loadTxLifecycle(getenv Getenv, errs *[]error) {
	c.TxSendTimeout = DefaultTxSendTimeout
	if raw, ok := getenv(EnvTxSignerURL); ok && raw != "" {
		if err := validateHTTPURL(raw); err != nil {
			*errs = append(*errs, invalid(EnvTxSignerURL, "%v", err))
		} else {
			c.TxSignerURL = raw
		}
	}
	if raw, ok := getenv(EnvTxSignerCredential); ok && raw != "" {
		c.TxSignerCredential = raw
	}
	c.TxSendTimeout = duration(getenv, EnvTxSendTimeout, c.TxSendTimeout, errs)
}

// loadWorker parses the 011 worker knobs. Every knob is optional with an
// approved-initial-config default; malformed or non-positive values are Load
// errors, and the relationship heartbeat < TTL < stall is enforced fail-closed
// (T001, research R14). The stall window is never derived from the TTL.
func (c *Config) loadWorker(getenv Getenv, errs *[]error) {
	c.WorkerTTL = DefaultWorkerTTL
	c.WorkerHeartbeat = DefaultWorkerHeartbeat
	c.WorkerStall = DefaultWorkerStall
	c.WorkerBackoffBase = DefaultWorkerBackoffBase
	c.WorkerBackoffMax = DefaultWorkerBackoffMax
	c.WorkerScanInterval = DefaultWorkerScanInterval

	positive := func(name string, def time.Duration, unit time.Duration) time.Duration {
		raw, ok := getenv(name)
		if !ok || raw == "" {
			return def
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n <= 0 {
			*errs = append(*errs, invalid(name, "%q is not a positive decimal integer", raw))
			return def
		}
		return time.Duration(n) * unit
	}
	c.WorkerTTL = positive(EnvWorkerTTLSeconds, c.WorkerTTL, time.Second)
	c.WorkerHeartbeat = positive(EnvWorkerHeartbeatSeconds, c.WorkerHeartbeat, time.Second)
	c.WorkerStall = positive(EnvWorkerStallSeconds, c.WorkerStall, time.Second)
	c.WorkerBackoffBase = positive(EnvWorkerBackoffBaseMS, c.WorkerBackoffBase, time.Millisecond)
	c.WorkerBackoffMax = positive(EnvWorkerBackoffMaxMS, c.WorkerBackoffMax, time.Millisecond)
	c.WorkerScanInterval = positive(EnvWorkerScanIntervalMS, c.WorkerScanInterval, time.Millisecond)

	if raw, ok := getenv(EnvWorkerLabel); ok {
		c.WorkerLabel = raw
	}

	if c.WorkerHeartbeat >= c.WorkerTTL {
		*errs = append(*errs, invalid(EnvWorkerHeartbeatSeconds,
			"heartbeat %s must be < ttl %s", c.WorkerHeartbeat, c.WorkerTTL))
	}
	if c.WorkerStall <= c.WorkerTTL {
		*errs = append(*errs, invalid(EnvWorkerStallSeconds,
			"stall window %s must be > ttl %s (independent value)", c.WorkerStall, c.WorkerTTL))
	}
}

// SignerPolicyConfig enforces required-ness for the signer paths and builds
// the versioned policy input. Serve/migrate flows never call it, so their
// environments stay valid without signer knobs.
func (c *Config) SignerPolicyConfig() (signer.PolicyConfig, error) {
	var zero signer.PolicyConfig
	missing := func(name string) (signer.PolicyConfig, error) {
		return zero, fmt.Errorf("%s is required for the signer paths", name)
	}
	if len(c.SignerChains) == 0 {
		return missing(EnvSignerChains)
	}
	if len(c.SignerSenders) == 0 {
		return missing(EnvSignerSenders)
	}
	if len(c.SignerAssets) == 0 {
		return missing(EnvSignerAssets)
	}
	if len(c.SignerRecipients) == 0 {
		return missing(EnvSignerRecipients)
	}
	if c.SignerMaxAmount == nil {
		return missing(EnvSignerMaxAmount)
	}
	if c.SignerMaxGasLimit == 0 {
		return missing(EnvSignerMaxGasLimit)
	}
	if c.SignerMaxFeePerGas == nil {
		return missing(EnvSignerMaxFeePerGas)
	}
	if c.SignerMaxPriorityFee == nil {
		return missing(EnvSignerMaxPriorityFee)
	}
	if c.SignerMaxGasPrice == nil {
		return missing(EnvSignerMaxGasPrice)
	}
	return signer.PolicyConfig{
		ChainIDs:             append([]int64(nil), c.SignerChains...),
		Senders:              append([]string(nil), c.SignerSenders...),
		Assets:               append([]string(nil), c.SignerAssets...),
		Recipients:           append([]string(nil), c.SignerRecipients...),
		MaxAmount:            new(big.Int).Set(c.SignerMaxAmount),
		MaxGasLimit:          c.SignerMaxGasLimit,
		MaxFeePerGas:         new(big.Int).Set(c.SignerMaxFeePerGas),
		MaxPriorityFeePerGas: new(big.Int).Set(c.SignerMaxPriorityFee),
		MaxGasPrice:          new(big.Int).Set(c.SignerMaxGasPrice),
	}, nil
}
