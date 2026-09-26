// attribution_unit_test.go is the unit layer for the T033/T034/T036
// reconcile-admin seams and the new 014 configuration keys:
//
//   - the attribution source sets (004 `<address>:<effective>` snapshots and
//     the configured signer senders) canonicalize conservatively: a malformed
//     entry is refused, a blank snapshot is unknown (never an empty set);
//   - direction attribution requires the asset allowlist AND the matching
//     project address side; empty/undecodable directions are never guessed;
//   - merged chain-first passes count each decided-unattributed log exactly
//     once and never let a metrics-only fact erase a gap or an attributed fact;
//   - the new required keys fail closed by name: WINDOW_MAX_PROBES, the scan
//     budget limits, the scan bounds and the settle limit.
//
// No database, no Docker.
package reconcileadmin

import (
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/reconciliation"
)

const (
	attrAsset    = "0x1111111111111111111111111111111111111111"
	attrWatch    = "0x2222222222222222222222222222222222222222"
	attrSender   = "0x3333333333333333333333333333333333333333"
	attrOther    = "0x4444444444444444444444444444444444444444"
	attrContract = "0x5555555555555555555555555555555555555555"
)

func TestCanonicalReconcileAddresses(t *testing.T) {
	canonical, err := canonicalReconcileAddresses([]string{
		strings.ToUpper(attrSender),
		attrSender, // duplicate after lowercasing
		" " + attrWatch + "\t",
		"",
		"   ",
	})
	if err != nil {
		t.Fatalf("canonicalReconcileAddresses: %v", err)
	}
	if len(canonical) != 2 {
		t.Fatalf("canonical set = %d entries, want 2 (deduplicated, blanks skipped)", len(canonical))
	}
	for _, want := range []string{attrSender, attrWatch} {
		if _, ok := canonical[want]; !ok {
			t.Fatalf("canonical set %v lost %s", canonical, want)
		}
	}

	empty, err := canonicalReconcileAddresses(nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty list = %v (err %v), want an empty set", empty, err)
	}

	for _, bad := range []string{"not-an-address", "0x1234", "0x" + strings.Repeat("zz", 20)} {
		if _, err := canonicalReconcileAddresses([]string{bad}); err == nil {
			t.Fatalf("malformed configured address %q accepted", bad)
		}
	}
}

func TestParseReconcileAddressSnapshot(t *testing.T) {
	known, isKnown, err := parseReconcileAddressSnapshot(
		strings.ToUpper(attrAsset) + ":0\n" + attrWatch + ":12345\n" + attrAsset + ":7\n")
	if err != nil {
		t.Fatalf("parseReconcileAddressSnapshot: %v", err)
	}
	if !isKnown || len(known) != 2 {
		t.Fatalf("parsed snapshot = %v known=%v, want 2 canonical addresses", known, isKnown)
	}
	if _, ok := known[attrAsset]; !ok {
		t.Fatalf("snapshot did not canonically key %s", attrAsset)
	}

	for _, blank := range []string{"", "   ", "\n\t\n"} {
		set, isKnown, err := parseReconcileAddressSnapshot(blank)
		if err != nil || isKnown || len(set) != 0 {
			t.Fatalf("blank snapshot %q -> (%v, known=%v, err=%v), want unknown/empty", blank, set, isKnown, err)
		}
	}

	for _, malformed := range []string{
		"not-an-address:0",
		attrAsset, // no :effective separator
	} {
		if set, _, err := parseReconcileAddressSnapshot(malformed); err == nil {
			t.Fatalf("malformed snapshot %q accepted -> %v", malformed, set)
		}
	}
	// `<address>:` still yields the address set: the address part is what
	// attribution consumes, the effective marker is carried by 004 itself.
	if set, known, err := parseReconcileAddressSnapshot(attrAsset + ":"); err != nil || !known || len(set) != 1 {
		t.Fatalf("address-with-empty-marker snapshot = (%v, known=%v, err=%v), want the address set", set, known, err)
	}
}

func TestReconcileMemberDirection(t *testing.T) {
	assets := map[string]struct{}{attrContract: {}}
	outgoing := map[string]struct{}{attrSender: {}}
	incoming := map[string]struct{}{attrWatch: {}}

	mkLog := func(from, to, contract string) reconciliation.ChainFactLog {
		return reconciliation.ChainFactLog{
			BlockNumber: 100, BlockHash: "0xaa", TxHash: "0xt1", LogIndex: 0,
			Contract: contract, Topic0: "0xd", From: from, To: to,
		}
	}

	cases := []struct {
		name       string
		log        reconciliation.ChainFactLog
		project    map[string]struct{}
		out        bool
		attributed bool
	}{
		{"withdrawal_attributed", mkLog(attrSender, attrOther, attrContract), outgoing, true, true},
		{"withdrawal_wrong_direction", mkLog(attrOther, attrSender, attrContract), outgoing, true, false},
		{"withdrawal_unknown_sender", mkLog("", attrOther, attrContract), outgoing, true, false},
		{"withdrawal_asset_not_allowed", mkLog(attrSender, attrOther, attrOther), outgoing, true, false},
		{"deposit_attributed", mkLog(attrOther, attrWatch, attrContract), incoming, false, true},
		{"deposit_wrong_direction", mkLog(attrWatch, attrOther, attrContract), incoming, false, false},
		{"deposit_unknown_recipient", mkLog(attrOther, "0x", attrContract), incoming, false, false},
		{"case_insensitive", mkLog(strings.ToUpper(attrSender), strings.ToUpper(attrOther), strings.ToUpper(attrContract)), outgoing, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, got := reconcileMemberDirection(tc.log, assets, tc.project, tc.out); got != tc.attributed {
				t.Fatalf("reconcileMemberDirection = %v, want %v", got, tc.attributed)
			}
		})
	}
}

func TestMergeChainFirstPasses(t *testing.T) {
	key := func(tx string, index int64) reconcileChainLogKey {
		return reconcileChainLogKey{blockNumber: 100, blockHash: "0xaa", txHash: tx, logIndex: index}
	}
	withdrawalPass := newReconcileChainFirstPass()
	withdrawalPass.attributed[key("0xt1", 0)] = struct{}{}
	withdrawalPass.unattributed[key("0xt2", 0)] = struct{}{}
	withdrawalPass.candidates = append(withdrawalPass.candidates, reconciliation.ScanCandidate{
		BusinessType: reconciliation.BusinessWithdrawal,
		BusinessKey:  reconciliation.BusinessKey{Kind: reconciliation.BusinessKeyTxHash, Value: "0xt1"},
	})

	depositPass := newReconcileChainFirstPass()
	// The same decided-unattributed log observed by a second direction pass
	// must stay one metrics-only count, not two.
	depositPass.unattributed[key("0xt2", 0)] = struct{}{}
	depositPass.unattributed[key("0xt3", 0)] = struct{}{}
	depositPass.attributed[key("0xt1", 0)] = struct{}{} // also attributed elsewhere

	undecidable := newReconcileChainFirstPass()
	undecidable.gap = true

	enum := reconciliation.ScanEnumeration{}
	mergeChainFirstPasses(&enum, withdrawalPass, depositPass, undecidable)
	if len(enum.Candidates) != 1 {
		t.Fatalf("candidates = %d, want the single attributed withdrawal candidate", len(enum.Candidates))
	}
	if enum.MetricsOnly != 2 {
		t.Fatalf("metrics-only = %d, want 2 (0xt2 counted once, 0xt3 once)", enum.MetricsOnly)
	}
	if len(enum.GapReasons) != 1 || enum.GapReasons[0] != reconciliation.GapQueryFailed {
		t.Fatalf("gap reasons = %v, want one query_failed gap from the undecidable pass", enum.GapReasons)
	}

	// An empty merge changes nothing: no phantom metrics-only or gap.
	clean := reconciliation.ScanEnumeration{}
	mergeChainFirstPasses(&clean, newReconcileChainFirstPass())
	if clean.MetricsOnly != 0 || len(clean.GapReasons) != 0 || len(clean.Candidates) != 0 {
		t.Fatalf("empty merge = %+v, want untouched", clean)
	}
}

func TestReconcileWindowProbesRefusesMissingKey(t *testing.T) {
	if probes, err := reconcileWindowProbes(&config.Config{Recon: config.ReconConfig{WindowMaxProbes: 5}}); err != nil || probes != 5 {
		t.Fatalf("configured probes = (%d, %v), want (5, nil)", probes, err)
	}
	for _, value := range []int{0, -1} {
		_, err := reconcileWindowProbes(&config.Config{Recon: config.ReconConfig{WindowMaxProbes: value}})
		if err == nil || !strings.Contains(err.Error(), config.EnvReconWindowMaxProbes) {
			t.Fatalf("probes=%d err = %v, want a refusal naming %s", value, err, config.EnvReconWindowMaxProbes)
		}
	}
}

func TestReconcileConfigWindowMaxProbesKeyParsing(t *testing.T) {
	base := map[string]string{
		config.EnvPGDSN:                 "postgres://txharbor:sup3rs3cret@127.0.0.1:5432/txharbor?sslmode=disable",
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
	}
	load := func(t *testing.T, env map[string]string) (*config.Config, error) {
		t.Helper()
		return config.Load(func(key string) (string, bool) { value, ok := env[key]; return value, ok })
	}

	env := map[string]string{}
	for key, value := range base {
		env[key] = value
	}
	env[config.EnvReconWindowMaxProbes] = "7"
	cfg, err := load(t, env)
	if err != nil {
		t.Fatalf("config.Load with %s=7: %v", config.EnvReconWindowMaxProbes, err)
	}
	if cfg.Recon.WindowMaxProbes != 7 {
		t.Fatalf("WindowMaxProbes = %d, want 7", cfg.Recon.WindowMaxProbes)
	}

	for _, bad := range []string{"0", "-3", "abc", "1.5"} {
		env[config.EnvReconWindowMaxProbes] = bad
		if _, err := load(t, env); err == nil || !strings.Contains(err.Error(), config.EnvReconWindowMaxProbes) {
			t.Fatalf("%s=%q err = %v, want a refusal naming the key", config.EnvReconWindowMaxProbes, bad, err)
		}
	}

	// The key is optional at load time (height scans never use it); the
	// command refuses it by name only when a time scope needs it.
	env = map[string]string{}
	for key, value := range base {
		env[key] = value
	}
	if _, err := load(t, env); err != nil {
		t.Fatalf("config.Load without %s: %v", config.EnvReconWindowMaxProbes, err)
	}
}

func TestReconcileScanLimitHelpersRefuseMissingKeys(t *testing.T) {
	limits, err := reconcileScanLimits(&config.Config{})
	if err == nil {
		t.Fatalf("zero scan limits accepted: %+v", limits)
	}
	for _, name := range []string{
		config.EnvReconConcurrency, config.EnvReconMaxSpanPerClaim, config.EnvReconMaxPGRequests,
		config.EnvReconMaxRPCRequests, config.EnvReconMaxDuration,
	} {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("limits refusal %q does not name %s", err, name)
		}
	}

	good := &config.Config{Recon: config.ReconConfig{
		Concurrency: 1, MaxSpanPerClaim: 4, MaxPGRequests: 8, MaxRPCRequests: 2, MaxDuration: time.Minute,
	}}
	limits, err = reconcileScanLimits(good)
	if err != nil || limits.MaxSpanPerClaim != 4 {
		t.Fatalf("valid limits = (%+v, %v), want the configured budget", limits, err)
	}

	if _, _, _, _, _, err := reconcileScanBounds(&config.Config{}); err == nil {
		t.Fatalf("zero scan bounds accepted")
	} else {
		for _, name := range []string{
			config.EnvReconLeaseTTL, config.EnvReconFreshnessTolerance, config.EnvReconMaxTipLag,
			config.EnvReconMaxCandidates, config.EnvReconMaxEventRows,
		} {
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("bounds refusal %q does not name %s", err, name)
			}
		}
	}
	if _, err := reconcileSettleLimit(&config.Config{}); err == nil || !strings.Contains(err.Error(), config.EnvReconSettleLimit) {
		t.Fatalf("zero settle limit err = %v, want a refusal naming %s", err, config.EnvReconSettleLimit)
	}
}

func TestReconcileCandidateKey(t *testing.T) {
	key, ok := reconcileCandidateKey("intent-1", "req-1")
	if !ok || key.Kind != reconciliation.BusinessKeyRequestID || key.Value != "req-1" {
		t.Fatalf("request/intent key = (%+v, %v), want request_id=req-1", key, ok)
	}
	key, ok = reconcileCandidateKey("intent-1", "")
	if !ok || key.Kind != reconciliation.BusinessKeyIntentID || key.Value != "intent-1" {
		t.Fatalf("intent-only key = (%+v, %v), want intent_id=intent-1", key, ok)
	}
	if _, ok := reconcileCandidateKey("", " "); ok {
		t.Fatalf("key with neither identity was accepted")
	}
}
