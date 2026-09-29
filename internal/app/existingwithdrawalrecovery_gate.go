// existingwithdrawalrecovery_gate.go is the 015 resumption-gate assembly for
// the existing_withdrawal_recovery capability family owned by T032/T070: the
// withdrawal-worker process, the withdrawal-exec operator CLI, the execution
// HTTP write path (POST /withdrawals/{request_id}/execution) and the signer
// delivery checkpoint (T070 process-internal check point).
//
// It mirrors the serve-side assembly (serve.go, T030) so all B9 lanes share one
// fail-closed posture:
//
//   - Without TXHARBOR_RECOVERY_CONTROL_DSN the process is in normal mode
//     (FR-023): no recovery instance can be observed and every entry keeps its
//     pre-015 behavior. A process explicitly bound through
//     TXHARBOR_RECOVERY_INSTANCE without the control store is a contradiction
//     and refuses: a bound process cannot be evaluated.
//   - With the control store configured, a missing/zero gate TTL, an
//     unreachable store or an unknown/incompatible schema version refuses
//     (startup or command refusal) instead of degrading to pass-through. The
//     gate is reached only through the version-guarded controlstore.Store, so
//     the T069 guard is inherited.
//
// This file performs no admission logic of its own: every evaluation calls the
// single T012 derived evaluation (*recovery.Gate). No call point copies or
// short-circuits the gate decision, and no configuration key disables it.
//
// Canonical scope (T050): every entry of this family acts on the capability
// existing_withdrawal_recovery, so they all derive the same canonical business
// scope through existingWithdrawalRecoveryScope (chain + capability; entry
// identity is not a scope dimension and the serve-side execution path derives
// the same scope). A release recorded at the canonical scope covers the whole
// family; a pre-normalization opaque scope matches nothing and requires a
// fresh approval/release.
package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// existingWithdrawalRecoveryAdmission is the admission surface the
// withdrawal entries consume; T012's *recovery.Gate is the only production
// implementation. The interface keeps the wiring testable without a control
// store.
type existingWithdrawalRecoveryAdmission interface {
	Admit(ctx context.Context, req recovery.GateRequest) (recovery.GateDecision, error)
}

// existingWithdrawalRecoveryWiring is the shared 015 assembly of the
// existing_withdrawal_recovery entries. A nil wiring means recovery mode is
// not configured (no TXHARBOR_RECOVERY_CONTROL_DSN): normal runtime, and a
// method call on the nil receiver admits normally (FR-023). A non-nil wiring
// evaluates every gated action against the control store; the pool is owned by
// the caller and closed through close().
type existingWithdrawalRecoveryWiring struct {
	gate existingWithdrawalRecoveryAdmission
	pool *pgxpool.Pool
	// scope is the opaque non-empty capability scope of this entry family
	// (T050 owns the canonical scope vocabulary).
	scope string
	// actor is the audit label of the deployment principal; it authorizes
	// nothing by itself.
	actor string
	// instance is the TXHARBOR_RECOVERY_INSTANCE binding ("" = unbound). An
	// unbound process refuses while any recovery instance is open.
	instance string
}

// close releases the control-store pool. Nil-safe.
func (w *existingWithdrawalRecoveryWiring) close() {
	if w != nil && w.pool != nil {
		w.pool.Close()
	}
}

// admit evaluates one single action of one capability through the gate. A nil
// wiring (normal mode) is a normal-mode pass-through and never an admission to
// act inside recovery mode: with no control store this process cannot observe
// an open recovery instance, and the deployment isolation checklist covers the
// process boundary (contracts/resumption-gate.md §1.2).
func (w *existingWithdrawalRecoveryWiring) admit(ctx context.Context, capability recovery.Capability, action, operationID string) (recovery.GateDecision, error) {
	if w == nil || w.gate == nil {
		return recovery.GateDecision{
			Allowed:      true,
			Normal:       true,
			RefusalClass: recovery.RefusalNoInstance,
			Capability:   capability,
			Reason:       "no recovery control store configured; normal-mode pass-through (FR-023)",
		}, nil
	}
	return w.gate.Admit(ctx, recovery.GateRequest{
		InstanceID:  w.instance,
		Capability:  capability,
		ScopeHash:   w.scope,
		Actor:       w.actor,
		OperationID: operationID,
		Action:      action,
	})
}

// existingWithdrawalRecoveryScope is the canonical business scope of the
// existing_withdrawal_recovery entry family (T050): chain + capability, no
// entry identity. A zero chain or an unknown capability refuses; there is no
// default scope.
func existingWithdrawalRecoveryScope(chainID uint64) (string, error) {
	return recoveryScopeFor(chainID, recovery.CapabilityExistingWithdrawalRecovery)
}

// assembleExistingWithdrawalRecovery builds the shared admission from the
// deployment configuration. It is fail-closed in both directions:
//
//   - no TXHARBOR_RECOVERY_CONTROL_DSN and no bound instance: (nil, nil) —
//     normal mode, the daily runtime is unchanged;
//   - no control store but a bound instance, or a control store without a
//     positive gate TTL, or an unreachable/unknown-schema control store: a
//     refusal naming the configuration key or the unavailable store. There is
//     no degraded pass-through.
func assembleExistingWithdrawalRecovery(ctx context.Context, cfg *config.Config, getenv func(string) (string, bool)) (*existingWithdrawalRecoveryWiring, error) {
	var instance string
	if raw, ok := getenv(config.EnvRecoveryInstance); ok {
		instance = strings.TrimSpace(raw)
	}
	if strings.TrimSpace(cfg.Recovery.ControlDSN) == "" {
		if instance != "" {
			return nil, fmt.Errorf(
				"%s binds this process to instance %s but %s is not configured; a bound recovery process cannot be evaluated (fail-closed)",
				config.EnvRecoveryInstance, instance, config.EnvRecoveryControlDSN)
		}
		return nil, nil // normal mode: no recovery instance can exist
	}
	if cfg.Recovery.GateTTL <= 0 {
		return nil, fmt.Errorf(
			"%s is required when %s is configured and must be a positive duration; the resumption gate has no default TTL",
			config.EnvRecoveryGateTTL, config.EnvRecoveryControlDSN)
	}
	ruling, err := recoveryEffectClassRuling(cfg)
	if err != nil {
		return nil, fmt.Errorf("gate assembly: %s", logx.Redact(err.Error()))
	}
	scope, err := existingWithdrawalRecoveryScope(cfg.ChainID)
	if err != nil {
		return nil, fmt.Errorf("recovery capability scope: %s", logx.Redact(err.Error()))
	}
	pool, err := db.OpenPool(ctx, cfg.Recovery.ControlDSN, cfg.ProbeTimeout)
	if err != nil {
		return nil, fmt.Errorf("control store unavailable: %s", logx.Redact(err.Error()))
	}
	store, err := controlstore.NewStore(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("control store unavailable: %s", logx.Redact(err.Error()))
	}
	gate, err := recovery.NewGate(store, recovery.GateOptions{TTL: cfg.Recovery.GateTTL, EffectClassRuling: ruling})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("gate assembly: %s", logx.Redact(err.Error()))
	}
	return &existingWithdrawalRecoveryWiring{
		gate:     gate,
		pool:     pool,
		scope:    scope,
		actor:    cfg.Recovery.Principal,
		instance: instance,
	}, nil
}

// existingWithdrawalRecoveryConfigFromEnv builds the partial deployment
// configuration for an entry whose construction site does not carry the
// process Deps (the execution HTTP handler rides the serve construction).
// Only the keys this assembly reads are parsed, with the same rules as
// config.Load: a malformed/non-positive TTL refuses by name, a missing TTL
// stays zero and assembleExistingWithdrawalRecovery refuses it when the
// control store is configured. The probe timeout is the existing config
// default (the handler has no Deps to read an override from).
func existingWithdrawalRecoveryConfigFromEnv(getenv func(string) (string, bool), chainID uint64) (*config.Config, error) {
	cfg := &config.Config{ChainID: chainID, ProbeTimeout: config.DefaultProbeTimeout}
	if raw, ok := getenv(config.EnvRecoveryControlDSN); ok {
		cfg.Recovery.ControlDSN = strings.TrimSpace(raw)
	}
	if raw, ok := getenv(config.EnvRecoveryPrincipal); ok {
		cfg.Recovery.Principal = strings.TrimSpace(raw)
	}
	if raw, ok := getenv(config.EnvRecoveryGateTTL); ok {
		raw = strings.TrimSpace(raw)
		if raw != "" {
			ttl, err := time.ParseDuration(raw)
			if err != nil {
				return nil, fmt.Errorf("%s is not a valid duration: %s", config.EnvRecoveryGateTTL, logx.Redact(err.Error()))
			}
			if ttl <= 0 {
				return nil, fmt.Errorf("%s must be a positive duration; the resumption gate has no default TTL", config.EnvRecoveryGateTTL)
			}
			cfg.Recovery.GateTTL = ttl
		}
	}
	if raw, ok := getenv(config.EnvRecoveryEffectClassRuling); ok {
		cfg.Recovery.EffectClassRulingJSON = strings.TrimSpace(raw)
	}
	return cfg, nil
}

// existingWithdrawalRecoveryRefusalClass maps one admission outcome onto the
// closed refusal set for callers that surface it. An unknown or missing class
// is surfaced as control_store_unavailable: the caller never invents a class
// and never turns a refused admission into a 2xx.
func existingWithdrawalRecoveryRefusalClass(dec recovery.GateDecision) recovery.RefusalClass {
	class := dec.RefusalClass
	if class == recovery.RefusalNoInstance || !class.Known() {
		class = recovery.RefusalControlStoreUnavailable
	}
	return class
}
