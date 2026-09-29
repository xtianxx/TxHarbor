// eventrecovery.go owns the shared 015 resumption-gate assembly of the two
// events entry points (T033 event-publisher / T034 event-consumer): the gate
// wiring, the closed-refusal error both entries surface, and the canonical
// capability scopes of their surfaces (T050: chain + capability, no entry
// identity).
//
// Both entries follow the same fail-closed rule as every other 015 entry
// wiring:
//
//   - No TXHARBOR_RECOVERY_CONTROL_DSN: normal mode. No recovery instance can
//     be observed, so the entry keeps its pre-015 behavior unchanged (FR-023)
//     and the wiring is nil (a nil wiring passes every admission through).
//   - TXHARBOR_RECOVERY_CONTROL_DSN configured: the entry refuses to start when
//     the gate cannot be assembled (missing gate TTL, unreachable control
//     store, unknown/incompatible schema version). It never degrades to
//     pass-through.
//   - TXHARBOR_RECOVERY_INSTANCE bound without the control store: refuse. A
//     bound recovery process cannot be evaluated without the control store.
//
// The gate (T012, gate.go) is the single derived evaluation; this file only
// assembles it and converts its verdict into the entry-visible refusal. No
// admission logic is re-derived or copied here.
package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// eventsGateAdmitter is the single admission surface the events entries
// consume; the T012 gate is the only production implementation. The interface
// keeps the wiring testable without a control store.
type eventsGateAdmitter interface {
	Admit(ctx context.Context, req recovery.GateRequest) (recovery.GateDecision, error)
}

// eventsRecoveryWiring is the events-side 015 assembly shared by the
// event-publisher and event-consumer entries. A nil wiring means recovery mode
// is not configured (no TXHARBOR_RECOVERY_CONTROL_DSN): no recovery instance
// can exist and the entry keeps its pre-015 behavior (FR-023). A non-nil
// wiring evaluates every gated action against the control store; the pool is
// owned by the command and closed on return.
type eventsRecoveryWiring struct {
	gate eventsGateAdmitter
	pool *pgxpool.Pool
	// scope is the canonical business scope of the entry's capability (T050:
	// chain + capability, no entry identity). It is derived from the
	// capability by the assembly, never passed in as a string.
	scope string
	// actor is the audit label of the deployment principal; it authorizes
	// nothing by itself.
	actor string
	// instance is the TXHARBOR_RECOVERY_INSTANCE binding ("" = unbound). An
	// unbound process refuses while any recovery instance is open.
	instance string
}

// close releases the control-store pool. Nil-safe.
func (w *eventsRecoveryWiring) close() {
	if w != nil && w.pool != nil {
		w.pool.Close()
	}
}

// eventsGateRefusedError marks one action refused by the 015 gate. The gate
// already recorded the refusal audit row; this type carries the closed
// refusal_class to the entry's output and stops the gated loop without
// claiming, settling, publishing or applying anything.
type eventsGateRefusedError struct {
	Capability   recovery.Capability
	Action       string
	RefusalClass recovery.RefusalClass
	Reason       string
}

// Error renders the refusal in the entry-visible form. The `refusal_class`
// marker is the contract surface of contracts/resumption-gate.md §1.1 ("拒绝
// 必须暴露闭集 refusal_class"): every command refusal line carries it.
func (e *eventsGateRefusedError) Error() string {
	return fmt.Sprintf("capability=%s action=%s refusal_class=%s reason=%s",
		e.Capability, e.Action, e.RefusalClass, e.Reason)
}

// require evaluates one single-action admission of capability through the
// gate. A nil wiring (no control store configured) passes through: normal mode
// never reaches an open recovery instance and the daily runtime is unchanged
// (FR-023). A non-nil error is always a *eventsGateRefusedError: the action
// must not run and the refusal_class is already audited by the gate.
//
// One admission covers exactly this one action (the gate's R3 protocol); the
// caller never reuses it for a batch, loop step or retry.
func (w *eventsRecoveryWiring) require(ctx context.Context, capability recovery.Capability, action string) error {
	if w == nil {
		return nil
	}
	dec, err := w.gate.Admit(ctx, recovery.GateRequest{
		InstanceID: w.instance,
		Capability: capability,
		ScopeHash:  w.scope,
		Actor:      w.actor,
		Action:     action,
	})
	if err != nil {
		// The evaluation could not complete (control store unavailable, or a
		// caller contract error). Fail closed with the class the gate already
		// decided, falling back to control_store_unavailable so the surfaced
		// class is always a member of the closed set.
		class := dec.RefusalClass
		if !class.Known() || class == recovery.RefusalNoInstance {
			class = recovery.RefusalControlStoreUnavailable
		}
		return &eventsGateRefusedError{
			Capability:   capability,
			Action:       action,
			RefusalClass: class,
			Reason:       logx.Redact(err.Error()),
		}
	}
	if dec.Allowed {
		return nil
	}
	class := dec.RefusalClass
	if !class.Known() || class == recovery.RefusalNoInstance {
		class = recovery.RefusalControlStoreUnavailable
	}
	return &eventsGateRefusedError{
		Capability:   capability,
		Action:       action,
		RefusalClass: class,
		Reason:       dec.Reason,
	}
}

// assembleEventsRecovery builds the events-entry gate wiring. The capability
// is the single source of the entry's canonical scope (T050); the caller never
// passes a scope string. It is fail-closed in both directions: without
// TXHARBOR_RECOVERY_CONTROL_DSN the entry is in normal mode and returns
// (nil, nil) — a process bound by TXHARBOR_RECOVERY_INSTANCE without the
// control store refuses; with the control store configured, a missing gate
// TTL, a malformed effect-class ruling, an unusable chain, an unreachable
// store or an unknown/incompatible schema version refuses startup instead of
// degrading to pass-through (INV-5/INV-11).
func assembleEventsRecovery(ctx context.Context, cfg *config.Config, getenv func(string) (string, bool), capability recovery.Capability) (*eventsRecoveryWiring, error) {
	var instance string
	if getenv != nil {
		if raw, ok := getenv(config.EnvRecoveryInstance); ok {
			instance = strings.TrimSpace(raw)
		}
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
	scope, err := recoveryScopeFor(cfg.ChainID, capability)
	if err != nil {
		return nil, fmt.Errorf("event capability scope: %s", logx.Redact(err.Error()))
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
	return &eventsRecoveryWiring{
		gate:     gate,
		pool:     pool,
		scope:    scope,
		actor:    cfg.Recovery.Principal,
		instance: instance,
	}, nil
}
