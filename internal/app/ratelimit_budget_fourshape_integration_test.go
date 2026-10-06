//go:build e2e

// ratelimit_budget_fourshape_integration_test.go is the assembly-level
// acceptance of the 013 "rate-limit decision total wait budget" four-shape
// decision-time candidate (design docs/evidence/013/redis-latency-budget-design.md
// §7 整决策耗时): shapes ①拒连 (dial refused) / ②合成拨号阻塞 (synthesized
// blocking dial) / ③冷连接初始化无响应 (cold connect, init unanswered) /
// ④热连接命令无响应 (warm conn converted to no-reply), asserting the caller
// observable `Allow` duration stays inside the budget envelope
// (T_allow <= L + tolerance with the actual maxima recorded) and that every
// failure is an ErrUnavailable decision.
//
// The matrix is 4 shapes × L ∈ {200ms, 300ms} × 2 rounds × 16 samples = 256
// samples. Every group builds the real assembly pieces
// (newLimiterRedisClient + ratelimit.NewRedisScriptStore + buildLimiter) over
// the testutil scripted gate; the refused/blackhole groups replace ONLY the
// client's Dialer and record an explicit per-field Options equivalence check
// against the production construction.
//
// The refused shape's caller-visible error identity is TWO-PHASED on go-redis
// v9.22.0 (the library's pool-level dial retry runs detached from the command
// context): phase 1 surfaces the budget deadline, phase 2 the cached refused
// dial error. The class set and the phase records capture the observed
// mechanism — see fourShapeExpectedClasses for the source loci. No timing or
// fault-hit bound is weakened by that.
//
// Real Redis carrier and the scripted gate; no serve process and no
// PostgreSQL are needed.
package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/ratelimit"
	"github.com/xtianxx/txharbor/internal/testutil"
)

// --- parameters -------------------------------------------------------------

const (
	// fourShapeRounds / fourShapeSamplesPerRound are the §7 sampling
	// parameters: n >= 16 per shape, two rounds.
	fourShapeRounds          = 2
	fourShapeSamplesPerRound = 16
	// fourShapeTolerance is the assembly-layer timing tolerance — the same
	// L+150ms envelope the pool-competition acceptance uses. The observed
	// maxima are recorded per group so the real epsilon stays visible.
	fourShapeTolerance = 150 * time.Millisecond

	fourShapeRefused       = "refused"
	fourShapeDialBlackhole = "dial_blackhole"
	fourShapeColdInit      = "cold_init"
	fourShapeHotBlocked    = "hot_blocked"

	// Refused-shape mechanism phases, derived from the caller-visible class
	// (see fourShapeExpectedClasses and the source loci cited there).
	fourShapePhaseBudgetExpiry = "budget_expiry_dial_retrying"
	fourShapePhaseFastPath     = "dial_error_fast_path"

	// fourShapeBackgroundDialNote labels the after-return counter
	// observation: background dial continuation after caller return is an
	// observable, not goroutine-exit proof（非协程退出证明）.
	fourShapeBackgroundDialNote = "background dial observation（非协程退出证明）: counter increments after the last caller return show the pool's detached dial work may continue; they are an observable, not goroutine-exit proof"

	// fourShapeRefusedPhaseNote is the recorded explanation of the refused
	// shape's two-phase identity (details at fourShapeExpectedClasses).
	fourShapeRefusedPhaseNote = "refused shape on go-redis v9.22.0: phase 1 (budget_expiry_dial_retrying, class timeout) — queuedNewConn runs the dial on a detached context (internal/pool/pool.go:1059) so the caller budget aborts only the wait while dialConn keeps retrying (DialerRetries default 5 x 100ms backoff, pool.go:707/782 ≈ 0.4-0.5s > L); phase 2 (dial_error_fast_path, class refused) — once dialErrorsNum saturates, dialConn short-circuits to the cached refused error (pool.go:692, bookkeeping pool.go:765, background tryDial probe resets only on success pool.go:806-830). Both are ErrUnavailable; no timing bound is weakened."

	// fourShapeProductionNoEquivalenceNote marks snapshots whose client IS
	// the production construction (no dialer substitution): there is nothing
	// to compare against, so no Options equivalence result applies.
	fourShapeProductionNoEquivalenceNote = "production construction used as-is (no dialer substitution); no Options equivalence comparison applies"

	// fourShapeBlackholeFallback is the blackhole dialer's own fallback
	// (mirrors the experiment dialer): the attempt context deadline (the
	// pool's per-attempt DialTimeout) normally fires first.
	fourShapeBlackholeFallback = 5 * time.Second
	// fourShapeBlackholeMarker is the ctx-cancel marker the synthesized dial
	// returns; the experiment dialer uses the same wording.
	fourShapeBlackholeMarker = "blackhole dial canceled"
)

var (
	fourShapeBudgets = []time.Duration{200 * time.Millisecond, 300 * time.Millisecond}
	fourShapeNames   = []string{fourShapeRefused, fourShapeDialBlackhole, fourShapeColdInit, fourShapeHotBlocked}
)

// --- evidence records -------------------------------------------------------

// fourShapeParams is the fixed-parameter block of one run.
type fourShapeParams struct {
	Test             string              `json:"test"`
	Shapes           []string            `json:"shapes"`
	BudgetsMS        []float64           `json:"budgets_ms"`
	Rounds           int                 `json:"rounds"`
	SamplesPerRound  int                 `json:"samples_per_round"`
	SamplesPerGroup  int                 `json:"samples_per_group"`
	TotalSamples     int                 `json:"total_samples"`
	ToleranceMS      float64             `json:"tolerance_ms"`
	ClassExpect      map[string][]string `json:"class_expectations"`
	RefusedPhaseNote string              `json:"refused_phase_note"`
	LowerBoundNote   string              `json:"lower_bound_note"`
	ParentCtx        string              `json:"parent_ctx"`
	GateAddr         string              `json:"gate_addr"`
	RedisAddr        string              `json:"redis_addr"`
	RedisTimeoutMS   float64             `json:"redis_timeout_ms"`
	EvidenceDir      string              `json:"evidence_dir"`
	StartedAtUTC     string              `json:"started_at_utc"`
}

// fourShapeOptionSnapshot is one raw Options() readback for a group's client
// plus, for the instrumented clients, the production-equivalence result.
type fourShapeOptionSnapshot struct {
	Group                 string   `json:"group"`
	Shape                 string   `json:"shape"`
	LMS                   float64  `json:"l_ms"`
	ClientRole            string   `json:"client_role"`
	Addr                  string   `json:"addr"`
	ContextTimeoutEnabled bool     `json:"context_timeout_enabled"`
	MaxRetriesEffective   int      `json:"max_retries_effective"`
	PoolSize              int      `json:"pool_size"`
	DialTimeoutMS         float64  `json:"dial_timeout_ms"`
	ReadTimeoutMS         float64  `json:"read_timeout_ms"`
	WriteTimeoutMS        float64  `json:"write_timeout_ms"`
	ProductionPass        bool     `json:"production_assert_pass"`
	SentinelRestored      bool     `json:"max_retries_sentinel_restored"`
	EquivalenceApplicable bool     `json:"equivalence_applicable"`
	EquivalenceMethod     string   `json:"equivalence_method,omitempty"`
	EquivalenceDiffs      []string `json:"equivalence_diffs,omitempty"`
	EquivalenceDeepEqual  bool     `json:"equivalence_deep_equal"`
	EquivalencePass       bool     `json:"equivalence_pass"`
	ExcludedFields        []string `json:"excluded_fields,omitempty"`
	ExcludedNonNil        []string `json:"excluded_fields_non_nil,omitempty"`
	Notes                 string   `json:"notes,omitempty"`
}

// fourShapeAssertionFlags is the per-sample assertion outcome.
type fourShapeAssertionFlags struct {
	ErrorNonNil    bool `json:"error_non_nil"`
	ErrUnavailable bool `json:"err_unavailable"`
	DurationUpper  bool `json:"duration_upper"`
	DurationLower  bool `json:"duration_lower"`
	ClassExpected  bool `json:"class_expected"`
	// DialAttemptsHit: refused phase-1 samples must have started >= 1 dial
	// attempt in-window (the budget expired inside the detached pool dial
	// retry loop). Phase-2 samples answer from the cached dial error without
	// dialing, so it holds by construction there (raw counts are recorded).
	// Shapes without a dial-stage guard keep true (no such assertion).
	DialAttemptsHit bool `json:"dial_attempts_hit"`
	GateAccepts     bool `json:"gate_accepts"`
	GateEvalFrames  bool `json:"gate_eval_frames"`
	AllPass         bool `json:"all_pass"`
}

// fourShapeSample is one measured Allow call.
type fourShapeSample struct {
	Round               int                     `json:"round"`
	LMS                 float64                 `json:"l_ms"`
	Shape               string                  `json:"shape"`
	Sample              int                     `json:"sample"`
	DurationMS          float64                 `json:"duration_ms"`
	OK                  bool                    `json:"ok"`
	ErrorClass          string                  `json:"error_class"`
	RefusedPhase        string                  `json:"refused_phase,omitempty"`
	ErrorText           string                  `json:"error_text"`
	DialAttempts        uint64                  `json:"dial_attempts"`
	GateAcceptsDelta    uint64                  `json:"gate_accepts_delta"`
	GateEvalFramesDelta uint64                  `json:"gate_eval_frames_delta"`
	Assertions          fourShapeAssertionFlags `json:"assertions"`
}

// fourShapeGroupRecord is the per-group summary (8 groups).
type fourShapeGroupRecord struct {
	Shape             string         `json:"shape"`
	LMS               float64        `json:"l_ms"`
	RedisTimeoutMS    float64        `json:"redis_timeout_ms"`
	N                 int            `json:"n"`
	AllPass           int            `json:"samples_all_assertions_pass"`
	BoundPass         bool           `json:"bound_pass"`
	ErrorNonNilPass   int            `json:"samples_error_non_nil_pass"`
	UnavailablePass   int            `json:"samples_err_unavailable_pass"`
	DurationUpperPass int            `json:"samples_duration_upper_pass"`
	DurationLowerPass int            `json:"samples_duration_lower_pass"`
	ClassPass         int            `json:"samples_class_pass"`
	GateAcceptsPass   int            `json:"samples_gate_accepts_pass"`
	EvalFramesPass    int            `json:"samples_eval_frames_pass"`
	MinMS             float64        `json:"min_ms"`
	P50MS             float64        `json:"p50_ms"`
	MaxMS             float64        `json:"max_ms"`
	Classes           map[string]int `json:"error_classes"`
	DialAttemptsTotal uint64         `json:"dial_attempts_total"`
	// Refused-shape phase split and fault-stage guard (zero/absent for the
	// other shapes). RefusedFaultStageExercised is asserted strictly: the
	// refused dial stage must have been dialed at least once.
	RefusedFaultStageExercised bool `json:"refused_fault_stage_exercised"`
	RefusedBudgetExpirySamples int  `json:"refused_budget_expiry_samples"`
	RefusedFastPathSamples     int  `json:"refused_fast_path_samples"`
	DialAttemptsHitPass        int  `json:"samples_dial_attempts_hit_pass"`
	// Background dial observation（非协程退出证明）: the counter is read once
	// at the last caller return and once ~300ms later; an increment shows the
	// pool's detached dial work may continue after the caller is gone. It is
	// an observable, NOT goroutine-exit proof.
	DialAttemptsAtLastReturn uint64 `json:"dial_attempts_at_last_return"`
	DialAttemptsAfterReturn  uint64 `json:"dial_attempts_after_return_300ms"`
	BackgroundDialObserved   bool   `json:"background_dial_increment_observed"`
	BackgroundDialNote       string `json:"background_dial_note,omitempty"`
}

// fourShapeRecords is the whole-run evidence document.
type fourShapeRecords struct {
	Params          fourShapeParams           `json:"params"`
	OptionSnapshots []fourShapeOptionSnapshot `json:"option_snapshots"`
	Samples         []fourShapeSample         `json:"samples"`
	Groups          []fourShapeGroupRecord    `json:"groups"`
}

// --- test -------------------------------------------------------------------

// TestRatelimitBudgetFourShapeAcceptance drives the §7 four-shape matrix at
// the assembly layer: every group's limiter is the production construction,
// every sample is one `limiter.Allow`, and every failure must be a bounded
// ErrUnavailable decision. Assertions are never relaxed; actual durations,
// error texts, dial attempts and gate counter deltas are recorded per sample.
func TestRatelimitBudgetFourShapeAcceptance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	redisCtr := e2eStartRedis(t)
	gate, err := testutil.NewRedisGate(redisCtr.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	t.Logf("four-shape evidence dir: %s", writer.Dir())

	records := fourShapeRecords{
		Params: fourShapeParams{
			Test:            "TestRatelimitBudgetFourShapeAcceptance",
			Shapes:          fourShapeNames,
			BudgetsMS:       []float64{fourShapeMS(fourShapeBudgets[0]), fourShapeMS(fourShapeBudgets[1])},
			Rounds:          fourShapeRounds,
			SamplesPerRound: fourShapeSamplesPerRound,
			SamplesPerGroup: fourShapeRounds * fourShapeSamplesPerRound,
			TotalSamples:    len(fourShapeNames) * len(fourShapeBudgets) * fourShapeRounds * fourShapeSamplesPerRound,
			ToleranceMS:     fourShapeMS(fourShapeTolerance),
			ClassExpect: map[string][]string{
				fourShapeRefused:       fourShapeExpectedClasses(fourShapeRefused),
				fourShapeDialBlackhole: fourShapeExpectedClasses(fourShapeDialBlackhole),
				fourShapeColdInit:      fourShapeExpectedClasses(fourShapeColdInit),
				fourShapeHotBlocked:    fourShapeExpectedClasses(fourShapeHotBlocked),
			},
			RefusedPhaseNote: fourShapeRefusedPhaseNote,
			LowerBoundNote:   "duration >= L/2 asserted for dial_blackhole/cold_init/hot_blocked (guards a spurious instant failure); refused has no lower bound",
			ParentCtx:        "context.Background with a 10m test deadline, so min(parent, L) = L for every sample",
			GateAddr:         gate.HostPort(),
			RedisAddr:        redisCtr.HostPort(),
			RedisTimeoutMS:   fourShapeMS(config.DefaultRedisTimeout),
			EvidenceDir:      writer.Dir(),
			StartedAtUTC:     time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	written := false
	defer func() {
		if written {
			return
		}
		if err := writer.WriteJSON("fourshape_records.json", records); err != nil {
			t.Errorf("write fourshape_records (failure path): %v", err)
		}
	}()

	for _, shape := range fourShapeNames {
		for _, L := range fourShapeBudgets {
			fourShapeRunGroup(t, ctx, gate, shape, L, &records)
		}
	}

	if want := records.Params.TotalSamples; len(records.Samples) != want {
		t.Errorf("recorded samples = %d, want %d", len(records.Samples), want)
	}
	written = true
	if err := writer.WriteJSON("fourshape_records.json", records); err != nil {
		t.Fatalf("write fourshape_records: %v", err)
	}
}

// --- group runner -----------------------------------------------------------

// fourShapeRig is one client + limiter pair used by a group.
type fourShapeRig struct {
	client  *redis.Client
	limiter *ratelimit.Limiter
}

// fourShapeRunGroup measures one (shape, L) group: 2 rounds × 16 samples on
// ONE client for refused/dial_blackhole/hot_blocked (cold_init rebuilds a
// fresh client per sample), with the gate posture switched per shape.
func fourShapeRunGroup(t *testing.T, ctx context.Context, gate *testutil.RedisGate, shape string, L time.Duration, records *fourShapeRecords) {
	t.Helper()
	label := fmt.Sprintf("%s/L=%s", shape, L)

	env := e2eBaseEnv(
		"postgres://u:p@127.0.0.1:5432/db?sslmode=disable",
		"http://127.0.0.1:1",
		e2eFreeAddr(t),
		[]string{"127.0.0.1:9092"},
	)
	env["TXHARBOR_REDIS_ADDR"] = gate.HostPort()
	env[config.EnvRateLimitBudget] = L.String()
	cfg, err := config.Load(e2eGetenv(env))
	if err != nil {
		t.Fatalf("group %s: config.Load: %v", label, err)
	}
	if cfg.RateLimit.Budget != L {
		t.Fatalf("group %s: cfg.RateLimit.Budget = %v, want %v", label, cfg.RateLimit.Budget, L)
	}
	cfg.Redis.Addr = gate.HostPort()

	var (
		rig      *fourShapeRig
		attempts *atomic.Uint64
	)

	switch shape {
	case fourShapeRefused:
		if err := gate.SetMode(testutil.GateDown); err != nil {
			t.Fatalf("group %s: gate down: %v", label, err)
		}
		defer fourShapeRestorePass(t, gate, label)
		counter := &atomic.Uint64{}
		attempts = counter
		client, snap := fourShapeInstrumentedClient(t, cfg, label, func(defaultDial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
			return func(dctx context.Context, network, addr string) (net.Conn, error) {
				counter.Add(1)
				return defaultDial(dctx, network, addr)
			}
		})
		rig = fourShapeRigFromClient(t, cfg, client, label)
		records.OptionSnapshots = append(records.OptionSnapshots, fourShapeTagSnapshot(snap, label, shape, L))
	case fourShapeDialBlackhole:
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("group %s: gate pass: %v", label, err)
		}
		counter := &atomic.Uint64{}
		attempts = counter
		blackhole := &fourShapeBlackholeDialer{attempts: counter, fallback: fourShapeBlackholeFallback}
		client, snap := fourShapeInstrumentedClient(t, cfg, label, func(func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
			return blackhole.Dial
		})
		rig = fourShapeRigFromClient(t, cfg, client, label)
		records.OptionSnapshots = append(records.OptionSnapshots, fourShapeTagSnapshot(snap, label, shape, L))
	case fourShapeColdInit:
		if err := gate.SetMode(testutil.GateHold); err != nil {
			t.Fatalf("group %s: gate hold: %v", label, err)
		}
		defer fourShapeRestorePass(t, gate, label)
	case fourShapeHotBlocked:
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("group %s: gate pass: %v", label, err)
		}
		rig = fourShapeProductionRig(t, cfg, label)
		snap := fourShapeAssertOptions(t, rig.client.Options(), cfg, label, "production_group")
		snap.Notes = fourShapeProductionNoEquivalenceNote
		records.OptionSnapshots = append(records.OptionSnapshots, fourShapeTagSnapshot(snap, label, shape, L))
	default:
		t.Fatalf("group %s: unknown shape %q", label, shape)
	}

	var samples []fourShapeSample
	for round := 1; round <= fourShapeRounds; round++ {
		if shape == fourShapeColdInit && round > 1 {
			// Drain between rounds: Pass closes the held pairs and reopens
			// the listener, then Hold re-arms it for the next round.
			fourShapeRestorePass(t, gate, label)
			if err := gate.SetMode(testutil.GateHold); err != nil {
				t.Fatalf("group %s round %d: gate hold: %v", label, round, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		for i := range fourShapeSamplesPerRound {
			switch shape {
			case fourShapeColdInit:
				client := newLimiterRedisClient(cfg)
				store, err := ratelimit.NewRedisScriptStore(client)
				if err != nil {
					t.Fatalf("group %s round %d sample %02d: NewRedisScriptStore: %v", label, round, i, err)
				}
				limiter, err := buildLimiter(store, cfg, nil)
				if err != nil {
					t.Fatalf("group %s round %d sample %02d: buildLimiter: %v", label, round, i, err)
				}
				snap := fourShapeAssertOptions(t, client.Options(), cfg,
					fmt.Sprintf("%s round %d sample %02d", label, round, i), "production_fresh_per_sample")
				if round == 1 && i == 0 {
					snap.Notes = fourShapeProductionNoEquivalenceNote
					records.OptionSnapshots = append(records.OptionSnapshots, fourShapeTagSnapshot(snap, label, shape, L))
				}
				time.Sleep(10 * time.Millisecond) // settle: this client has no warm path
				samples = append(samples, fourShapeMeasureSample(t, ctx, gate, limiter, nil, label, shape, L, round, i))
				_ = client.Close() // the next sample must start unconnected
			case fourShapeHotBlocked:
				// Warm in Pass so one established pair exists, then Hold
				// converts it: the measured command has a warm conn and no
				// reply path.
				if _, err := rig.limiter.Allow(ctx, ratelimit.ClassQuery); err != nil {
					t.Errorf("group %s round %d sample %02d: warm Allow in GatePass failed: %v", label, round, i, err)
				}
				if err := gate.SetMode(testutil.GateHold); err != nil {
					t.Fatalf("group %s round %d sample %02d: gate hold: %v", label, round, i, err)
				}
				time.Sleep(50 * time.Millisecond)
				samples = append(samples, fourShapeMeasureSample(t, ctx, gate, rig.limiter, nil, label, shape, L, round, i))
				if err := gate.SetMode(testutil.GatePass); err != nil {
					t.Fatalf("group %s round %d sample %02d: gate pass: %v", label, round, i, err)
				}
				time.Sleep(50 * time.Millisecond)
			default:
				samples = append(samples, fourShapeMeasureSample(t, ctx, gate, rig.limiter, attempts, label, shape, L, round, i))
			}
		}
	}

	// Refused-shape background dial observation（非协程退出证明）: read the
	// cumulative dial-attempt counter once at the last caller return and once
	// ~300ms later. An increment shows the pool's detached dial work may keep
	// running after the caller is gone — an observable, NOT goroutine-exit
	// proof (the design keeps those two apart).
	var (
		attemptsAtLastReturn uint64
		attemptsAfterReturn  uint64
		haveReturnObs        bool
	)
	if shape == fourShapeRefused && attempts != nil {
		attemptsAtLastReturn = attempts.Load()
		time.Sleep(300 * time.Millisecond)
		attemptsAfterReturn = attempts.Load()
		haveReturnObs = true
	}

	summary := fourShapeSummarize(shape, L, cfg, samples)
	if shape == fourShapeRefused {
		summary.RefusedFaultStageExercised = summary.DialAttemptsTotal > 0
		if !summary.RefusedFaultStageExercised {
			t.Errorf("group %s: dial_attempts_total = 0, want > 0 (the refused fault stage must be exercised)", label)
		}
	}
	if haveReturnObs {
		summary.DialAttemptsAtLastReturn = attemptsAtLastReturn
		summary.DialAttemptsAfterReturn = attemptsAfterReturn
		summary.BackgroundDialObserved = attemptsAfterReturn > attemptsAtLastReturn
		summary.BackgroundDialNote = fourShapeBackgroundDialNote
	}
	records.Samples = append(records.Samples, samples...)
	records.Groups = append(records.Groups, summary)
	t.Logf("group %s: n=%d all_pass=%d bound_pass=%v duration_upper=%d duration_lower=%d class=%d err_unavailable=%d dial_hit=%d min=%.2fms p50=%.2fms max=%.2fms classes=%v dial_attempts=%d refused_phase1=%d refused_phase2=%d after_return_increment=%d background_observed=%v",
		label, summary.N, summary.AllPass, summary.BoundPass, summary.DurationUpperPass, summary.DurationLowerPass,
		summary.ClassPass, summary.UnavailablePass, summary.DialAttemptsHitPass,
		summary.MinMS, summary.P50MS, summary.MaxMS,
		summary.Classes, summary.DialAttemptsTotal,
		summary.RefusedBudgetExpirySamples, summary.RefusedFastPathSamples,
		summary.DialAttemptsAfterReturn-summary.DialAttemptsAtLastReturn, summary.BackgroundDialObserved)

	// Group teardown (before the deferred Pass restore): close the group
	// client — the serve-side lifecycle closes the dedicated client too — and
	// settle under the CURRENT posture so the library's background dial
	// machinery cannot land a stray connection in the NEXT group's assertion
	// window. The one that matters is the pool's saturation probe (tryDial:
	// retries every 1s until a dial succeeds or the pool closes), which a
	// refused group's client starts once its dial-error counter saturates.
	if rig != nil {
		_ = rig.client.Close()
		time.Sleep(1200 * time.Millisecond)
	}
}

// fourShapeRestorePass returns the gate to Pass and lets tracked pairs drain.
func fourShapeRestorePass(t *testing.T, gate *testutil.RedisGate, label string) {
	t.Helper()
	if err := gate.SetMode(testutil.GatePass); err != nil {
		t.Fatalf("group %s: restore gate pass: %v", label, err)
	}
	time.Sleep(100 * time.Millisecond)
}

// --- rig construction -------------------------------------------------------

// fourShapeProductionRig builds the exact production limiter wiring for a
// group: newLimiterRedisClient + ratelimit.NewRedisScriptStore + buildLimiter.
func fourShapeProductionRig(t *testing.T, cfg *config.Config, label string) *fourShapeRig {
	t.Helper()
	client := newLimiterRedisClient(cfg)
	t.Cleanup(func() { _ = client.Close() })
	return fourShapeRigFromClient(t, cfg, client, label)
}

// fourShapeRigFromClient wraps an already-built client with the production
// store and limiter construction.
func fourShapeRigFromClient(t *testing.T, cfg *config.Config, client *redis.Client, label string) *fourShapeRig {
	t.Helper()
	store, err := ratelimit.NewRedisScriptStore(client)
	if err != nil {
		t.Fatalf("group %s: NewRedisScriptStore: %v", label, err)
	}
	limiter, err := buildLimiter(store, cfg, nil)
	if err != nil {
		t.Fatalf("group %s: buildLimiter: %v", label, err)
	}
	return &fourShapeRig{client: client, limiter: limiter}
}

// --- instrumented clients ---------------------------------------------------

// fourShapeExcludedOptionFields are the Options fields deliberately not
// compared in the equivalence check: Dialer is the replaced field itself and
// PushNotificationProcessor is a per-client instance NewClient installs for
// every client (production included).
var fourShapeExcludedOptionFields = []string{"Dialer", "PushNotificationProcessor"}

// fourShapeInstrumentedClient builds the production dedicated client with ONLY
// the dialer replaced by wrap(defaultDial). It copies the Options of a real
// newLimiterRedisClient result and records an explicit per-field equivalence
// check that every other field stays identical.
func fourShapeInstrumentedClient(
	t *testing.T,
	cfg *config.Config,
	label string,
	wrap func(defaultDial func(ctx context.Context, network, addr string) (net.Conn, error)) func(ctx context.Context, network, addr string) (net.Conn, error),
) (*redis.Client, fourShapeOptionSnapshot) {
	t.Helper()
	base := newLimiterRedisClient(cfg)
	t.Cleanup(func() { _ = base.Close() })
	baseOpts := base.Options()
	defaultDial := baseOpts.Dialer
	if defaultDial == nil {
		t.Fatalf("group %s: production Options().Dialer = nil; the library default dialer must be installed", label)
	}
	opts := *baseOpts
	// NewClient normalizes Options once per construction (options.init). The
	// only normalization that is NOT idempotent is MaxRetries: the raw
	// sentinel -1 ("no retries") reads back as the effective 0, and feeding
	// that post-init 0 into another NewClient re-normalizes it to the library
	// default 3 (re-sends would silently be back on). Restore the production
	// constructor's raw sentinel so the instrumented client's effective
	// readback stays 0; the equivalence check below then proves every other
	// field is identical to production.
	opts.MaxRetries = -1
	opts.Dialer = wrap(defaultDial)
	client := redis.NewClient(&opts)
	snap := fourShapeAssertOptions(t, client.Options(), cfg, label, "instrumented_production_options_dialer_replaced")
	snap.SentinelRestored = true
	snap.EquivalenceApplicable = true
	snap.Notes = "Options copied from a real newLimiterRedisClient result; raw MaxRetries sentinel -1 restored (post-init effective 0 is not idempotent under options.init: 0 would re-normalize to the library default 3)"
	diffs := fourShapeOptionDiffs(baseOpts, client.Options())
	deepEqual := fourShapeOptionDeepEqual(baseOpts, client.Options())
	snap.EquivalenceMethod = "per-field reflect.DeepEqual across every exported redis.Options field (Dialer and PushNotificationProcessor excluded) plus whole-struct DeepEqual with those two fields nil'd on both sides"
	snap.EquivalenceDiffs = diffs
	snap.EquivalenceDeepEqual = deepEqual
	snap.EquivalencePass = len(diffs) == 0 && deepEqual
	snap.ExcludedFields = append([]string(nil), fourShapeExcludedOptionFields...)
	snap.ExcludedNonNil = []string{
		fmt.Sprintf("Dialer: production=%v instrumented=%v", baseOpts.Dialer != nil, client.Options().Dialer != nil),
		fmt.Sprintf("PushNotificationProcessor: production=%v instrumented=%v", baseOpts.PushNotificationProcessor != nil, client.Options().PushNotificationProcessor != nil),
	}
	if !snap.EquivalencePass {
		t.Errorf("group %s: instrumented client options diverge from the production construction (deep_equal=%v): %v", label, deepEqual, diffs)
	}
	return client, snap
}

// fourShapeAssertOptions asserts the production option wall on one client's
// raw Options() readback and returns the snapshot row.
func fourShapeAssertOptions(t *testing.T, opts *redis.Options, cfg *config.Config, label, role string) fourShapeOptionSnapshot {
	t.Helper()
	snap := fourShapeOptionSnapshot{
		ClientRole:            role,
		Addr:                  opts.Addr,
		ContextTimeoutEnabled: opts.ContextTimeoutEnabled,
		MaxRetriesEffective:   opts.MaxRetries,
		PoolSize:              opts.PoolSize,
		DialTimeoutMS:         fourShapeMS(opts.DialTimeout),
		ReadTimeoutMS:         fourShapeMS(opts.ReadTimeout),
		WriteTimeoutMS:        fourShapeMS(opts.WriteTimeout),
	}
	pass := true
	fail := func(format string, args ...any) {
		pass = false
		t.Errorf("%s (%s): %s", label, role, fmt.Sprintf(format, args...))
	}
	if opts.Addr != cfg.Redis.Addr {
		fail("Addr = %q, want cfg.Redis.Addr = %q", opts.Addr, cfg.Redis.Addr)
	}
	if !opts.ContextTimeoutEnabled {
		fail("ContextTimeoutEnabled = false, want true")
	}
	if opts.MaxRetries != 0 {
		fail("MaxRetries = %d, want the effective 0 (constructed as -1: no re-send)", opts.MaxRetries)
	}
	if opts.PoolSize != limiterPoolSize {
		fail("PoolSize = %d, want %d (limiterPoolSize)", opts.PoolSize, limiterPoolSize)
	}
	if opts.DialTimeout != cfg.Redis.Timeout {
		fail("DialTimeout = %v, want cfg.Redis.Timeout = %v", opts.DialTimeout, cfg.Redis.Timeout)
	}
	if opts.ReadTimeout != cfg.Redis.Timeout {
		fail("ReadTimeout = %v, want cfg.Redis.Timeout = %v", opts.ReadTimeout, cfg.Redis.Timeout)
	}
	if opts.WriteTimeout != cfg.Redis.Timeout {
		fail("WriteTimeout = %v, want cfg.Redis.Timeout = %v", opts.WriteTimeout, cfg.Redis.Timeout)
	}
	snap.ProductionPass = pass
	return snap
}

// fourShapeOptionDiffs is the explicit per-field comparison of two Options
// structs, skipping the two excluded fields.
func fourShapeOptionDiffs(a, b *redis.Options) []string {
	av, bv := reflect.ValueOf(*a), reflect.ValueOf(*b)
	typ := av.Type()
	var diffs []string
	for i := range av.NumField() {
		field := typ.Field(i)
		if field.PkgPath != "" { // unexported
			continue
		}
		if fourShapeContains(fourShapeExcludedOptionFields, field.Name) {
			continue
		}
		ai, bi := av.Field(i).Interface(), bv.Field(i).Interface()
		if !reflect.DeepEqual(ai, bi) {
			diffs = append(diffs, fmt.Sprintf("%s: %+v != %+v", field.Name, ai, bi))
		}
	}
	return diffs
}

// fourShapeOptionDeepEqual is the whole-struct comparison with the two
// excluded fields nil'd on both sides (function fields make DeepEqual false
// otherwise).
func fourShapeOptionDeepEqual(a, b *redis.Options) bool {
	x, y := *a, *b
	x.Dialer, y.Dialer = nil, nil
	x.PushNotificationProcessor, y.PushNotificationProcessor = nil, nil
	return reflect.DeepEqual(&x, &y)
}

// fourShapeTagSnapshot fills the group coordinates into one snapshot row.
func fourShapeTagSnapshot(snap fourShapeOptionSnapshot, group, shape string, L time.Duration) fourShapeOptionSnapshot {
	snap.Group = group
	snap.Shape = shape
	snap.LMS = fourShapeMS(L)
	return snap
}

// --- dialers ----------------------------------------------------------------

// fourShapeBlackholeDialer is the shape-② injection: it never opens a socket;
// it blocks until the attempt context is done, then returns a dial-shaped
// error wrapping the context cause with the blackhole marker (mirrors the
// experiment blackholeDialer in internal/ratelimit/redislatency_ab_test.go).
type fourShapeBlackholeDialer struct {
	attempts *atomic.Uint64
	fallback time.Duration
}

func (d *fourShapeBlackholeDialer) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	d.attempts.Add(1)
	deadline := time.Now().Add(d.fallback)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, fourShapeBlackholeErr(network, ctx.Err())
	case <-timer.C:
		if err := ctx.Err(); err != nil {
			return nil, fourShapeBlackholeErr(network, err)
		}
		return nil, &net.OpError{Op: "dial", Net: network, Err: fourShapeBlackholeTimeoutMarker{}}
	}
}

// fourShapeBlackholeErr renders the ctx-cancelled blackhole shape.
func fourShapeBlackholeErr(network string, err error) error {
	return &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("%s: %w", fourShapeBlackholeMarker, err)}
}

// fourShapeBlackholeTimeoutMarker mirrors the experiment's fallback dial
// timeout marker (kept for shape parity; the attempt context deadline fires
// first in this matrix).
type fourShapeBlackholeTimeoutMarker struct{}

func (fourShapeBlackholeTimeoutMarker) Error() string {
	return "dial timeout (experiment blackhole marker)"
}
func (fourShapeBlackholeTimeoutMarker) Timeout() bool { return true }

// --- classification ---------------------------------------------------------

// fourShapeClassify buckets the caller-visible error text. Order matters: a
// budget-exhausted decision can carry several joined causes.
func fourShapeClassify(text string) string {
	switch {
	case text == "":
		return "ok"
	case strings.Contains(text, "connection refused"):
		return "refused"
	case strings.Contains(text, fourShapeBlackholeMarker):
		return "blackhole"
	case strings.Contains(text, "i/o timeout"), strings.Contains(text, "context deadline exceeded"):
		return "timeout"
	default:
		return "other"
	}
}

// fourShapeExpectedClasses is the per-shape expected error class set.
//
// Refused (shape ①) is TWO-PHASED on go-redis v9.22.0 because the library
// retries dials at the POOL level, detached from the command context, in
// addition to the command-level retry loop MaxRetries=-1 disables:
//
//   - phase ① "budget_expiry_dial_retrying" (class timeout): queuedNewConn
//     runs the dial work on `context.WithCancel(context.Background())`
//     (internal/pool/pool.go:1059), so the caller's budget aborts only the
//     WAIT — the outer select returns ctx.Err() while dialConn
//     (pool.go:687) keeps retrying, up to DialerRetries (library default 5,
//     pool.go:707) attempts with the DialerRetryTimeout backoff (default
//     100ms, pool.go:782; ≈0.4-0.5s) — far beyond L=200/300ms. The
//     caller-visible identity is the budget deadline, not the refused dial
//     error. MaxRetries=-1 only disables command-level retries; the
//     production constructor never touches DialerRetries.
//   - phase ② "dial_error_fast_path" (class refused): once the pool's
//     dial-error counter saturates, dialConn short-circuits to the cached
//     last dial error (`dialErrorsNum >= PoolSize` → getLastDialError(),
//     pool.go:692; bookkeeping pool.go:765; the background tryDial probe
//     resets the counter only on a SUCCESSFUL dial, pool.go:806-830), so
//     later decisions return "connect: connection refused" in microseconds.
//
// Both identities are ErrUnavailable decisions and both are accepted here:
// the class set records the OBSERVED v9.22.0 pool-dial mechanism (raw
// error_text plus a per-sample `refused_phase` are recorded); it does not
// weaken any timing or fault-hit bound.
func fourShapeExpectedClasses(shape string) []string {
	switch shape {
	case fourShapeRefused:
		return []string{"refused", "timeout"}
	case fourShapeDialBlackhole:
		return []string{"blackhole", "timeout"}
	default:
		return []string{"timeout"}
	}
}

// fourShapeRefusedPhase labels one refused-shape sample with the mechanism
// phase its caller-visible class identifies (see fourShapeExpectedClasses).
func fourShapeRefusedPhase(class string) string {
	switch class {
	case "timeout":
		return fourShapePhaseBudgetExpiry
	case "refused":
		return fourShapePhaseFastPath
	default:
		return ""
	}
}

// --- measurement ------------------------------------------------------------

// fourShapeMeasureSample runs one Allow under the injected posture, asserts
// every per-sample bound and returns the evidence row.
func fourShapeMeasureSample(
	t *testing.T,
	ctx context.Context,
	gate *testutil.RedisGate,
	limiter *ratelimit.Limiter,
	attempts *atomic.Uint64,
	label, shape string,
	L time.Duration,
	round, sample int,
) fourShapeSample {
	t.Helper()
	before := gate.CounterSnapshot()
	var attemptsBefore uint64
	if attempts != nil {
		attemptsBefore = attempts.Load()
	}
	start := time.Now()
	_, allowErr := limiter.Allow(ctx, ratelimit.ClassQuery)
	dur := time.Since(start)
	after := gate.CounterSnapshot()
	var attemptsDelta uint64
	if attempts != nil {
		if now := attempts.Load(); now >= attemptsBefore {
			attemptsDelta = now - attemptsBefore
		}
	}

	text := fourShapeErrText(allowErr)
	class := fourShapeClassify(text)
	phase := ""
	if shape == fourShapeRefused {
		phase = fourShapeRefusedPhase(class)
	}
	rec := fourShapeSample{
		Round:               round,
		LMS:                 fourShapeMS(L),
		Shape:               shape,
		Sample:              sample,
		DurationMS:          fourShapeMS(dur),
		OK:                  allowErr == nil,
		ErrorClass:          class,
		RefusedPhase:        phase,
		ErrorText:           text,
		DialAttempts:        attemptsDelta,
		GateAcceptsDelta:    after.Accepts - before.Accepts,
		GateEvalFramesDelta: after.EvalFrames - before.EvalFrames,
	}

	pre := fmt.Sprintf("group %s round %d sample %02d", label, round, sample)

	nonNil := allowErr != nil
	if !nonNil {
		t.Errorf("%s: Allow error = nil, want a failure under the injected fault", pre)
	}
	unavailable := nonNil && errors.Is(allowErr, ratelimit.ErrUnavailable)
	if nonNil && !unavailable {
		t.Errorf("%s: errors.Is(err, ratelimit.ErrUnavailable) = false; err = %q", pre, text)
	}
	upper := dur <= L+fourShapeTolerance
	if !upper {
		t.Errorf("%s: duration %v > L+tolerance = %v (shape %s must return inside the budget envelope)", pre, dur, L+fourShapeTolerance, shape)
	}
	lower := true
	if shape != fourShapeRefused {
		lower = dur >= L/2
		if !lower {
			t.Errorf("%s: duration %v < L/2 = %v (spurious instant failure for shape %s)", pre, dur, L/2, shape)
		}
	}
	expected := fourShapeExpectedClasses(shape)
	classOK := fourShapeContains(expected, class)
	if !classOK {
		t.Errorf("%s: error class %q not in expected %v for shape %s (dial_attempts=%d, duration=%.2fms); err = %q",
			pre, class, expected, shape, attemptsDelta, fourShapeMS(dur), text)
	}
	// Fault-hit guard: a refused phase-1 sample's budget expired INSIDE the
	// detached pool dial retry loop, so at least one dial attempt must have
	// been started in-window. Phase-2 samples answer from the cached dial
	// error without dialing (raw counts are still recorded; they may catch
	// still-draining background attempts), and the group-level
	// fault_stage_exercised guard covers the stage.
	dialHit := true
	if shape == fourShapeRefused && phase == fourShapePhaseBudgetExpiry {
		dialHit = attemptsDelta >= 1
		if !dialHit {
			t.Errorf("%s: refused phase-1 dial_attempts = %d, want >= 1 (budget expired inside the detached pool dial retry loop)", pre, attemptsDelta)
		}
	}
	acceptsOK := true
	switch shape {
	case fourShapeRefused, fourShapeDialBlackhole:
		acceptsOK = rec.GateAcceptsDelta == 0
		if !acceptsOK {
			t.Errorf("%s: gate accepts delta = %d, want 0 (shape %s never reaches the gate)", pre, rec.GateAcceptsDelta, shape)
		}
	case fourShapeColdInit:
		acceptsOK = rec.GateAcceptsDelta >= 1
		if !acceptsOK {
			t.Errorf("%s: gate accepts delta = %d, want >= 1 (the cold dial must have reached the gate)", pre, rec.GateAcceptsDelta)
		}
	}
	evalsOK := true
	switch shape {
	case fourShapeRefused, fourShapeColdInit:
		evalsOK = rec.GateEvalFramesDelta == 0
		if !evalsOK {
			t.Errorf("%s: gate EVAL frames delta = %d, want 0 (nothing may be forwarded to the backend)", pre, rec.GateEvalFramesDelta)
		}
	}
	rec.Assertions = fourShapeAssertionFlags{
		ErrorNonNil:     nonNil,
		ErrUnavailable:  unavailable,
		DurationUpper:   upper,
		DurationLower:   lower,
		ClassExpected:   classOK,
		DialAttemptsHit: dialHit,
		GateAccepts:     acceptsOK,
		GateEvalFrames:  evalsOK,
		AllPass:         nonNil && unavailable && upper && lower && classOK && dialHit && acceptsOK && evalsOK,
	}
	return rec
}

// fourShapeSummarize aggregates one group's samples.
func fourShapeSummarize(shape string, L time.Duration, cfg *config.Config, samples []fourShapeSample) fourShapeGroupRecord {
	rec := fourShapeGroupRecord{
		Shape:          shape,
		LMS:            fourShapeMS(L),
		RedisTimeoutMS: fourShapeMS(cfg.Redis.Timeout),
		N:              len(samples),
		Classes:        map[string]int{},
	}
	durs := make([]float64, 0, len(samples))
	for _, s := range samples {
		durs = append(durs, s.DurationMS)
		rec.Classes[s.ErrorClass]++
		rec.DialAttemptsTotal += s.DialAttempts
		switch s.RefusedPhase {
		case fourShapePhaseBudgetExpiry:
			rec.RefusedBudgetExpirySamples++
		case fourShapePhaseFastPath:
			rec.RefusedFastPathSamples++
		}
		if s.Assertions.AllPass {
			rec.AllPass++
		}
		if s.Assertions.ErrorNonNil {
			rec.ErrorNonNilPass++
		}
		if s.Assertions.ErrUnavailable {
			rec.UnavailablePass++
		}
		if s.Assertions.DurationUpper {
			rec.DurationUpperPass++
		}
		if s.Assertions.DurationLower {
			rec.DurationLowerPass++
		}
		if s.Assertions.ClassExpected {
			rec.ClassPass++
		}
		if s.Assertions.DialAttemptsHit {
			rec.DialAttemptsHitPass++
		}
		if s.Assertions.GateAccepts {
			rec.GateAcceptsPass++
		}
		if s.Assertions.GateEvalFrames {
			rec.EvalFramesPass++
		}
	}
	sort.Float64s(durs)
	if len(durs) > 0 {
		rec.MinMS = durs[0]
		rec.P50MS = durs[len(durs)/2]
		rec.MaxMS = durs[len(durs)-1]
	}
	rec.BoundPass = rec.AllPass == rec.N
	return rec
}

// --- small helpers ----------------------------------------------------------

// fourShapeMS renders a duration in milliseconds with microsecond precision.
func fourShapeMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// fourShapeErrText renders an error for an evidence record ("" when nil).
func fourShapeErrText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// fourShapeContains reports whether want contains s.
func fourShapeContains(want []string, s string) bool {
	for _, w := range want {
		if w == s {
			return true
		}
	}
	return false
}
