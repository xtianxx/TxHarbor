// budget.go implements T008: bounded-resource, backoff, and cancellation
// primitives for 014 reconciliation (FR-019, Q3; contracts/task-lifecycle.md
// Integrity Rules).
//
// Every scan invocation runs under a Budget: concurrency, per-claim range span,
// wall-clock duration, and PG/RPC request quotas are all hard upper bounds.
// Exhausting any quota stops new work, marks the budget suspended with an
// observable reason, and lets the caller persist the suspended state plus gap
// rows. There is no busy loop anywhere: waiting is either a channel/timer wait
// or a caller-driven Wait on an external wake signal, and retries go through
// BoundedBackoff, which caps attempts and delay.
//
// Cancellation is cooperative and context-native: every gate checks ctx, and
// Canceler bridges an out-of-band pause/cancel signal into a context so
// in-flight work can settle boundedly (Canceler.Settle).
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

var (
	// ErrBudgetExhausted is the sentinel matched (via errors.Is) by every
	// bounded-resource refusal. Callers stop claiming new work on it.
	ErrBudgetExhausted = errors.New("reconciliation budget exhausted")

	// ErrInvalidBudget marks malformed limit configuration. Budgets are never
	// created with unlimited values: an invalid configuration fails closed.
	ErrInvalidBudget = errors.New("invalid reconciliation budget")
)

// BudgetResource is the closed quota vocabulary.
type BudgetResource string

// The bounded resources of one scan invocation.
const (
	// ResourceConcurrency bounds parallel in-flight work units.
	ResourceConcurrency BudgetResource = "concurrency"
	// ResourceSpanPerClaim bounds the scope units (blocks or microseconds) of
	// one claimed interval.
	ResourceSpanPerClaim BudgetResource = "range_span"
	// ResourceDuration bounds wall-clock execution of one invocation.
	ResourceDuration BudgetResource = "duration"
	// ResourcePG bounds PostgreSQL requests issued by the invocation.
	ResourcePG BudgetResource = "pg_requests"
	// ResourceRPC bounds chain RPC requests issued by the invocation.
	ResourceRPC BudgetResource = "rpc_requests"
)

// Valid reports whether r belongs to the closed resource vocabulary.
func (r BudgetResource) Valid() bool {
	switch r {
	case ResourceConcurrency, ResourceSpanPerClaim, ResourceDuration, ResourcePG, ResourceRPC:
		return true
	default:
		return false
	}
}

// BudgetLimits are the hard upper bounds for one scan invocation. Every field
// MUST be positive: unbounded is not representable (FR-019). Concrete values
// are deployment/test parameters (research §7); this package never invents
// production thresholds.
type BudgetLimits struct {
	// MaxConcurrency is the maximum number of parallel in-flight work units.
	MaxConcurrency int
	// MaxSpanPerClaim is the maximum scope units (blocks or unix microseconds,
	// matching the task scope kind) of one claimed interval.
	MaxSpanPerClaim int64
	// MaxDuration is the wall-clock ceiling of one invocation.
	MaxDuration time.Duration
	// MaxPGRequests is the PostgreSQL request quota of one invocation.
	MaxPGRequests int
	// MaxRPCRequests is the chain RPC request quota of one invocation.
	MaxRPCRequests int
}

// Validate fails closed on any non-positive field.
func (l BudgetLimits) Validate() error {
	var problems []string
	if l.MaxConcurrency <= 0 {
		problems = append(problems, "max concurrency must be positive")
	}
	if l.MaxSpanPerClaim <= 0 {
		problems = append(problems, "max span per claim must be positive")
	}
	if l.MaxDuration <= 0 {
		problems = append(problems, "max duration must be positive")
	}
	if l.MaxPGRequests <= 0 {
		problems = append(problems, "max PG requests must be positive")
	}
	if l.MaxRPCRequests <= 0 {
		problems = append(problems, "max RPC requests must be positive")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidBudget, strings.Join(problems, "; "))
	}
	return nil
}

// ClampSpan clamps a requested interval span to the per-claim bound. A
// non-positive request or an invalid limit yields 0 (callers treat 0 as "no
// interval to claim").
func (l BudgetLimits) ClampSpan(span int64) int64 {
	if span <= 0 || l.MaxSpanPerClaim <= 0 {
		return 0
	}
	if span > l.MaxSpanPerClaim {
		return l.MaxSpanPerClaim
	}
	return span
}

// BudgetLimitError is the typed refusal of a bounded resource. It always
// matches ErrBudgetExhausted through errors.Is, so callers can stop new work
// without inspecting the resource.
type BudgetLimitError struct {
	Resource BudgetResource
	Limit    int64
	Used     int64
	Detail   string
}

// Error renders the refusal without secret material.
func (e *BudgetLimitError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (resource=%s limit=%d used=%d)", ErrBudgetExhausted, e.Detail, e.Resource, e.Limit, e.Used)
	}
	return fmt.Sprintf("%s: resource=%s limit=%d used=%d", ErrBudgetExhausted, e.Resource, e.Limit, e.Used)
}

// Is makes every BudgetLimitError match ErrBudgetExhausted.
func (e *BudgetLimitError) Is(target error) bool { return target == ErrBudgetExhausted }

// ScanQueryBudget is the query-by-query accounting seam of the scan seams
// (enumeration, window resolution; T038). Every internal read of a seam is
// either charged through Consume* as it executes, or covered by a proven
// statement cap declared by the seam. Charging happens during execution:
// after-the-fact accounting is never accepted. *Budget satisfies the
// interface; a seam returning ErrBudgetExhausted from a charge aborts
// boundedly (gap + honest checkpoint, never a silent drop).
type ScanQueryBudget interface {
	ConsumePG(ctx context.Context, requests int) error
	ConsumeRPC(ctx context.Context, requests int) error
}

// QueryStatementCap is a proven per-call upper bound of a seam's internal
// queries. A seam that cannot charge query-by-query declares its cap and the
// caller charges the worst case before the seam executes (conservative, never
// an after-the-fact top-up).
type QueryStatementCap struct {
	PG  int
	RPC int
}

// Validate fails closed on a malformed cap: both bounds must be non-negative
// and at least one must be positive.
func (c QueryStatementCap) Validate() error {
	if c.PG < 0 || c.RPC < 0 {
		return fmt.Errorf("%w: statement cap must be non-negative", ErrInvalidBudget)
	}
	if c.PG == 0 && c.RPC == 0 {
		return fmt.Errorf("%w: statement cap must cover at least one query", ErrInvalidBudget)
	}
	return nil
}

// Charge charges the declared cap against the budget before the seam executes.
func (c QueryStatementCap) Charge(ctx context.Context, budget ScanQueryBudget) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if budget == nil {
		return contractErrorf("query statement cap requires a budget seam")
	}
	if err := budget.ConsumePG(ctx, c.PG); err != nil {
		return err
	}
	return budget.ConsumeRPC(ctx, c.RPC)
}

// ScanStatementCapSource optionally declares the proven internal query cap of
// one candidate source call. It is the alternative to per-query charging for
// seams that cannot thread the budget callback.
type ScanStatementCapSource interface {
	ScanStatementCap() QueryStatementCap
}

// BudgetUsage is an immutable, observable snapshot of one invocation's budget
// state. It is the primitive that makes a wait/suspend state observable instead
// of silent.
type BudgetUsage struct {
	Limits        BudgetLimits
	StartedAt     time.Time
	Deadline      time.Time
	InFlight      int
	PGUsed        int
	RPCUsed       int
	Suspended     BudgetResource
	SuspendDetail string
	SuspendedAt   time.Time
}

// Budget enforces one invocation's bounds. Create it with NewBudget; the zero
// value is not usable.
type Budget struct {
	limits   BudgetLimits
	slots    chan struct{}
	started  time.Time
	deadline time.Time

	mu            sync.Mutex
	pgUsed        int
	rpcUsed       int
	suspended     BudgetResource
	suspendDetail string
	suspendedAt   time.Time
}

// NewBudget validates the limits and starts the invocation clock.
func NewBudget(limits BudgetLimits) (*Budget, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	now := time.Now()
	return &Budget{
		limits:   limits,
		slots:    make(chan struct{}, limits.MaxConcurrency),
		started:  now,
		deadline: now.Add(limits.MaxDuration),
	}, nil
}

// Limits returns the configured bounds.
func (b *Budget) Limits() BudgetLimits { return b.limits }

// Started returns the invocation start instant.
func (b *Budget) Started() time.Time { return b.started }

// Deadline returns the invocation wall-clock deadline.
func (b *Budget) Deadline() time.Time { return b.deadline }

// Acquire reserves one concurrency slot for an in-flight work unit. It blocks
// (channel wait, never a spin) while all slots are busy, returns early when the
// duration quota is reached (marking the budget suspended), and always honors
// ctx cancellation. The returned release MUST be called when the work unit
// finishes; it is idempotent.
func (b *Budget) Acquire(ctx context.Context) (release func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resource, detail, suspended := b.Suspended(); suspended {
		return nil, b.limitError(resource, detail)
	}
	if resource, detail, limited := b.durationGate(); limited {
		return nil, b.limitError(resource, detail)
	}

	timer := time.NewTimer(time.Until(b.deadline))
	defer timer.Stop()
	select {
	case b.slots <- struct{}{}:
		if resource, detail, limited := b.durationGate(); limited {
			<-b.slots
			return nil, b.limitError(resource, detail)
		}
		var once sync.Once
		return func() { once.Do(func() { <-b.slots }) }, nil
	case <-timer.C:
		resource, detail, _ := b.durationGate()
		if resource == "" {
			resource, detail = ResourceDuration, durationLimitDetail
			b.suspend(resource, detail)
		}
		return nil, b.limitError(resource, detail)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ConsumePG charges n PostgreSQL requests against the invocation quota. The
// charge is atomic and refuses without charging when it would exceed the bound
// or when the budget is already suspended.
func (b *Budget) ConsumePG(ctx context.Context, requests int) error {
	return b.consume(ctx, ResourcePG, requests)
}

// ConsumeRPC charges n chain RPC requests against the invocation quota, with
// the same atomic refusal semantics as ConsumePG.
func (b *Budget) ConsumeRPC(ctx context.Context, requests int) error {
	return b.consume(ctx, ResourceRPC, requests)
}

// consume charges one counter quota atomically.
func (b *Budget) consume(ctx context.Context, resource BudgetResource, requests int) error {
	if requests < 0 {
		return fmt.Errorf("%w: negative %s request count %d", ErrInvalidBudget, resource, requests)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if requests == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.suspended != "" {
		return b.limitErrorLocked(b.suspended, b.suspendDetail)
	}
	now := time.Now()
	if !now.Before(b.deadline) {
		b.suspendLocked(ResourceDuration, durationLimitDetail, now)
		return b.limitErrorLocked(ResourceDuration, durationLimitDetail)
	}

	var used *int
	var limit int
	switch resource {
	case ResourcePG:
		used, limit = &b.pgUsed, b.limits.MaxPGRequests
	case ResourceRPC:
		used, limit = &b.rpcUsed, b.limits.MaxRPCRequests
	default:
		return fmt.Errorf("%w: unknown budget resource %q", ErrInvalidBudget, resource)
	}
	if *used >= limit || requests > limit-*used {
		detail := fmt.Sprintf("%s quota reached", resource)
		b.suspendLocked(resource, detail, now)
		return b.limitErrorLocked(resource, detail)
	}
	*used += requests
	return nil
}

// Suspended reports the observable suspension state: the resource that stopped
// new work, a human-readable detail, and whether the budget is suspended. Once
// suspended, the budget stays suspended for the invocation.
func (b *Budget) Suspended() (BudgetResource, string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.suspended == "" {
		return "", "", false
	}
	return b.suspended, b.suspendDetail, true
}

// Usage returns the observable budget snapshot (counters, in-flight work,
// suspension reason, deadline).
func (b *Budget) Usage() BudgetUsage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BudgetUsage{
		Limits:        b.limits,
		StartedAt:     b.started,
		Deadline:      b.deadline,
		InFlight:      len(b.slots),
		PGUsed:        b.pgUsed,
		RPCUsed:       b.rpcUsed,
		Suspended:     b.suspended,
		SuspendDetail: b.suspendDetail,
		SuspendedAt:   b.suspendedAt,
	}
}

// Wait blocks while the budget is suspended until ctx is done or the caller's
// wake channel fires (nil waits only on ctx). It never polls: the wait is a
// single select, so a suspended invocation costs no CPU. A nil error means the
// caller may re-check the gates; the caller drives the loop, this primitive
// does not spin it.
func (b *Budget) Wait(ctx context.Context, wake <-chan struct{}) error {
	if _, _, suspended := b.Suspended(); !suspended {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	}
}

// ClampSpan clamps an interval span to the configured per-claim bound.
func (b *Budget) ClampSpan(span int64) int64 { return b.limits.ClampSpan(span) }

// durationLimitDetail is the stable detail text of a duration exhaustion.
const durationLimitDetail = "scan duration limit reached"

// durationGate reports (and latches) duration exhaustion.
func (b *Budget) durationGate() (BudgetResource, string, bool) {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.suspended != "" {
		return b.suspended, b.suspendDetail, true
	}
	if !now.Before(b.deadline) {
		b.suspendLocked(ResourceDuration, durationLimitDetail, now)
		return ResourceDuration, durationLimitDetail, true
	}
	return "", "", false
}

// suspend latches the first exhaustion reason.
func (b *Budget) suspend(resource BudgetResource, detail string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.suspendLocked(resource, detail, time.Now())
}

// suspendLocked latches the exhaustion reason; the first reason wins.
func (b *Budget) suspendLocked(resource BudgetResource, detail string, now time.Time) {
	if b.suspended != "" {
		return
	}
	b.suspended = resource
	b.suspendDetail = detail
	b.suspendedAt = now
}

// limitError builds the typed refusal, reading current counters.
func (b *Budget) limitError(resource BudgetResource, detail string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limitErrorLocked(resource, detail)
}

// limitErrorLocked builds the typed refusal; callers hold b.mu when needed.
func (b *Budget) limitErrorLocked(resource BudgetResource, detail string) *BudgetLimitError {
	var used, limit int64
	switch resource {
	case ResourceConcurrency:
		used, limit = int64(len(b.slots)), int64(b.limits.MaxConcurrency)
	case ResourceSpanPerClaim:
		limit = b.limits.MaxSpanPerClaim
	case ResourceDuration:
		used, limit = int64(time.Since(b.started)), int64(b.limits.MaxDuration)
	case ResourcePG:
		used, limit = int64(b.pgUsed), int64(b.limits.MaxPGRequests)
	case ResourceRPC:
		used, limit = int64(b.rpcUsed), int64(b.limits.MaxRPCRequests)
	}
	return &BudgetLimitError{Resource: resource, Limit: limit, Used: used, Detail: detail}
}

// Canceler is a cooperative cancellation primitive for in-flight work driven by
// an out-of-band signal (operator pause/cancel, task-state change) rather than
// the caller's own context. Cancel is idempotent; the reason is audit-only.
type Canceler struct {
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	reason string
}

// NewCanceler returns a live canceler.
func NewCanceler() *Canceler {
	return &Canceler{done: make(chan struct{})}
}

// Cancel fires the signal once. Later calls are no-ops; the first reason wins.
func (c *Canceler) Cancel(reason string) {
	c.once.Do(func() {
		c.mu.Lock()
		c.reason = reason
		c.mu.Unlock()
		close(c.done)
	})
}

// Canceled reports whether the signal has fired.
func (c *Canceler) Canceled() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// Reason returns the recorded cancel reason (empty when not canceled).
func (c *Canceler) Reason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// Err returns context.Canceled when the signal has fired, nil otherwise. It is
// convenience for assigning into error-returning paths.
func (c *Canceler) Err() error {
	if c.Canceled() {
		return context.Canceled
	}
	return nil
}

// Context derives a context that is canceled when parent is done or when the
// canceler fires. The returned CancelFunc MUST be called by the caller when the
// derived context is no longer needed (it releases the bridge goroutine).
func (c *Canceler) Context(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-c.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// Settle waits for in-flight work to drain, bounded by grace and by ctx. A
// positive grace arms one timer; grace <= 0 arms no extra timer, so the wait is
// bounded by ctx alone (pass a deadline-bearing ctx for a hard bound). It
// returns true when the work settled within the bound. The wait is a single
// select over a WaitGroup bridge, so it is a bounded wait, not a spin.
func (c *Canceler) Settle(ctx context.Context, inFlight *sync.WaitGroup, grace time.Duration) bool {
	if inFlight == nil {
		return true
	}
	done := make(chan struct{})
	go func() {
		inFlight.Wait()
		close(done)
	}()
	var graceC <-chan time.Time
	if grace > 0 {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		graceC = timer.C
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	case <-graceC:
		return false
	}
}

// BoundedBackoff is a capped exponential retry delay with a hard attempt cap.
// It makes "no unbounded retry" structural: Delay refuses attempts beyond
// MaxAttempts and clamps every delay at Max. It carries no jitter by design so
// behavior stays deterministic in tests.
type BoundedBackoff struct {
	// Initial is the first-attempt delay (positive).
	Initial time.Duration
	// Factor multiplies the delay per attempt (>= 1).
	Factor float64
	// Max caps the delay (>= Initial).
	Max time.Duration
	// MaxAttempts is the hard retry cap (>= 1).
	MaxAttempts int
}

// Validate fails closed on a malformed retry policy.
func (b BoundedBackoff) Validate() error {
	switch {
	case b.Initial <= 0:
		return fmt.Errorf("%w: backoff initial delay must be positive", ErrInvalidBudget)
	case b.Factor < 1:
		return fmt.Errorf("%w: backoff factor must be >= 1", ErrInvalidBudget)
	case b.Max < b.Initial:
		return fmt.Errorf("%w: backoff max delay must be >= initial", ErrInvalidBudget)
	case b.MaxAttempts < 1:
		return fmt.Errorf("%w: backoff max attempts must be >= 1", ErrInvalidBudget)
	}
	return nil
}

// Delay returns the delay for a 1-based attempt and whether that attempt is
// permitted at all. It returns ok=false once attempts are exhausted; the delay
// is clamped at Max so it can never grow unboundedly.
func (b BoundedBackoff) Delay(attempt int) (time.Duration, bool) {
	if attempt < 1 || attempt > b.MaxAttempts {
		return 0, false
	}
	delay := float64(b.Initial) * math.Pow(b.Factor, float64(attempt-1))
	if delay >= float64(b.Max) {
		return b.Max, true
	}
	return time.Duration(delay), true
}

// Wait sleeps the bounded delay for attempt, honoring ctx cancellation. It
// returns the slept delay, whether another attempt may follow, and ctx.Err()
// when canceled. An exhausted attempt budget returns (0, false, nil); there is
// no unbounded wait.
func (b BoundedBackoff) Wait(ctx context.Context, attempt int) (time.Duration, bool, error) {
	if err := b.Validate(); err != nil {
		return 0, false, err
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	delay, ok := b.Delay(attempt)
	if !ok {
		return 0, false, nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return delay, true, nil
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}
