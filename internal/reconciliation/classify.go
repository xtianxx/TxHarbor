// classify.go implements the T017 machine classifier: the machine-readable
// category decision of FR-009 and the "only business divergence creates a
// ticket" boundary of Q4 (data-model.md §1.4/§2, contracts/
// discrepancy-lifecycle.md `Classification (machine)`, quickstart §1–3/8).
//
// Classify is a pure function over one normalized three-way observation
// (chain facts / PostgreSQL business state / event delivery-consumer results)
// plus its scan coverage, freshness and per-business-type upstream receipt
// source state. It performs no I/O and never reads the clock: the caller
// injects both the evidence time and the classification time.
//
// Hard rules implemented here:
//
//   - Only business divergence creates a ticket (Q4). An at-least-once
//     duplicate proven idempotently absorbed with zero business divergence
//     yields no category and no ticket; it is a metrics/audit signal only.
//     A delivery count alone NEVER indicates a fund anomaly: divergence must
//     be evidenced by content, business effect, repeated withdrawal intent,
//     or a version-rule violation.
//   - Insufficient evidence (unclosed scan, open gaps, stale or trimmed
//     evidence, unreadable parties, orphaned blocks, unproven duplicate
//     absorption, unknown shapes) classifies as incomplete/unknown and stays
//     pending; it is never rendered as consistent (FR-004/005, Edge).
//   - The upstream receipt source is an input only. An unconnected or
//     configured-but-unavailable source can never support a consistent
//     conclusion, and a connected flag alone never proves upstream success
//     (FR-006): the external-credit dimension is verified only by a positive
//     receipt from a connected, available source.
//   - Tickets carry the stable identity of T005 (scope, category, business
//     key, content hash, evidence version domain), so repeated detections
//     dedup onto one ticket, and the observation's evidence reference is
//     preserved for the occurrence/audit trail (FR-007/008).
//
// A classification is evidence, never a permission: it MUST NOT be read as a
// re-payment authorization (FR-015) and it never triggers recovery, replay,
// or any external action (FR-014/Q1).
package reconciliation

import (
	"errors"
	"time"
)

// PartyStatus is the closed availability vocabulary of one compared party:
// chain facts, PostgreSQL business state, or event delivery/consumer results.
type PartyStatus string

const (
	// PartyPresent: the party produced a positive fact inside the scanned
	// range (a confirmed chain fact, a PG business row, an event delivery or
	// consumer record).
	PartyPresent PartyStatus = "present"

	// PartyAbsent: the party definitively has no fact in the scanned range.
	// Absence is only meaningful inside a closed, gap-free scan; an absent
	// party in an incomplete scan stays pending instead of becoming a claim.
	PartyAbsent PartyStatus = "absent"

	// PartyUnknown: the party could not be determined (transaction result
	// unknown, receipt missing, read failure, source unreachable, evidence
	// removed by retention). Unknown is never treated as present or absent.
	PartyUnknown PartyStatus = "unknown"

	// PartyNotApplicable: the business type has no such party, so the party
	// is excluded from the comparison instead of being guessed.
	PartyNotApplicable PartyStatus = "not_applicable"
)

// Valid reports whether s is part of the closed party-status vocabulary.
func (s PartyStatus) Valid() bool {
	switch s {
	case PartyPresent, PartyAbsent, PartyUnknown, PartyNotApplicable:
		return true
	}
	return false
}

// PartyObservation is one party's evidence for one business key. Content is
// the party's canonical snapshot bytes; the T013-T015 adapters own
// canonicalization, including canonicalizing a definitive absence as its own
// marker (a ticket needs all three parties' canonical bytes, so an adapter
// that leaves an absent party's content empty downgrades the observation to
// incomplete rather than minting a weak identity).
type PartyObservation struct {
	// Status is the party availability; unknown shapes are refused by shape
	// validation and fall into the conservative default.
	Status PartyStatus
	// Content is the canonical snapshot bytes of the party.
	Content []byte
	// Orphaned marks chain evidence from an orphaned block (reorg). Orphaned
	// evidence can never support a permanent conclusion: it downgrades the
	// classification to incomplete/pending (FR-017).
	Orphaned bool
}

// DuplicateEvidence is the at-least-once delivery evidence of the event party
// for one business identity (Q4, FR-009). Every field is an observed fact:
// the classifier never infers absorption or divergence from the delivery
// count.
type DuplicateEvidence struct {
	// Deliveries is the observed delivery/consumption count for the identity
	// (>= 0). It feeds metrics and the "duplicate observed" trigger only; a
	// count alone NEVER indicates a fund anomaly.
	Deliveries int

	// ContentChecked reports whether the same-identity content comparison
	// ran. Absorption can never be asserted without it.
	ContentChecked bool
	// ContentDivergent: same identity, different content (Q4 divergence).
	ContentDivergent bool

	// VersionGuardIgnoredLegalOld: the repeated delivery is a legal old
	// version ignored by the version guard (Q4: absorbed).
	VersionGuardIgnoredLegalOld bool
	// VersionRuleViolated: the duplicate violates the version rules
	// (Q4 divergence).
	VersionRuleViolated bool

	// IdempotencyRecorded: the event contract's idempotency record
	// (consumer_inbox / consumer_versions) shows the delivery was absorbed.
	IdempotencyRecorded bool
	// EffectEvidencePresent: business-effect evidence was actually
	// collected. Absorption is never asserted from a count alone.
	EffectEvidencePresent bool
	// EffectCount is the observed number of business effects; <= 1 together
	// with an idempotency record is the absorbed-zero-effect shape.
	EffectCount int

	// RepeatedBusinessEffect: the duplicate produced or reveals a repeated
	// business effect (Q4 divergence).
	RepeatedBusinessEffect bool
	// RepeatedWithdrawalIntent: the duplicate reveals a repeated withdrawal
	// intent (Q4 divergence).
	RepeatedWithdrawalIntent bool
}

// Divergent reports whether the duplicate carries an evidenced business
// divergence (Q4). Delivery counts are deliberately absent from this
// predicate: only content, effect, intent, and version-rule facts can make a
// duplicate a fund anomaly.
func (d DuplicateEvidence) Divergent() bool {
	return d.ContentDivergent || d.VersionRuleViolated ||
		d.RepeatedBusinessEffect || d.RepeatedWithdrawalIntent
}

// absorbed reports whether the duplicate is proven idempotently absorbed with
// zero business divergence (Q4): same-identity content was checked and
// agrees, and either the version guard ignored a legal old version or the
// idempotency record plus effect evidence prove a single business effect.
// Anything weaker stays unproven and classifies as incomplete.
func (d DuplicateEvidence) absorbed() bool {
	if d.Divergent() || !d.ContentChecked {
		return false
	}
	if d.VersionGuardIgnoredLegalOld {
		return true
	}
	return d.IdempotencyRecorded && d.EffectEvidencePresent && d.EffectCount <= 1
}

// Coverage is the scan completeness and freshness context of one observation
// (FR-003/004/005; data-model.md §1.2/§1.3/§4). Completeness and freshness are
// gates, not hints: a conclusion that cannot prove both stays pending.
type Coverage struct {
	// ScanComplete is true only when the observation lies inside the closed,
	// gap-free persisted prefix of the task.
	ScanComplete bool
	// OpenGaps counts open recon_gap rows overlapping the observation range.
	// Any open gap forbids a definitive conclusion (FR-004).
	OpenGaps int
	// RetentionTrimmed marks evidence removed by retention/trimming: it is
	// unavailable, never an absence (research §2).
	RetentionTrimmed bool
	// EvidenceAt is when the evidence was collected.
	EvidenceAt time.Time
	// Now is the classification time, injected by the caller.
	Now time.Time
	// FreshnessTolerance is the configured freshness window; evidence older
	// than this is stale and downgrades the conclusion (FR-005).
	FreshnessTolerance time.Duration
}

// fresh reports the freshness verdict and its reason. A missing evidence
// time, missing classification time, or unconfigured (non-positive)
// tolerance means freshness cannot be proven and the conclusion stays
// pending (fail-closed; a source lag never silently passes).
func (c Coverage) fresh() (bool, ClassificationReason, string) {
	if c.EvidenceAt.IsZero() {
		return false, ReasonFreshnessUnproven, "evidence time is missing"
	}
	if c.Now.IsZero() {
		return false, ReasonFreshnessUnproven, "classification time is missing"
	}
	if c.FreshnessTolerance <= 0 {
		return false, ReasonFreshnessUnproven, "freshness tolerance is not configured"
	}
	if c.EvidenceAt.After(c.Now) {
		return false, ReasonFreshnessUnproven, "evidence time is in the future"
	}
	if c.Now.Sub(c.EvidenceAt) > c.FreshnessTolerance {
		return false, ReasonFreshnessExpired, "evidence is older than the freshness tolerance"
	}
	return true, "", ""
}

// UpstreamReceiptSource is the per-business-type upstream processing-receipt
// state: the task's recorded configuration
// (recon_task.upstream_receipt_source, data-model.md §1.1) plus what the
// read-only adapter observed. connected is an input only: a connected flag
// alone never proves upstream success (FR-006, Q4).
type UpstreamReceiptSource struct {
	// Source names the receipt source (audit only, no secrets).
	Source string
	// Connected is the recorded configuration: false means the upstream
	// processing receipt is not connected (未接入).
	Connected bool
	// Available is the observed reachability of a configured source. It is
	// only meaningful when Connected; configured-but-unavailable can never
	// support a consistent conclusion.
	Available bool
	// Receipt is the actual processing receipt read from a connected and
	// available source; nil means no receipt evidence was observed.
	Receipt *UpstreamReceipt
}

// UpstreamReceipt is a processing receipt observed at the upstream ledger.
type UpstreamReceipt struct {
	// Ref identifies the receipt evidence (audit only, no secrets).
	Ref string
	// Success is the receipt's verdict: only a positive receipt can support
	// the external-credit-verified statement.
	Success bool
}

// externalState derives the honest state of the upstream-credit dimension.
func (u UpstreamReceiptSource) externalState() ExternalCreditState {
	switch {
	case !u.Connected:
		return ExternalCreditUnverified
	case !u.Available:
		return ExternalCreditUnavailable
	case u.Receipt == nil:
		return ExternalCreditUnverified
	case u.Receipt.Success:
		return ExternalCreditVerified
	default:
		return ExternalCreditFailed
	}
}

// blockReason returns the reason an unconnected or configured-but-unavailable
// source forbids a consistent conclusion, if any.
func (u UpstreamReceiptSource) blockReason() (ClassificationReason, bool) {
	switch {
	case !u.Connected:
		return ReasonUpstreamUnconnected, true
	case !u.Available:
		return ReasonUpstreamUnavailable, true
	}
	return "", false
}

// ExternalCreditState is the honest state of the upstream-ledger credit
// dimension (FR-006). The classifier MUST NOT report external credit as
// verified without a connected, available source and a positive receipt.
type ExternalCreditState string

const (
	// ExternalCreditUnverified: no positive receipt evidence exists (the
	// source is not connected, or it is connected without a receipt). This is
	// the "未验证/不可判定" output of FR-006.
	ExternalCreditUnverified ExternalCreditState = "unverified"
	// ExternalCreditUnavailable: the configured source is unreachable.
	ExternalCreditUnavailable ExternalCreditState = "unavailable"
	// ExternalCreditVerified: connected, available, and a positive receipt
	// was observed.
	ExternalCreditVerified ExternalCreditState = "verified"
	// ExternalCreditFailed: connected, available, and the receipt reports
	// failure. Only this evidenced shape may classify a state mismatch
	// against otherwise clean in-project evidence.
	ExternalCreditFailed ExternalCreditState = "failed"
)

// Conclusion is the in-project three-way conclusion vocabulary.
type Conclusion string

const (
	// ConclusionConsistent: the required parties were present and agreed, on
	// complete, fresh, sufficient evidence. Never rendered when an evidence
	// gate failed or the upstream source is unconnected/unavailable.
	ConclusionConsistent Conclusion = "consistent"
	// ConclusionDivergent: an evidenced in-project business divergence.
	ConclusionDivergent Conclusion = "divergent"
	// ConclusionPending: incomplete or unknown; the observation stays under
	// observation and is never rendered as consistent.
	ConclusionPending Conclusion = "pending"
)

// ClassificationReason is the machine-readable reason behind a
// classification. It is audit/observability material (bounded vocabulary,
// low cardinality) and never contains secrets.
type ClassificationReason string

const (
	// ReasonThreeWayMatch: the three-way comparison agrees.
	ReasonThreeWayMatch ClassificationReason = "three_way_match"
	// ReasonMissing: a confirmed chain fact has no required business record
	// or event delivery.
	ReasonMissing ClassificationReason = "missing"
	// ReasonStateMismatch: present parties disagree, or a business fact
	// exists without its chain fact.
	ReasonStateMismatch ClassificationReason = "state_mismatch"
	// ReasonDuplicateDivergent: a duplicate carries evidenced business
	// divergence.
	ReasonDuplicateDivergent ClassificationReason = "duplicate_divergent"
	// ReasonDuplicateAbsorbed: a duplicate is idempotently absorbed with zero
	// business divergence (metrics/audit only).
	ReasonDuplicateAbsorbed ClassificationReason = "duplicate_absorbed"
	// ReasonDuplicateUnproven: a duplicate was observed but neither
	// divergence nor absorption is evidenced.
	ReasonDuplicateUnproven ClassificationReason = "duplicate_unproven"
	// ReasonUnknownResult: the transaction result is unknown; keep observing.
	ReasonUnknownResult ClassificationReason = "unknown_result"
	// ReasonScanIncomplete: the scan range is not closed and gap-free.
	ReasonScanIncomplete ClassificationReason = "scan_incomplete"
	// ReasonFreshnessExpired: evidence is older than the freshness tolerance.
	ReasonFreshnessExpired ClassificationReason = "freshness_expired"
	// ReasonFreshnessUnproven: freshness cannot be proven (missing times or
	// unconfigured tolerance).
	ReasonFreshnessUnproven ClassificationReason = "freshness_unproven"
	// ReasonEvidenceTrimmed: evidence was removed by retention/trimming.
	ReasonEvidenceTrimmed ClassificationReason = "evidence_trimmed"
	// ReasonEvidenceOrphaned: chain evidence comes from an orphaned block.
	ReasonEvidenceOrphaned ClassificationReason = "evidence_orphaned"
	// ReasonEvidenceMissing: a required party or identity component is not
	// available.
	ReasonEvidenceMissing ClassificationReason = "evidence_missing"
	// ReasonUpstreamUnconnected: the upstream processing receipt is not
	// connected (未接入); no consistent conclusion is possible.
	ReasonUpstreamUnconnected ClassificationReason = "upstream_unconnected"
	// ReasonUpstreamUnavailable: the configured upstream source is
	// unreachable.
	ReasonUpstreamUnavailable ClassificationReason = "upstream_unavailable"
	// ReasonUpstreamReceiptFailed: a connected, available source returned a
	// receipt reporting failure against otherwise clean in-project evidence.
	ReasonUpstreamReceiptFailed ClassificationReason = "upstream_receipt_failed"
	// ReasonUnknownShape: the observation shape is not recognized; the
	// conservative default applies (alert-only, never dropped).
	ReasonUnknownShape ClassificationReason = "unknown_shape"
	// ReasonEventNotApplicable: the candidate's event dimension provably has
	// no catalog event obligation (R2); the chain/PG comparison stands on its
	// own and the event dimension is excluded instead of being guessed.
	ReasonEventNotApplicable ClassificationReason = "event_not_applicable"
	// ReasonEventObligationUnproven: the event delivery is absent but the
	// expectation cannot be proven (no durable marker, unreadable evidence,
	// or an audited legal trim could explain the absence; R3). It stays
	// pending/gap, alert-only, and is never rendered as missing or
	// consistent.
	ReasonEventObligationUnproven ClassificationReason = "event_obligation_unproven"
)

// DuplicateOutcome reports the Q4 duplicate judgment independently of the
// category, so the absorbed-duplicate rate can be observed separately from
// fund tickets.
type DuplicateOutcome string

const (
	// DuplicateNone: no duplicate delivery was observed.
	DuplicateNone DuplicateOutcome = "none"
	// DuplicateAbsorbed: a proven idempotent absorption with zero business
	// divergence (no ticket, metrics/audit only).
	DuplicateAbsorbed DuplicateOutcome = "absorbed"
	// DuplicateDivergent: a duplicate with evidenced business divergence.
	DuplicateDivergent DuplicateOutcome = "divergent"
	// DuplicateUnproven: a duplicate whose absorption is not evidenced;
	// incomplete/pending, never a claim.
	DuplicateUnproven DuplicateOutcome = "unproven"
)

// Observation is the normalized three-way observation of one business key
// inside one scan scope, as produced by the read-only adapters (T013-T015)
// and the scan compare loop (T016). It is the complete input of Classify.
type Observation struct {
	// Scope and BusinessKey are the identity context; BusinessType must be
	// part of the closed 014 vocabulary.
	Scope        IdentityScope
	BusinessKey  BusinessKey
	BusinessType BusinessType

	// Chain, PG, Event are the three compared parties.
	Chain PartyObservation
	PG    PartyObservation
	Event PartyObservation

	// Mismatch is the adapter comparison verdict for the present required
	// parties: true only when those parties were compared and their facts
	// disagree. It is never inferred from availability alone.
	Mismatch bool

	// Duplicates carries the at-least-once delivery evidence (Q4).
	Duplicates DuplicateEvidence

	// Coverage is the scan completeness/freshness context.
	Coverage Coverage

	// Upstream is the per-business-type receipt source state.
	Upstream UpstreamReceiptSource

	// Version is the evidence version domain used for the identity key; any
	// later change to it re-enters verification (Q5).
	Version VersionDomain

	// EventObligation is the T040 discriminator outcome for this observation.
	// It is only set by the scan compare loop's decisive event-only absence
	// path (chain and PG present, event delivery absent); the zero value
	// means the discriminator did not apply and the historical classification
	// rules are unchanged.
	EventObligation EventObligationState

	// EvidenceRef references the evidence bundle for occurrence/audit rows.
	EvidenceRef string
}

// Classification is the machine verdict for one observation (T017). It is
// evidence for the lifecycle, never a disposition: a ticket still requires
// operator handling (FR-014/015).
type Classification struct {
	// Category is the discrepancy category; empty when no in-project
	// discrepancy was classified (consistent or absorbed duplicate).
	Category Category
	// Ticket reports whether a discrepancy ticket may be created or merged.
	// Only business divergence creates tickets (Q4).
	Ticket bool
	// Identity is the stable discrepancy identity; valid iff Ticket. The
	// same identity reopens the original ticket, a different identity
	// creates a linked ticket (FR-007).
	Identity Identity
	// Members enumerates the member log facts of a tx-aggregate detection
	// (T035): they are persisted as occurrence evidence and never split the
	// transaction into several tickets.
	Members []TxAggregateMember
	// Conclusion is the honest in-project three-way conclusion.
	Conclusion Conclusion
	// ExternalCredit is the upstream-ledger credit dimension state (FR-006).
	ExternalCredit ExternalCreditState
	// Duplicate is the Q4 duplicate judgment, independent of the category.
	Duplicate DuplicateOutcome
	// Reason is the machine-readable decision reason.
	Reason ClassificationReason
	// EvidenceRef carries the observation's evidence reference (preserved
	// for the occurrence/audit trail).
	EvidenceRef string
	// Detail is a short human-readable explanation (no secrets).
	Detail string
}

// HasDiscrepancy reports whether a discrepancy category was classified.
func (c Classification) HasDiscrepancy() bool { return c.Category != "" }

// MetricsOnly reports the absorbed-duplicate case: no ticket, only metrics
// and necessary audit (Q4). The absorbed-duplicate rate is an operational
// signal and stays separate from fund tickets.
func (c Classification) MetricsOnly() bool {
	return c.Duplicate == DuplicateAbsorbed && !c.Ticket
}

// FullyConsistent reports whether the observation is verified on both
// dimensions: the in-project conclusion is consistent AND the upstream credit
// dimension is verified. Unconnected/unavailable sources and absent receipts
// can never be fully consistent (FR-006).
func (c Classification) FullyConsistent() bool {
	return c.Conclusion == ConclusionConsistent && c.ExternalCredit == ExternalCreditVerified
}

// classifyDuplicate maps the Q4 duplicate facts onto an outcome. The delivery
// count only decides whether a duplicate exists at all; divergence and
// absorption are facts, never counts.
func classifyDuplicate(d DuplicateEvidence) DuplicateOutcome {
	if d.Deliveries <= 1 {
		return DuplicateNone
	}
	if d.Divergent() {
		return DuplicateDivergent
	}
	if d.absorbed() {
		return DuplicateAbsorbed
	}
	return DuplicateUnproven
}

// Classify applies the T017 machine classifier to one observation. It is a
// total function: an unrecognized shape falls into the conservative default
// (incomplete, alert-only) instead of being dropped or guessed.
func Classify(obs Observation) Classification {
	base := Classification{
		ExternalCredit: obs.Upstream.externalState(),
		Duplicate:      classifyDuplicate(obs.Duplicates),
		EvidenceRef:    obs.EvidenceRef,
	}

	if err := obs.validateShape(); err != nil {
		base.Category = CategoryIncomplete
		base.Conclusion = ConclusionPending
		base.Reason = ReasonUnknownShape
		base.Detail = "observation shape is not recognized; conservative default applies"
		return base
	}

	// Evidence gates first: a transient or unverifiable state must never
	// become a claim (FR-004/005, Edge).
	if obs.Chain.Orphaned {
		return pending(base, ReasonEvidenceOrphaned, "chain evidence is orphaned; only pending review is possible")
	}
	if !obs.Coverage.ScanComplete || obs.Coverage.OpenGaps > 0 {
		return pending(base, ReasonScanIncomplete, "scan range is not closed and gap-free")
	}
	if obs.Coverage.RetentionTrimmed {
		return pending(base, ReasonEvidenceTrimmed, "evidence was removed by retention")
	}
	if ok, reason, detail := obs.Coverage.fresh(); !ok {
		return pending(base, reason, detail)
	}

	// An unknown transaction result is preserved and observed; it is never
	// recorded as success or failure (FR-018).
	if obs.Chain.Status == PartyUnknown {
		base.Category = CategoryUnknown
		base.Conclusion = ConclusionPending
		base.Reason = ReasonUnknownResult
		base.Detail = "transaction result is unknown; keep observing"
		return base
	}
	// T040 R3 comes first: an absent event delivery whose expectation cannot
	// be proven is its own bounded reason (observable, alert-only) instead of
	// the generic unreadable-party reason. The discriminator only sets this
	// state on the decisive event-only absence path; the check is
	// unconditional so a caller can never route an unproven expectation into
	// a ticket.
	if obs.EventObligation == EventObligationUnproven {
		return pending(base, ReasonEventObligationUnproven,
			"event delivery is absent and no sufficient expectation evidence proves it was due")
	}
	if obs.PG.Status == PartyUnknown || obs.Event.Status == PartyUnknown {
		return pending(base, ReasonEvidenceMissing, "a compared party could not be read")
	}

	// Three-way availability and mismatch. Missing is a confirmed chain fact
	// without a required counterpart; a present business fact without its
	// chain fact is a state mismatch.
	switch {
	case obs.Chain.Status == PartyPresent &&
		(obs.PG.Status == PartyAbsent || obs.Event.Status == PartyAbsent):
		return ticket(base, CategoryMissing, ReasonMissing, "confirmed chain fact has no required business record or event delivery", obs)
	case obs.Mismatch:
		return ticket(base, CategoryStateMismatch, ReasonStateMismatch, "present parties disagree", obs)
	case obs.Chain.Status == PartyAbsent &&
		(obs.PG.Status == PartyPresent || obs.Event.Status == PartyPresent):
		return ticket(base, CategoryStateMismatch, ReasonStateMismatch, "business fact exists without its chain fact", obs)
	}

	// Q4: duplicate judgment only when the three-way compare found no
	// divergence. A duplicate count alone is never a fund anomaly.
	switch base.Duplicate {
	case DuplicateDivergent:
		return ticket(base, CategoryDuplicateDivergent, ReasonDuplicateDivergent, "duplicate carries evidenced business divergence", obs)
	case DuplicateUnproven:
		return pending(base, ReasonDuplicateUnproven, "duplicate observed but neither divergence nor absorption is evidenced")
	}

	// An evidenced upstream receipt failure against clean in-project evidence
	// is a state mismatch; a connected flag alone is not evidence.
	if base.ExternalCredit == ExternalCreditFailed {
		return ticket(base, CategoryStateMismatch, ReasonUpstreamReceiptFailed, "upstream receipt reports failure while in-project evidence is clean", obs)
	}

	// Unconnected or unreachable upstream sources forbid a consistent
	// conclusion (FR-006); evidenced in-project divergences above are
	// unaffected. An absorbed duplicate keeps its metrics-only outcome.
	if reason, blocked := obs.Upstream.blockReason(); blocked {
		return pending(base, reason, "upstream receipt source cannot support a consistent conclusion")
	}

	base.Conclusion = ConclusionConsistent
	switch {
	case base.Duplicate == DuplicateAbsorbed:
		base.Reason = ReasonDuplicateAbsorbed
		base.Detail = "duplicate is idempotently absorbed with zero business divergence; metrics/audit only"
	case obs.EventObligation == EventObligationNotApplicable:
		// R2: the event dimension is excluded because the candidate
		// provably carries no catalog event obligation. This is a chain/PG
		// agreement, never a claim that all three parties agreed.
		base.Reason = ReasonEventNotApplicable
		base.Detail = "the event dimension is not applicable to this candidate; chain/PG evidence agrees"
	default:
		base.Reason = ReasonThreeWayMatch
		base.Detail = "three-way comparison agrees"
	}
	return base
}

// pending marks a classification as incomplete/pending without a ticket.
func pending(base Classification, reason ClassificationReason, detail string) Classification {
	base.Category = CategoryIncomplete
	base.Ticket = false
	base.Identity = Identity{}
	base.Conclusion = ConclusionPending
	base.Reason = reason
	base.Detail = detail
	return base
}

// ticket mints the stable identity for a ticketable category. If the identity
// cannot be minted (invalid shape or insufficient evidence), the
// classification is downgraded conservatively to incomplete: no ticket is
// created from weak evidence (FR-009/Q4).
func ticket(base Classification, category Category, reason ClassificationReason, detail string, obs Observation) Classification {
	base.Category = category
	base.Conclusion = ConclusionDivergent
	base.Reason = reason
	base.Detail = detail

	identity, err := BuildIdentity(IdentityInput{
		Scope:       obs.Scope,
		Category:    category,
		BusinessKey: obs.BusinessKey,
		Snapshot: Snapshot{
			Chain: obs.Chain.Content,
			PG:    obs.PG.Content,
			Event: obs.Event.Content,
		},
		VersionDomain: obs.Version,
	})
	if err != nil {
		base.Ticket = false
		base.Identity = Identity{}
		base.Category = CategoryIncomplete
		base.Conclusion = ConclusionPending
		if errors.Is(err, ErrInvalidIdentityShape) {
			base.Reason = ReasonUnknownShape
		} else {
			base.Reason = ReasonEvidenceMissing
		}
		base.Detail = "identity could not be minted; classification downgraded conservatively"
		return base
	}

	base.Ticket = true
	base.Identity = identity
	return base
}

// validateShape refuses unrecognized shapes conservatively. Messages are
// static and never echo raw business values.
func (o Observation) validateShape() error {
	if err := o.Scope.Validate(); err != nil {
		return errors.New("invalid scope shape")
	}
	if !o.BusinessType.Known() {
		return errors.New("unknown business type")
	}
	if err := o.BusinessKey.Validate(); err != nil {
		return errors.New("invalid business key shape")
	}
	for _, party := range []PartyObservation{o.Chain, o.PG, o.Event} {
		if !party.Status.Valid() {
			return errors.New("unknown party status")
		}
	}
	if o.Coverage.OpenGaps < 0 {
		return errors.New("negative open-gap count")
	}
	if o.Duplicates.Deliveries < 0 || o.Duplicates.EffectCount < 0 {
		return errors.New("negative duplicate/effect count")
	}
	if o.EventObligation != "" && !o.EventObligation.Valid() {
		return errors.New("unknown event obligation state")
	}
	if o.Duplicates.Deliveries > 1 && o.Event.Status != PartyPresent {
		return errors.New("duplicate evidence without a present event party")
	}
	return nil
}
