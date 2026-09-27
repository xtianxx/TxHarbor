// identity.go implements T005: stable discrepancy identity and evidence
// hashing for 014 reconciliation (data-model.md §2, research §3), and T021:
// occurrence append, dedup and linked-ticket persistence (data-model.md
// §1.4–1.5/§2, contracts/discrepancy-lifecycle.md Q5, FR-007).
//
// The identity key is the fixed-order tuple
//
//	(scope, category, business key, content hash, evidence version domain)
//
// canonicalized into self-delimiting bytes and derived into a deterministic
// UUIDv5. Detecting the same fact twice therefore yields the same identity
// (dedup/reopen), while distinct facts stay distinct (linked tickets).
//
// Boundaries honored here:
//   - Unknown or incomplete shapes are conservative: an identity is only minted
//     when every key component can be canonicalized; otherwise a sentinel error
//     is returned so the caller classifies the observation as incomplete
//     instead of creating a ticket.
//   - Q4 ("only business divergence creates a ticket") is NOT decided here.
//     The machine classifier (T017) owns divergence detection; this file only
//     exposes which recognized categories are ticket-eligible.
package reconciliation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrInvalidIdentityShape marks input that cannot be canonicalized: an
	// unknown scope kind, an unknown category, a malformed business key, or an
	// inconsistent block identity. Callers MUST NOT mint a ticket from a shape
	// error; conservative handling keeps the observation alert-only.
	ErrInvalidIdentityShape = errors.New("invalid reconciliation identity shape")

	// ErrInsufficientEvidence marks evidence that is absent or too weak to
	// support a stable identity (missing snapshot parties, a version domain
	// with neither block identity nor business version). Callers MUST classify
	// the observation as incomplete, never as a discrepancy claim.
	ErrInsufficientEvidence = errors.New("insufficient reconciliation evidence")
)

// BusinessType is the closed 014 business vocabulary (data-model.md §1.1).
// The migration CHECK for recon_task.business_types and any scope JSON MUST
// agree with this set; an unknown type is refused conservatively.
type BusinessType string

// The recognized business types. These values are frozen vocabulary.
const (
	BusinessWithdrawal    BusinessType = "withdrawal"
	BusinessDeposit       BusinessType = "deposit"
	BusinessEventDelivery BusinessType = "event-delivery"
)

// Known reports whether b is part of the closed business-type vocabulary.
func (b BusinessType) Known() bool {
	switch b {
	case BusinessWithdrawal, BusinessDeposit, BusinessEventDelivery:
		return true
	default:
		return false
	}
}

// KnownBusinessTypes returns the closed business-type vocabulary in stable
// order. The caller receives a fresh slice and may mutate it freely.
func KnownBusinessTypes() []BusinessType {
	return []BusinessType{BusinessWithdrawal, BusinessDeposit, BusinessEventDelivery}
}

// ScopeKind is the closed scope-dimension vocabulary of a reconciliation key
// (data-model.md §1.1 recon_task.scope_kind).
type ScopeKind string

// The recognized scope kinds. These values are frozen vocabulary.
const (
	ScopeHeight ScopeKind = "height"
	ScopeTime   ScopeKind = "time"
)

// Known reports whether k is a recognized scope kind.
func (k ScopeKind) Known() bool {
	return k == ScopeHeight || k == ScopeTime
}

// IdentityScope is the range dimension of a stable identity: chain, scope kind,
// inclusive bounds, and the business types covered. For ScopeHeight bounds are
// block heights; for ScopeTime bounds are UTC Unix microseconds (the resolution
// of PostgreSQL timestamptz), normalized by TimeIdentityScope.
//
// The scope participates in the identity key so that the same fact discovered
// by two different scopes (or two chains) stays distinct; two detections inside
// one scope collapse to one identity.
type IdentityScope struct {
	ChainID       string         `json:"chain_id"`
	Kind          ScopeKind      `json:"kind"`
	From          int64          `json:"from"`
	To            int64          `json:"to"`
	BusinessTypes []BusinessType `json:"business_types"`
}

// HeightIdentityScope builds a validated height scope. from/to are inclusive
// block heights and businessTypes is the closed set the scope covers.
func HeightIdentityScope(chainID string, from, to int64, businessTypes ...BusinessType) (IdentityScope, error) {
	s := IdentityScope{
		ChainID:       chainID,
		Kind:          ScopeHeight,
		From:          from,
		To:            to,
		BusinessTypes: append([]BusinessType(nil), businessTypes...),
	}
	if err := s.Validate(); err != nil {
		return IdentityScope{}, err
	}
	return s, nil
}

// TimeIdentityScope builds a validated time scope from an inclusive UTC time
// range. Bounds are stored in Unix microseconds so the canonical bytes are
// independent of time location and monotonic clock readings.
func TimeIdentityScope(chainID string, from, to time.Time, businessTypes ...BusinessType) (IdentityScope, error) {
	if from.IsZero() || to.IsZero() {
		return IdentityScope{}, fmt.Errorf("%w: time scope requires both bounds", ErrInvalidIdentityShape)
	}
	s := IdentityScope{
		ChainID:       chainID,
		Kind:          ScopeTime,
		From:          from.UnixMicro(),
		To:            to.UnixMicro(),
		BusinessTypes: append([]BusinessType(nil), businessTypes...),
	}
	if err := s.Validate(); err != nil {
		return IdentityScope{}, err
	}
	return s, nil
}

// Validate checks the scope conservatively: a chain identity is required, the
// kind must be known, from/to must be an inclusive ascending range, and at
// least one known business type must be present.
func (s IdentityScope) Validate() error {
	if err := validateIdentityChainID(s.ChainID); err != nil {
		return err
	}
	switch s.Kind {
	case ScopeHeight:
		if s.From < 0 || s.To < s.From {
			return fmt.Errorf("%w: height scope %d..%d is not an inclusive ascending range", ErrInvalidIdentityShape, s.From, s.To)
		}
	case ScopeTime:
		if s.From <= 0 || s.To < s.From {
			return fmt.Errorf("%w: time scope %d..%d is not an inclusive ascending unix-microsecond range", ErrInvalidIdentityShape, s.From, s.To)
		}
	default:
		return fmt.Errorf("%w: unknown scope kind %q", ErrInvalidIdentityShape, s.Kind)
	}
	if _, err := canonicalBusinessTypes(s.BusinessTypes); err != nil {
		return err
	}
	return nil
}

// TimeBounds returns the UTC time bounds of a time scope. ok is false for
// height scopes.
func (s IdentityScope) TimeBounds() (from, to time.Time, ok bool) {
	if s.Kind != ScopeTime {
		return time.Time{}, time.Time{}, false
	}
	return time.UnixMicro(s.From).UTC(), time.UnixMicro(s.To).UTC(), true
}

// String renders a compact human-readable scope for logs and audit details.
// It carries no secret material.
func (s IdentityScope) String() string {
	types := make([]string, len(s.BusinessTypes))
	for i, t := range s.BusinessTypes {
		types[i] = string(t)
	}
	return fmt.Sprintf("chain=%s kind=%s %d..%d business_types=%s",
		s.ChainID, s.Kind, s.From, s.To, strings.Join(types, ","))
}

// Category is the machine classification vocabulary of a discrepancy
// (data-model.md §1.4, contracts/discrepancy-lifecycle.md).
type Category string

// The recognized discrepancy categories. These values are frozen vocabulary and
// are the data-model's category ENUM.
const (
	CategoryMissing            Category = "missing"
	CategoryDuplicateDivergent Category = "duplicate_divergent"
	CategoryStateMismatch      Category = "state_mismatch"
	CategoryUnknown            Category = "unknown"
	CategoryIncomplete         Category = "incomplete"
)

// Known reports whether c is part of the closed category vocabulary.
func (c Category) Known() bool {
	switch c {
	case CategoryMissing, CategoryDuplicateDivergent, CategoryStateMismatch, CategoryUnknown, CategoryIncomplete:
		return true
	default:
		return false
	}
}

// Ticketable reports whether the category is eligible for ticket creation.
// Only business divergence creates tickets (Q4): missing,
// duplicate_divergent, and state_mismatch. unknown/incomplete are alert-only
// and MUST NOT be rendered as a claim. The classifier (T017) remains the
// authority on whether a concrete detection actually diverges; the
// zero-divergence absorbed-duplicate rule is applied there, not here.
func (c Category) Ticketable() bool {
	switch c {
	case CategoryMissing, CategoryDuplicateDivergent, CategoryStateMismatch:
		return true
	default:
		return false
	}
}

// BusinessKeyKind names the business object namespace of a business key. Kinds
// MUST be stable tokens: the kind is part of the identity, so renaming one is a
// dedup-breaking change. New kinds are added deliberately by adapter owners;
// malformed tokens are refused.
type BusinessKeyKind string

// The business key kinds named by data-model.md §1.4 ("request_id/intent_id/
// event 身份等"). Further kinds may be introduced by adapters as stable tokens.
const (
	BusinessKeyRequestID BusinessKeyKind = "request_id"
	BusinessKeyIntentID  BusinessKeyKind = "intent_id"
	BusinessKeyEventID   BusinessKeyKind = "event_id"
	BusinessKeyAttemptID BusinessKeyKind = "attempt_id"
	// BusinessKeyTxHash is the chain-anchored identity of one transaction:
	// the only stable business key available when a confirmed chain fact has
	// no PostgreSQL business row at all (014 US1 missing detection). The
	// value is the lowercase 0x-prefixed transaction hash; the PG-state read
	// resolves it back to the authoritative attempt rows and reports a
	// definitive absence when none exists.
	BusinessKeyTxHash BusinessKeyKind = "tx_hash"
)

// BusinessKey is one business object's primary key. The kind disambiguates
// literals that collide across namespaces (a request_id "abc" and an intent_id
// "abc" are different objects and MUST NOT dedup into one ticket).
type BusinessKey struct {
	Kind  BusinessKeyKind `json:"kind"`
	Value string          `json:"value"`
}

// Validate checks the business key shape conservatively.
func (k BusinessKey) Validate() error {
	if !validIdentityToken(string(k.Kind), 64) {
		return fmt.Errorf("%w: invalid business key kind %q", ErrInvalidIdentityShape, k.Kind)
	}
	if strings.TrimSpace(k.Value) == "" {
		return fmt.Errorf("%w: business key value is required", ErrInvalidIdentityShape)
	}
	if strings.ContainsRune(k.Value, 0) {
		return fmt.Errorf("%w: business key value contains NUL", ErrInvalidIdentityShape)
	}
	return nil
}

// VersionDomain is the evidence version domain of an identity (data-model.md
// §1.4 evidence_version_domain, research §3): block identity, the business
// versions that can invalidate a conclusion (recovery/authorization/scope/
// state_version), and the evidence time point. A conclusion is only valid for
// the recorded domain; any change enters reverification (Q5).
type VersionDomain struct {
	BlockNumber          uint64    `json:"block_number"`
	BlockHash            string    `json:"block_hash"`
	RecoveryVersion      string    `json:"recovery_version"`
	AuthorizationVersion string    `json:"authorization_version"`
	ScopeVersion         string    `json:"scope_version"`
	StateVersion         int64     `json:"state_version"`
	EvidenceAt           time.Time `json:"evidence_at"`
}

// Validate checks the version domain conservatively: an evidence time is
// always required; block number and hash must appear together; the domain must
// carry at least one block or business version signal, otherwise the evidence
// is too weak to anchor an identity and ErrInsufficientEvidence is returned.
func (v VersionDomain) Validate() error {
	if v.EvidenceAt.IsZero() {
		return fmt.Errorf("%w: evidence time is required", ErrInvalidIdentityShape)
	}
	switch {
	case v.BlockNumber == 0 && v.BlockHash != "":
		return fmt.Errorf("%w: block hash %q without a block number", ErrInvalidIdentityShape, v.BlockHash)
	case v.BlockNumber > 0 && v.BlockHash == "":
		return fmt.Errorf("%w: block number %d without a block hash", ErrInvalidIdentityShape, v.BlockNumber)
	}
	if v.StateVersion < 0 {
		return fmt.Errorf("%w: state version %d is negative", ErrInvalidIdentityShape, v.StateVersion)
	}
	for _, item := range []struct{ name, value string }{
		{"recovery_version", v.RecoveryVersion},
		{"authorization_version", v.AuthorizationVersion},
		{"scope_version", v.ScopeVersion},
	} {
		if strings.ContainsRune(item.value, 0) {
			return fmt.Errorf("%w: %s contains NUL", ErrInvalidIdentityShape, item.name)
		}
	}
	if v.BlockNumber == 0 && v.BlockHash == "" &&
		v.RecoveryVersion == "" && v.AuthorizationVersion == "" &&
		v.ScopeVersion == "" && v.StateVersion == 0 {
		return fmt.Errorf("%w: version domain has neither block identity nor business version", ErrInsufficientEvidence)
	}
	return nil
}

// Snapshot carries the normalized bytes of the three compared parties: chain
// facts, PostgreSQL business state, and event delivery/consumer results. Each
// party's bytes are canonicalized by its adapter (T013-T015); this package
// only freezes the combination order (chain, pg, event) so the content hash is
// stable across runs.
type Snapshot struct {
	Chain []byte
	PG    []byte
	Event []byte
}

// ContentHash is the SHA-256 digest over the canonical, fixed-order bytes of a
// three-party snapshot. It is the content-hash component of an identity key.
type ContentHash [sha256.Size]byte

// Bytes returns a defensive copy of the digest.
func (h ContentHash) Bytes() []byte {
	out := make([]byte, len(h))
	copy(out, h[:])
	return out
}

// Hex returns the lowercase hex digest, matching the storage convention of the
// 014 discrepancy.content_hash BYTEA column ("\x" + this hex).
func (h ContentHash) Hex() string { return hex.EncodeToString(h[:]) }

// IsZero reports whether the digest is the zero value (never hashed).
func (h ContentHash) IsZero() bool { return h == ContentHash{} }

// snapshotCanonicalVersion freezes the snapshot canonicalization domain.
// Changing it changes every content hash and is a dedup-breaking change.
const snapshotCanonicalVersion = "txharbor.reconciliation.snapshot.v1"

// SnapshotHash hashes the canonical bytes of a three-party snapshot. All three
// parties are required: a missing party is ErrInsufficientEvidence so the
// caller classifies incomplete instead of claiming a divergence.
func SnapshotHash(s Snapshot) (ContentHash, error) {
	parties := []struct {
		name string
		body []byte
	}{
		{"chain", s.Chain},
		{"pg", s.PG},
		{"event", s.Event},
	}
	var missing []string
	for _, p := range parties {
		if len(p.body) == 0 {
			missing = append(missing, p.name)
		}
	}
	if len(missing) > 0 {
		return ContentHash{}, fmt.Errorf("%w: missing %s snapshot", ErrInsufficientEvidence, strings.Join(missing, ", "))
	}
	w := &identityCanonWriter{}
	w.bytesField("snapshot.version", []byte(snapshotCanonicalVersion))
	w.bytesField("snapshot.chain", s.Chain)
	w.bytesField("snapshot.pg", s.PG)
	w.bytesField("snapshot.event", s.Event)
	return ContentHash(sha256.Sum256(w.buf.Bytes())), nil
}

// identityNamespace freezes the UUIDv5 namespace for discrepancy identities.
// Derivation depends only on the canonical key bytes documented below;
// changing it changes every discrepancy_id and is a dedup-breaking change.
var identityNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("txharbor/reconciliation/identity/v1"))

// identityCanonicalVersion freezes the identity key canonicalization domain.
const identityCanonicalVersion = "txharbor.reconciliation.identity.v1"

// Identity is one stable discrepancy identity. The zero value is invalid and
// authorizes nothing; build values through BuildIdentity only. Fields are
// unexported so the fixed key order and canonical bytes cannot be bypassed.
type Identity struct {
	scope         IdentityScope
	category      Category
	businessKey   BusinessKey
	contentHash   ContentHash
	versionDomain VersionDomain
	canonical     []byte
	id            uuid.UUID
}

// IdentityInput is the raw material of an identity: the scope context, the
// machine category, the business key, the normalized three-party snapshot, and
// the evidence version domain.
type IdentityInput struct {
	Scope         IdentityScope
	Category      Category
	BusinessKey   BusinessKey
	Snapshot      Snapshot
	VersionDomain VersionDomain
}

// BuildIdentity validates the input conservatively and mints the stable
// identity. The key order is fixed as (scope, category, business key, content
// hash, evidence version domain). A shape violation is ErrInvalidIdentityShape
// and missing evidence is ErrInsufficientEvidence; both MUST be handled by the
// caller as "no ticket / incomplete".
func BuildIdentity(in IdentityInput) (Identity, error) {
	if err := in.Scope.Validate(); err != nil {
		return Identity{}, err
	}
	if !in.Category.Known() {
		return Identity{}, fmt.Errorf("%w: unknown category %q", ErrInvalidIdentityShape, in.Category)
	}
	if err := in.BusinessKey.Validate(); err != nil {
		return Identity{}, err
	}
	if err := in.VersionDomain.Validate(); err != nil {
		return Identity{}, err
	}
	contentHash, err := SnapshotHash(in.Snapshot)
	if err != nil {
		return Identity{}, err
	}
	canonical := identityCanonicalBytes(in.Scope, in.Category, in.BusinessKey, contentHash, in.VersionDomain)
	return Identity{
		scope:         in.Scope,
		category:      in.Category,
		businessKey:   in.BusinessKey,
		contentHash:   contentHash,
		versionDomain: in.VersionDomain,
		canonical:     canonical,
		id:            uuid.NewSHA1(identityNamespace, canonical),
	}, nil
}

// identityCanonicalBytes serializes the key in the fixed data-model order with
// a frozen version tag and self-delimiting fields.
func identityCanonicalBytes(scope IdentityScope, category Category, key BusinessKey, hash ContentHash, version VersionDomain) []byte {
	w := &identityCanonWriter{}
	w.bytesField("identity.version", []byte(identityCanonicalVersion))

	types, _ := canonicalBusinessTypes(scope.BusinessTypes) // validated by the caller
	w.stringField("scope.chain_id", scope.ChainID)
	w.stringField("scope.kind", string(scope.Kind))
	w.int64Field("scope.from", scope.From)
	w.int64Field("scope.to", scope.To)
	w.uint64Field("scope.business_types.count", uint64(len(types)))
	for i, t := range types {
		w.stringField(fmt.Sprintf("scope.business_types.%d", i), string(t))
	}

	w.stringField("category", string(category))

	w.stringField("business_key.kind", string(key.Kind))
	w.stringField("business_key.value", key.Value)

	w.bytesField("content_hash", hash[:])

	w.uint64Field("version.block_number", version.BlockNumber)
	w.stringField("version.block_hash", version.BlockHash)
	w.stringField("version.recovery_version", version.RecoveryVersion)
	w.stringField("version.authorization_version", version.AuthorizationVersion)
	w.stringField("version.scope_version", version.ScopeVersion)
	w.int64Field("version.state_version", version.StateVersion)
	w.int64Field("version.evidence_at_unix_nano", version.EvidenceAt.UnixNano())

	return w.buf.Bytes()
}

// Valid reports whether the identity was minted by BuildIdentity.
func (id Identity) Valid() bool { return id.id != uuid.Nil }

// ID returns the deterministic UUIDv5 discrepancy_id (the stable identity).
func (id Identity) ID() uuid.UUID { return id.id }

// Scope returns a defensive copy of the scope dimension.
func (id Identity) Scope() IdentityScope {
	out := id.scope
	out.BusinessTypes = copyBusinessTypes(id.scope.BusinessTypes)
	return out
}

// Category returns the classification component.
func (id Identity) Category() Category { return id.category }

// BusinessKey returns the business key component.
func (id Identity) BusinessKey() BusinessKey { return id.businessKey }

// ContentHash returns the content hash component.
func (id Identity) ContentHash() ContentHash { return id.contentHash }

// VersionDomain returns the evidence version domain component.
func (id Identity) VersionDomain() VersionDomain { return id.versionDomain }

// CanonicalBytes returns a defensive copy of the frozen canonical key bytes.
func (id Identity) CanonicalBytes() []byte {
	out := make([]byte, len(id.canonical))
	copy(out, id.canonical)
	return out
}

// Ticketable reports whether this identity is eligible for ticket creation
// (Category.Ticketable). unknown/incomplete identities stay alert-only.
func (id Identity) Ticketable() bool { return id.category.Ticketable() }

// SameIdentity reports whether both values are the same minted identity. This
// is the dedup/reopen predicate: same identity reopens the original ticket,
// a different identity creates a linked ticket.
func (id Identity) SameIdentity(other Identity) bool {
	return id.Valid() && other.Valid() && id.id == other.id
}

// TxAggregateMember is one member log fact of a tx-aggregate detection (T035,
// data-model.md §2 tx 聚合身份): the transaction hash is the aggregate key, and
// every member log contributes its block height/hash, log index, contract and
// topic0 to the evidence trail. Amount payloads are deliberately absent: money
// stays in the chain adapter's raw hex form and is never parsed here.
type TxAggregateMember struct {
	BlockNumber int64  `json:"block_number"`
	BlockHash   string `json:"block_hash"`
	TxHash      string `json:"tx_hash"`
	LogIndex    int64  `json:"log_index"`
	Contract    string `json:"contract"`
	Topic0      string `json:"topic0"`
}

// Validate checks the member shape conservatively.
func (m TxAggregateMember) Validate() error {
	if m.BlockNumber < 0 {
		return fmt.Errorf("%w: member block number %d is negative", ErrInvalidIdentityShape, m.BlockNumber)
	}
	if m.LogIndex < 0 {
		return fmt.Errorf("%w: member log index %d is negative", ErrInvalidIdentityShape, m.LogIndex)
	}
	if strings.TrimSpace(m.TxHash) == "" {
		return fmt.Errorf("%w: member has no tx hash", ErrInvalidIdentityShape)
	}
	return nil
}

// txAggregateMemberKey identifies one member log for deduplication: block
// identity + log index is the on-chain log identity.
type txAggregateMemberKey struct {
	blockNumber int64
	blockHash   string
	logIndex    int64
	txHash      string
}

// CanonicalizeTxAggregateMembers validates, deduplicates and orders members by
// (block number, block hash, tx hash, log index) so the same member set always
// yields the same evidence bytes and the same ref regardless of discovery order.
func CanonicalizeTxAggregateMembers(members []TxAggregateMember) ([]TxAggregateMember, error) {
	seen := make(map[txAggregateMemberKey]struct{}, len(members))
	out := make([]TxAggregateMember, 0, len(members))
	for _, member := range members {
		if err := member.Validate(); err != nil {
			return nil, err
		}
		key := txAggregateMemberKey{
			blockNumber: member.BlockNumber,
			blockHash:   strings.ToLower(strings.TrimSpace(member.BlockHash)),
			logIndex:    member.LogIndex,
			txHash:      strings.ToLower(strings.TrimSpace(member.TxHash)),
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		member.BlockHash = key.blockHash
		member.TxHash = key.txHash
		out = append(out, member)
	}
	sort.Slice(out, func(i, j int) bool {
		a, z := out[i], out[j]
		switch {
		case a.BlockNumber != z.BlockNumber:
			return a.BlockNumber < z.BlockNumber
		case a.BlockHash != z.BlockHash:
			return a.BlockHash < z.BlockHash
		case a.TxHash != z.TxHash:
			return a.TxHash < z.TxHash
		default:
			return a.LogIndex < z.LogIndex
		}
	})
	return out, nil
}

// txAggregateEvidenceVersion freezes the member evidence canonicalization
// domain. Changing it changes every member digest.
const txAggregateEvidenceVersion = "txharbor.reconciliation.txaggregate.v1"

// txAggregateMemberCanonicalBytes encodes the member set deterministically for
// the evidence digest fallback.
func txAggregateMemberCanonicalBytes(members []TxAggregateMember) []byte {
	w := &identityCanonWriter{}
	w.bytesField("txaggregate.members.version", []byte(txAggregateEvidenceVersion))
	w.uint64Field("txaggregate.members.count", uint64(len(members)))
	for i, member := range members {
		prefix := fmt.Sprintf("txaggregate.members.%d", i)
		w.int64Field(prefix+".block_number", member.BlockNumber)
		w.stringField(prefix+".block_hash", member.BlockHash)
		w.stringField(prefix+".tx_hash", member.TxHash)
		w.int64Field(prefix+".log_index", member.LogIndex)
		w.stringField(prefix+".contract", member.Contract)
		w.stringField(prefix+".topic0", member.Topic0)
	}
	return w.buf.Bytes()
}

// TxAggregateEvidenceRef renders one bounded occurrence evidence reference that
// enumerates the member logs of a tx-aggregate detection (T035): every member's
// block, log index, contract and topic0 appears verbatim while the reference
// stays within the discrepancy_occurrence.evidence_ref bound. When the members
// exceed the bound the reference carries the member count plus a member digest
// (never a silently dropped subset).
func TxAggregateEvidenceRef(members []TxAggregateMember, max int) (string, error) {
	canonical, err := CanonicalizeTxAggregateMembers(members)
	if err != nil {
		return "", err
	}
	if len(canonical) == 0 {
		return "", fmt.Errorf("%w: tx aggregate evidence requires at least one member", ErrInvalidIdentityShape)
	}
	if max <= 0 {
		max = 512
	}
	txHash := canonical[0].TxHash
	head := fmt.Sprintf("txagg:v1 tx=%s members=%d", txHash, len(canonical))
	var full strings.Builder
	full.WriteString(head)
	for _, member := range canonical {
		fmt.Fprintf(&full, ";b=%d:h=%s:l=%d:c=%s:t0=%s",
			member.BlockNumber, member.BlockHash, member.LogIndex, member.Contract, member.Topic0)
	}
	if full.Len() <= max {
		return full.String(), nil
	}
	digest := sha256.Sum256(txAggregateMemberCanonicalBytes(canonical))
	bounded := fmt.Sprintf("%s digest=sha256:%s", head, hex.EncodeToString(digest[:]))
	if len(bounded) > max || len(head) > max {
		return "", fmt.Errorf("%w: tx aggregate evidence bound %d is too small", ErrInvalidIdentityShape, max)
	}
	return bounded, nil
}

// String renders a compact identity summary for logs and audit details. It
// carries no secret material.
func (id Identity) String() string {
	if !id.Valid() {
		return "reconciliation-identity[invalid]"
	}
	return fmt.Sprintf("reconciliation-identity[%s category=%s business_key=%s/%s id=%s]",
		id.scope, id.category, id.businessKey.Kind, id.businessKey.Value, id.id)
}

// DetectionInterval records the claimed detect interval a ticket was observed
// in (the scan invocation's one claimed range), so a later production
// re-verification can re-read exactly the evidence window the conclusion was
// derived from without re-scanning the whole task scope or inventing one. It
// is evidence metadata only: it never participates in the identity key. Bounds
// use the scope kind's unit (heights, or UTC Unix microseconds for time).
type DetectionInterval struct {
	Kind ScopeKind `json:"kind"`
	From int64     `json:"from"`
	To   int64     `json:"to"`
}

// Validate checks the interval shape conservatively.
func (d DetectionInterval) Validate() error {
	switch d.Kind {
	case ScopeHeight:
		if d.From < 0 || d.To < d.From {
			return fmt.Errorf("%w: detection interval %d..%d is not an inclusive ascending height range",
				ErrInvalidIdentityShape, d.From, d.To)
		}
	case ScopeTime:
		if d.From <= 0 || d.To < d.From {
			return fmt.Errorf("%w: detection interval %d..%d is not an inclusive ascending unix-microsecond range",
				ErrInvalidIdentityShape, d.From, d.To)
		}
	default:
		return fmt.Errorf("%w: detection interval has unknown scope kind %q", ErrInvalidIdentityShape, d.Kind)
	}
	return nil
}

// PersistedEvidenceDomain is the JSONB document stored in
// discrepancy.evidence_version_domain: the identity's version domain plus the
// scope it was detected under, so a later scan can distinguish "the same
// tx-aggregate identity in the same scope" from a same-key fact in a different
// scope (T035). The extra scope fields are evidence metadata; the identity
// derivation itself is untouched.
//
// The detection metadata (business type, claimed interval, tx-aggregate member
// set) is appended additively for the production pending_verify
// re-verification entry: old rows without it stay conservatively unverifiable
// (unknown), never guessed.
type PersistedEvidenceDomain struct {
	VersionDomain
	Scope *IdentityScope `json:"scope,omitempty"`
	// BusinessType is the detection's closed business type (the PG read
	// dimension). Written for tickets detected from this revision on.
	BusinessType BusinessType `json:"business_type,omitempty"`
	// Detection is the claimed interval the ticket was detected in.
	Detection *DetectionInterval `json:"detection,omitempty"`
	// TxMembers is the tx-aggregate member set at the latest detection
	// (missing member logs keyed by tx_hash). It is the completeness basis of
	// a re-verification: every recorded member log must have its business
	// record for the aggregate to be verifiable as fixed.
	TxMembers []TxAggregateMember `json:"tx_members,omitempty"`
}

// PersistedEvidenceDomainJSON marshals the version domain plus the detection
// scope for the discrepancy row. A scope whose business types cannot be
// canonicalized is refused instead of persisted without its scope marker.
func PersistedEvidenceDomainJSON(version VersionDomain, scope *IdentityScope) ([]byte, error) {
	return PersistedEvidenceDomainJSONFor(version, scope, "", nil, nil)
}

// PersistedEvidenceDomainJSONFor marshals the version domain plus the full
// detection metadata (scope, business type, claimed interval, tx-aggregate
// member set). Invalid shapes are refused instead of persisted half-recorded.
func PersistedEvidenceDomainJSONFor(version VersionDomain, scope *IdentityScope,
	businessType BusinessType, detection *DetectionInterval, members []TxAggregateMember) ([]byte, error) {
	doc := PersistedEvidenceDomain{VersionDomain: version, BusinessType: businessType}
	if scope != nil {
		copied := *scope
		copied.BusinessTypes = copyBusinessTypes(scope.BusinessTypes)
		if _, err := canonicalBusinessTypes(copied.BusinessTypes); err != nil {
			return nil, err
		}
		doc.Scope = &copied
	}
	if businessType != "" && !businessType.Known() {
		return nil, fmt.Errorf("%w: persisted business type %q is unknown", ErrInvalidIdentityShape, businessType)
	}
	if detection != nil {
		if err := detection.Validate(); err != nil {
			return nil, err
		}
		copied := *detection
		doc.Detection = &copied
	}
	if len(members) > 0 {
		canonical, err := CanonicalizeTxAggregateMembers(members)
		if err != nil {
			return nil, err
		}
		doc.TxMembers = canonical
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal persisted evidence domain: %w", err)
	}
	return raw, nil
}

// ParsePersistedEvidenceDomain decodes a stored evidence_version_domain
// document. A malformed document is refused conservatively (the caller must
// not guess a scope).
func ParsePersistedEvidenceDomain(raw []byte) (PersistedEvidenceDomain, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return PersistedEvidenceDomain{}, fmt.Errorf("%w: empty evidence version domain", ErrInvalidIdentityShape)
	}
	var doc PersistedEvidenceDomain
	if err := json.Unmarshal(raw, &doc); err != nil {
		return PersistedEvidenceDomain{}, fmt.Errorf("%w: evidence version domain is not a JSON object: %v", ErrInvalidIdentityShape, err)
	}
	return doc, nil
}

// SameIdentityScope reports whether a stored scope equals the observed scope
// (chain, kind, inclusive bounds and the canonical business-type set). It is
// the tx-aggregate ticket lookup predicate: the same business key under a
// different scope is a different identity, never a merge.
func SameIdentityScope(a, b IdentityScope) bool {
	if a.ChainID != b.ChainID || a.Kind != b.Kind || a.From != b.From || a.To != b.To {
		return false
	}
	ta, errA := canonicalBusinessTypes(a.BusinessTypes)
	tb, errB := canonicalBusinessTypes(b.BusinessTypes)
	if errA != nil || errB != nil || len(ta) != len(tb) {
		return false
	}
	for i := range ta {
		if ta[i] != tb[i] {
			return false
		}
	}
	return true
}

// identityCanonWriter writes unambiguous, self-delimiting canonical bytes:
// every field is a length-prefixed tag followed by a length-prefixed value, so
// no combination of values can alias another field sequence.
type identityCanonWriter struct {
	buf bytes.Buffer
}

func (w *identityCanonWriter) tag(name string) {
	w.buf.WriteByte(byte(len(name)))
	w.buf.WriteString(name)
}

func (w *identityCanonWriter) bytesField(name string, value []byte) {
	w.tag(name)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	w.buf.Write(length[:])
	w.buf.Write(value)
}

func (w *identityCanonWriter) stringField(name, value string) {
	w.bytesField(name, []byte(value))
}

func (w *identityCanonWriter) uint64Field(name string, value uint64) {
	w.tag(name)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	w.buf.Write(raw[:])
}

func (w *identityCanonWriter) int64Field(name string, value int64) {
	w.uint64Field(name, uint64(value))
}

// canonicalBusinessTypes validates and canonicalizes a business-type set:
// known values only, sorted ascending, deduplicated. Order-insensitivity keeps
// the same logical scope from minting two identities.
func canonicalBusinessTypes(in []BusinessType) ([]BusinessType, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("%w: at least one business type is required", ErrInvalidIdentityShape)
	}
	seen := make(map[BusinessType]struct{}, len(in))
	for _, t := range in {
		if !t.Known() {
			return nil, fmt.Errorf("%w: unknown business type %q", ErrInvalidIdentityShape, t)
		}
		seen[t] = struct{}{}
	}
	out := make([]BusinessType, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// copyBusinessTypes returns a defensive copy of a business-type slice.
func copyBusinessTypes(in []BusinessType) []BusinessType {
	if in == nil {
		return nil
	}
	out := make([]BusinessType, len(in))
	copy(out, in)
	return out
}

// validateIdentityChainID rejects empty or control-character chain identities.
func validateIdentityChainID(chainID string) error {
	if strings.TrimSpace(chainID) == "" {
		return fmt.Errorf("%w: chain_id is required", ErrInvalidIdentityShape)
	}
	if strings.ContainsAny(chainID, "\x00\n\r\t") {
		return fmt.Errorf("%w: chain_id contains control characters", ErrInvalidIdentityShape)
	}
	return nil
}

// validIdentityToken accepts a conservative stable-token shape: lowercase
// letters, digits, '_', '-', '.', '/'; length 1..max.
func validIdentityToken(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-', r == '.', r == '/':
		default:
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// T021: occurrence append, dedup and linked-ticket persistence
// (data-model.md §1.4–1.5/§2, contracts/discrepancy-lifecycle.md Q5, FR-007)
//
// BuildIdentity (T005) mints the stable identity: the same fact detected twice
// is the same ticket. This layer is the persistence counterpart and decides
// what one recorded detection does to that ticket:
//
//   - a first detection inserts the ticket (an optional linked_to associates a
//     different identity) and appends one occurrence row;
//   - a repeat detection of the same identity only appends
//     discrepancy_occurrence rows (evidence_ref + scan_task_id); it never
//     creates a second ticket, so re-scans cannot mint tickets without bound;
//   - a confirmed recurrence reopens the original ticket (reopen_count+1) and
//     keeps every historical row: rows are only ever inserted or updated, so
//     the close/disposition/reverify history stays intact;
//   - a tx-aggregate detection (missing + tx_hash business key) reuses the
//     T035 keyed root lookup and the evidence-change rule, so a reorg
//     replacement or a changed member set stays on the original ticket (Q5
//     invalidation + occurrence append) instead of minting a second ticket;
//   - linked_to is written only on creation and is never cleared by dedup,
//     invalidation or reopen, so reorg/re-scan cannot lose an association;
//   - only 014 tables are written: discrepancy, discrepancy_occurrence and
//     recon_audit.
// ---------------------------------------------------------------------------

// OccurrenceRecord is one append-only discrepancy_occurrence row
// (data-model.md §1.5): the reappearance evidence of one detection. A repeat
// detection appends a row and never creates or replaces a ticket.
type OccurrenceRecord struct {
	// DiscrepancyID is the stable identity handle the occurrence belongs to.
	DiscrepancyID uuid.UUID
	// ObservedAt is the evidence observation time; zero means now (UTC).
	ObservedAt time.Time
	// EvidenceRef is the bounded evidence reference (never empty).
	EvidenceRef string
	// ScanTaskID attributes the observing 014 scan task (FK to recon_task).
	ScanTaskID string
}

// Validate checks the occurrence shape conservatively: the migration 000016
// CHECK requires a 1..512 byte evidence_ref and the FK requires a real scan
// task, so a malformed record is refused before it reaches the database.
func (r OccurrenceRecord) Validate() error {
	if r.DiscrepancyID == uuid.Nil {
		return contractErrorf("occurrence requires a discrepancy_id")
	}
	if _, err := uuid.Parse(r.ScanTaskID); err != nil {
		return contractErrorf("occurrence requires a scan_task_id UUID: %v", err)
	}
	if err := validateOccurrenceEvidenceRef(r.EvidenceRef); err != nil {
		return err
	}
	return nil
}

// validateOccurrenceEvidenceRef bounds one occurrence evidence reference to the
// migration CHECK (1..scanEvidenceRefMax bytes, no NUL).
func validateOccurrenceEvidenceRef(ref string) error {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" || len(trimmed) > scanEvidenceRefMax || strings.ContainsRune(trimmed, 0) {
		return contractErrorf("occurrence evidence_ref must be 1..%d bytes without NUL", scanEvidenceRefMax)
	}
	return nil
}

// insertOccurrenceRecordSQL appends one occurrence row with an explicit
// observed_at. The scan loop's own statement (scan.go) keeps the database
// now(); both write the same append-only table.
const insertOccurrenceRecordSQL = `
INSERT INTO discrepancy_occurrence (discrepancy_id, observed_at, evidence_ref, scan_task_id)
VALUES ($1, $2, $3, $4)`

// AppendOccurrenceTx appends one occurrence row inside the caller's
// transaction. The caller must already have created or locked the ticket (the
// dedup decision belongs to RecordDetectionTx); the append itself is
// insert-only and never mutates the ticket.
func AppendOccurrenceTx(ctx context.Context, tx pgx.Tx, rec OccurrenceRecord) error {
	if tx == nil {
		return contractErrorf("occurrence append requires a transaction")
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	observedAt := rec.ObservedAt.UTC()
	if rec.ObservedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	if _, err := tx.Exec(ctx, insertOccurrenceRecordSQL,
		rec.DiscrepancyID, observedAt, strings.TrimSpace(rec.EvidenceRef), rec.ScanTaskID); err != nil {
		return fmt.Errorf("insert discrepancy occurrence: %w", err)
	}
	return nil
}

// AppendOccurrence appends one occurrence row in its own short transaction.
// It is the standalone reappearance-evidence path for callers that do not
// already hold a transaction (e.g. audit/show tooling); the dedup/reopen path
// uses RecordDetection/RecordDetectionTx instead.
func (s *Store) AppendOccurrence(ctx context.Context, rec OccurrenceRecord) error {
	if s == nil || s.db == nil {
		return contractErrorf("store has no database")
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin occurrence append: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := AppendOccurrenceTx(ctx, tx, rec); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit occurrence append: %w", err)
	}
	return nil
}

// ValidateLinkedTicket checks one linked_to reference (data-model.md
// §1.4/§2): the target is a different existing ticket and a ticket never links
// to itself (the migration CHECK enforces the same invariant). The link is an
// association between distinct identities, never a merge: both tickets keep
// their own lifecycle and history.
func ValidateLinkedTicket(id Identity, linkedTo uuid.UUID) error {
	if !id.Valid() {
		return contractErrorf("linked ticket requires a minted identity")
	}
	if linkedTo == uuid.Nil {
		return contractErrorf("linked ticket requires a target discrepancy_id")
	}
	if linkedTo == id.ID() {
		return contractErrorf("a ticket must not link to itself")
	}
	return nil
}

// DetectionRecord is one ticketable detection to persist: the stable identity,
// the occurrence evidence, optional tx-aggregate member facts, an optional
// linked_to for a different identity, and the caller's recurrence judgment.
type DetectionRecord struct {
	// Identity is the minted stable identity (BuildIdentity); it must be
	// ticketable (only business divergence creates tickets, Q4).
	Identity Identity
	// ObservedAt is the occurrence observation time; zero means now (UTC).
	ObservedAt time.Time
	// EvidenceRef is the bounded occurrence evidence reference. Required
	// unless member facts are present (each member then carries its own
	// reference).
	EvidenceRef string
	// ScanTaskID attributes the detection to the observing 014 scan task.
	ScanTaskID string
	// Members enumerates the member log facts of a tx-aggregate detection
	// (T035): each member is appended as its own occurrence row and the set
	// never splits the transaction into several tickets.
	Members []TxAggregateMember
	// LinkedTo optionally associates a NEW ticket with an existing different
	// ticket (data-model.md §2). It is honored only on creation; a dedup onto
	// an existing ticket never rewrites the recorded link.
	LinkedTo uuid.UUID
	// ConfirmedRecurrence reports the caller's (T026) confirmed recurrence
	// judgment: closed/pending_verify tickets then reopen with history
	// retained. Unconfirmed re-detections only append evidence.
	ConfirmedRecurrence bool
	// Actor is the audit actor for reopen/invalidation rows; empty defaults
	// to "system:detection".
	Actor string
	// Reason is an optional audit reason (no secrets).
	Reason string
}

// DetectionResult reports what happened to the stable identity.
type DetectionResult struct {
	// DiscrepancyID is the ticket the detection was recorded against (the
	// original ticket on dedup, including tx-aggregate reorg replacements).
	DiscrepancyID uuid.UUID
	// Created reports that a new ticket row was inserted.
	Created bool
	// Deduped reports that an existing ticket was reused: no new ticket, only
	// occurrence evidence appended (SC-002).
	Deduped bool
	// Invalidated reports the Q5 tx-aggregate invalidation (changed evidence
	// version) applied to an existing ticket.
	Invalidated bool
	// Reopened reports a confirmed recurrence reopening the original ticket
	// (reopen_count+1, history retained).
	Reopened bool
	// ReopenCount is the ticket's reopen_count after the operation.
	ReopenCount int64
	// LinkedTo is the ticket's linked_to association (the requested link on
	// creation, the recorded link when read back on dedup/reopen).
	LinkedTo uuid.UUID
	// Occurrences is the number of appended occurrence rows.
	Occurrences int
}

// validateDetectionRecord checks the detection shape conservatively.
func validateDetectionRecord(rec DetectionRecord) error {
	if !rec.Identity.Valid() {
		return contractErrorf("detection record requires a minted identity")
	}
	if !rec.Identity.Ticketable() {
		return contractErrorf("category %q is not ticket-eligible (alert-only)", rec.Identity.Category())
	}
	if _, err := uuid.Parse(rec.ScanTaskID); err != nil {
		return contractErrorf("detection record requires a scan_task_id UUID: %v", err)
	}
	if strings.TrimSpace(rec.EvidenceRef) == "" && len(rec.Members) == 0 {
		return contractErrorf("detection record requires occurrence evidence")
	}
	if strings.TrimSpace(rec.EvidenceRef) != "" {
		if err := validateOccurrenceEvidenceRef(rec.EvidenceRef); err != nil {
			return err
		}
	}
	if rec.LinkedTo != uuid.Nil {
		if err := ValidateLinkedTicket(rec.Identity, rec.LinkedTo); err != nil {
			return err
		}
	}
	if len(rec.Members) > 0 {
		if !detectionIsTxAggregate(rec) {
			return contractErrorf("member facts are tx-aggregate evidence (missing + tx_hash); %s/%s is not an aggregate identity",
				rec.Identity.Category(), rec.Identity.BusinessKey().Kind)
		}
		if _, err := CanonicalizeTxAggregateMembers(rec.Members); err != nil {
			return err
		}
	}
	return nil
}

// detectionIsTxAggregate reports whether the detection follows the T035
// tx-aggregate identity rule: a missing divergence keyed by tx_hash.
func detectionIsTxAggregate(rec DetectionRecord) bool {
	return rec.Identity.Category() == CategoryMissing &&
		rec.Identity.BusinessKey().Kind == BusinessKeyTxHash
}

// detectionAggregateGroup builds the T035 ticket group of one tx-aggregate
// detection so the scan loop's keyed lookup (findTxAggregateRootTx) is reused
// verbatim: one ticket per (scope, missing, tx_hash) regardless of member set
// or reorg replacement.
func detectionAggregateGroup(rec DetectionRecord) (*scanTicketGroup, error) {
	if !detectionIsTxAggregate(rec) {
		return nil, contractErrorf("tx aggregate detection requires a missing tx_hash identity")
	}
	primary := Classification{
		Category:    rec.Identity.Category(),
		Ticket:      true,
		Identity:    rec.Identity,
		Members:     rec.Members,
		EvidenceRef: rec.EvidenceRef,
	}
	return &scanTicketGroup{
		aggregate:       true,
		businessKey:     scanBusinessKeyRecord(rec.Identity.BusinessKey()),
		primary:         primary,
		classifications: []Classification{primary},
	}, nil
}

// insertLinkedDiscrepancySQL inserts one stable-identity ticket, optionally
// linking a different identity. ON CONFLICT DO NOTHING is the dedup guard:
// the same identity can never mint a second ticket. The recorded link is only
// written here (creation) and is never cleared by dedup/invalidation/reopen.
// For a link-less creation the scan loop's own insert statement
// (insertDiscrepancySQL, scan.go) is reused so both paths always record the
// identical ticket shape.
const insertLinkedDiscrepancySQL = `
INSERT INTO discrepancy (
    discrepancy_id, category, business_key, content_hash, evidence_version_domain,
    state, linked_to)
VALUES ($1, $2, $3, $4, $5::jsonb, 'open_claimable', $6)
ON CONFLICT (discrepancy_id) DO NOTHING`

// insertDetectionTicketTx inserts the detection's ticket row and reports
// whether it was created. The persisted evidence domain carries the detection
// scope (T035 aggregate-root matching).
func insertDetectionTicketTx(ctx context.Context, tx pgx.Tx, rec DetectionRecord) (bool, error) {
	identity := rec.Identity
	scope := identity.Scope()
	versionDomain, err := PersistedEvidenceDomainJSON(identity.VersionDomain(), &scope)
	if err != nil {
		return false, err
	}
	args := []any{
		identity.ID(), string(identity.Category()), scanBusinessKeyRecord(identity.BusinessKey()),
		identity.ContentHash().Bytes(), string(versionDomain),
	}
	statement := insertDiscrepancySQL
	if rec.LinkedTo != uuid.Nil {
		if err := ValidateLinkedTicket(identity, rec.LinkedTo); err != nil {
			return false, err
		}
		statement = insertLinkedDiscrepancySQL
		args = append(args, rec.LinkedTo)
	}
	tag, err := tx.Exec(ctx, statement, args...)
	if err != nil {
		return false, fmt.Errorf("insert discrepancy: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// lockDetectionTicketSQL locks one ticket and reads the lifecycle fields the
// dedup/reopen path needs. linked_to is rendered as text so a malformed stored
// value is refused instead of guessed.
const lockDetectionTicketSQL = `
SELECT state, reopen_count::bigint, COALESCE(linked_to::text, '')
FROM discrepancy
WHERE discrepancy_id = $1
FOR UPDATE`

// detectionTicketState is the locked ticket state read back by the dedup path.
type detectionTicketState struct {
	State       DiscrepancyState
	ReopenCount int64
	LinkedTo    uuid.UUID
}

// lockDetectionTicketTx locks one ticket FOR UPDATE and materializes its
// lifecycle state. The lock only lives inside the caller's short transaction
// (never across RPC; data-model.md §5.1).
func lockDetectionTicketTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (detectionTicketState, error) {
	var (
		state   detectionTicketState
		rawLink string
	)
	err := tx.QueryRow(ctx, lockDetectionTicketSQL, id).
		Scan(&state.State, &state.ReopenCount, &rawLink)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, fmt.Errorf("%w: discrepancy_id %s", ErrDiscrepancyNotFound, id)
	}
	if err != nil {
		return state, fmt.Errorf("lock discrepancy ticket: %w", err)
	}
	if !state.State.Valid() {
		return state, contractErrorf("discrepancy %s has unknown state %q", id, state.State)
	}
	if rawLink != "" {
		linked, parseErr := uuid.Parse(rawLink)
		if parseErr != nil {
			return state, contractErrorf("discrepancy %s has malformed linked_to %q", id, rawLink)
		}
		state.LinkedTo = linked
	}
	return state, nil
}

// detectionAuditActor resolves the audit actor of one detection.
func detectionAuditActor(rec DetectionRecord) string {
	if actor := strings.TrimSpace(rec.Actor); actor != "" {
		return actor
	}
	return "system:detection"
}

// detectionAuditReason resolves the audit reason of one detection.
func detectionAuditReason(rec DetectionRecord) string {
	if reason := strings.TrimSpace(rec.Reason); reason != "" {
		return reason
	}
	return "confirmed recurrence of the same identity"
}

// reopenConfirmedRecurrenceTx applies the Q5 reopen rule to a locked ticket:
// EvaluateRecurrence (T007) decides, the guarded edge is validated, and the
// row moves to reopened with reopen_count+1 while every historical row stays
// untouched. The transition is audited; claimed/disposing tickets are never
// settled here (the lifecycle owner owns claims).
func reopenConfirmedRecurrenceTx(ctx context.Context, tx pgx.Tx, id uuid.UUID,
	state detectionTicketState, rec DetectionRecord) (bool, int64, error) {
	next, reopen := EvaluateRecurrence(state.State, true)
	if !reopen {
		return false, state.ReopenCount, nil
	}
	if err := ValidateDiscrepancyTransition(state.State, next, TransitionGuard{ConfirmedRecurrence: true}); err != nil {
		return false, state.ReopenCount, err
	}
	var reopenCount int64
	if err := tx.QueryRow(ctx, updateDiscrepancySQL, id, next, nil, state.State, "").Scan(&reopenCount); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, state.ReopenCount, fmt.Errorf("%w: concurrent state change", ErrIllegalDiscrepancyTransition)
		}
		return false, state.ReopenCount, fmt.Errorf("reopen discrepancy: %w", err)
	}
	if err := insertAuditTx(ctx, tx, AuditRecord{
		Actor:  detectionAuditActor(rec),
		Action: AuditActionReopen,
		Target: discrepancyAuditTarget(id.String(), state.State, next, reopenCount),
		Reason: detectionAuditReason(rec),
		Result: "reopened",
	}); err != nil {
		return false, state.ReopenCount, err
	}
	return true, reopenCount, nil
}

// invalidateAggregateDetectionTx applies the T035/Q5 invalidation to an
// existing tx-aggregate ticket whose evidence version changed (reorg
// replacement to a new block, changed member set): the row moves to
// pending_verify, the latest evidence replaces the recorded evidence, and an
// append-only reverify audit row documents the trigger. No automatic
// disposal/recovery/payment is ever triggered.
func invalidateAggregateDetectionTx(ctx context.Context, tx pgx.Tx, root *scanAggregateRoot, rec DetectionRecord) error {
	scope := rec.Identity.Scope()
	domain, err := PersistedEvidenceDomainJSON(rec.Identity.VersionDomain(), &scope)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, invalidateTxAggregateSQL,
		root.ID, rec.Identity.ContentHash().Bytes(), string(domain)); err != nil {
		return fmt.Errorf("invalidate tx aggregate ticket: %w", err)
	}
	return insertAuditTx(ctx, tx, AuditRecord{
		Actor:  detectionAuditActor(rec),
		Action: AuditActionReverify,
		Target: map[string]any{
			"discrepancy_id": root.ID.String(),
			"task_id":        rec.ScanTaskID,
			"business_key":   scanBusinessKeyRecord(rec.Identity.BusinessKey()),
			"recorded_block": strings.TrimSpace(root.Domain.BlockHash),
			"observed_block": strings.TrimSpace(rec.Identity.VersionDomain().BlockHash),
			"trigger":        string(InvalidationReorg),
		},
		Reason: "tx aggregate evidence changed (reorg replacement or new member evidence); pending reverify",
		Result: "invalidated",
	})
}

// detectionOccurrence renders one occurrence row of a detection. A member
// reference overrides the detection-level evidence reference (each member log
// is individually traceable, T035).
func detectionOccurrence(ticketID uuid.UUID, rec DetectionRecord, evidenceRef string) OccurrenceRecord {
	if strings.TrimSpace(evidenceRef) == "" {
		evidenceRef = rec.EvidenceRef
	}
	return OccurrenceRecord{
		DiscrepancyID: ticketID,
		ObservedAt:    rec.ObservedAt,
		EvidenceRef:   evidenceRef,
		ScanTaskID:    rec.ScanTaskID,
	}
}

// appendDetectionOccurrencesTx appends the detection's occurrence evidence:
// one row per canonical member log for a tx-aggregate detection (each member
// individually traceable), otherwise one row for the detection. Repeats never
// create a ticket; the append is the only side effect of a re-detection.
func appendDetectionOccurrencesTx(ctx context.Context, tx pgx.Tx, ticketID uuid.UUID, rec DetectionRecord) (int, error) {
	members, err := CanonicalizeTxAggregateMembers(rec.Members)
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		if err := AppendOccurrenceTx(ctx, tx, detectionOccurrence(ticketID, rec, "")); err != nil {
			return 0, err
		}
		return 1, nil
	}
	for _, member := range members {
		ref, refErr := TxAggregateEvidenceRef([]TxAggregateMember{member}, scanEvidenceRefMax)
		if refErr != nil {
			return 0, refErr
		}
		if err := AppendOccurrenceTx(ctx, tx, detectionOccurrence(ticketID, rec, ref)); err != nil {
			return 0, err
		}
	}
	return len(members), nil
}

// RecordDetectionTx records one ticketable detection inside the caller's
// transaction (the CommitScanBatch PersistResults callback, the T022 dispose
// path, or the T026 recurrence evaluator). It is the dedup/reopen/link core:
//
//   - a first detection inserts the ticket and its occurrence evidence;
//   - a repeat detection of the same identity appends occurrence rows only
//     (evidence_ref + scan_task_id) and reports Deduped;
//   - a tx-aggregate detection reuses the T035 keyed lookup, so a reorg
//     replacement stays on the original ticket (Invalidated + occurrence);
//   - a confirmed recurrence reopens a closed/pending_verify ticket with
//     reopen_count+1 and an audit row, preserving all history.
//
// The caller owns commit/rollback; errors leave the transaction untouched
// (except for PostgreSQL's normal row locks).
func RecordDetectionTx(ctx context.Context, tx pgx.Tx, rec DetectionRecord) (DetectionResult, error) {
	if tx == nil {
		return DetectionResult{}, contractErrorf("detection record requires a transaction")
	}
	if err := validateDetectionRecord(rec); err != nil {
		return DetectionResult{}, err
	}
	identity := rec.Identity
	result := DetectionResult{DiscrepancyID: identity.ID()}

	// Resolve the ticket: the tx-aggregate rule looks up by (category,
	// business key, scope) so a reorg replacement with a changed identity UUID
	// merges onto the original ticket; every other identity dedups by its
	// stable discrepancy_id.
	var (
		created   bool
		aggregate *scanTicketGroup
		root      *scanAggregateRoot
		state     detectionTicketState
	)
	if detectionIsTxAggregate(rec) {
		group, err := detectionAggregateGroup(rec)
		if err != nil {
			return DetectionResult{}, err
		}
		aggregate = group
		root, err = findTxAggregateRootTx(ctx, tx, group)
		if err != nil {
			return DetectionResult{}, err
		}
		if root != nil {
			result.DiscrepancyID = root.ID
		} else {
			created, err = insertDetectionTicketTx(ctx, tx, rec)
			if err != nil {
				return DetectionResult{}, err
			}
		}
	} else {
		inserted, err := insertDetectionTicketTx(ctx, tx, rec)
		if err != nil {
			return DetectionResult{}, err
		}
		created = inserted
	}

	// Existing ticket: lock it before appending evidence. The occurrence
	// insert's FK check takes a KEY SHARE lock on the ticket row, and
	// upgrading to FOR UPDATE afterwards could deadlock with a concurrent
	// appender; the lock is short and never spans RPC (data-model.md §5.1).
	if !created {
		var err error
		state, err = lockDetectionTicketTx(ctx, tx, result.DiscrepancyID)
		if err != nil {
			return DetectionResult{}, err
		}
		result.LinkedTo = state.LinkedTo
		result.ReopenCount = state.ReopenCount
		if root != nil {
			changed, err := txAggregateEvidenceChanged(root, aggregate.primary)
			if err != nil {
				return DetectionResult{}, err
			}
			if changed {
				if err := invalidateAggregateDetectionTx(ctx, tx, root, rec); err != nil {
					return DetectionResult{}, err
				}
				result.Invalidated = true
				// The invalidation moved the row (e.g. closed ->
				// pending_verify); refresh the locked state so a confirmed
				// recurrence reopens from the state actually recorded.
				state, err = lockDetectionTicketTx(ctx, tx, result.DiscrepancyID)
				if err != nil {
					return DetectionResult{}, err
				}
				result.ReopenCount = state.ReopenCount
			}
		}
	}
	result.Created = created
	result.Deduped = !created
	if created {
		result.LinkedTo = rec.LinkedTo
	}

	occurrences, err := appendDetectionOccurrencesTx(ctx, tx, result.DiscrepancyID, rec)
	if err != nil {
		return DetectionResult{}, err
	}
	result.Occurrences = occurrences

	if !created && rec.ConfirmedRecurrence {
		reopened, reopenCount, err := reopenConfirmedRecurrenceTx(ctx, tx, result.DiscrepancyID, state, rec)
		if err != nil {
			return DetectionResult{}, err
		}
		result.Reopened = reopened
		result.ReopenCount = reopenCount
	}
	return result, nil
}

// RecordDetection records one ticketable detection in its own short
// transaction (see RecordDetectionTx for the dedup/reopen/link semantics).
func (s *Store) RecordDetection(ctx context.Context, rec DetectionRecord) (DetectionResult, error) {
	if s == nil || s.db == nil {
		return DetectionResult{}, contractErrorf("store has no database")
	}
	if err := validateDetectionRecord(rec); err != nil {
		return DetectionResult{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return DetectionResult{}, fmt.Errorf("begin detection record: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	result, err := RecordDetectionTx(ctx, tx, rec)
	if err != nil {
		return DetectionResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DetectionResult{}, fmt.Errorf("commit detection record: %w", err)
	}
	return result, nil
}
