// identity.go implements T005: stable discrepancy identity and evidence
// hashing for 014 reconciliation (data-model.md §2, research §3).
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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
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

// String renders a compact identity summary for logs and audit details. It
// carries no secret material.
func (id Identity) String() string {
	if !id.Valid() {
		return "reconciliation-identity[invalid]"
	}
	return fmt.Sprintf("reconciliation-identity[%s category=%s business_key=%s/%s id=%s]",
		id.scope, id.category, id.businessKey.Kind, id.businessKey.Value, id.id)
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
