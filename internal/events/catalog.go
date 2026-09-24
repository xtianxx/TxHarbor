// Package events owns the 013 reliable event infrastructure: the catalog v1
// event contract (envelope, identity derivation, payload canonicalization),
// the transactional outbox Append, the publish-state guards and the outbox
// queries the publisher and capacity guard drive.
//
// Boundary (T016; research R18; plan Structure Decision): this package never
// imports an upstream writer package (indexer/withdrawal/execution/
// txlifecycle/nonce), a signer or key-material package, or an RPC/dial
// package. Upstream business transactions call Append from inside their own
// transaction, so the dependency arrow is upstream -> events only. No
// triggers, no CDC, no network publish inside a transaction.
package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CatalogVersionV1 is the catalog version seeded into
// event_system_state.catalog_version by migration 000015.
const CatalogVersionV1 = 1

// SchemaVersionV1 is the first payload schema version. schema_version starts
// at 1 for every catalog type; breaking payload changes MUST add a new version
// and keep the previous one inside the compatibility window (FR-12;
// contracts/events.md §4).
const SchemaVersionV1 = 1

// Catalog v1 event types (contracts/events.md §3). Names are
// <domain>.<object>.<fact>; existing type semantics MUST NOT change in place.
const (
	EventTypeDepositObservationCreated       = "deposit.observation.created"
	EventTypeDepositObservationStatusChanged = "deposit.observation.status_changed"
	EventTypeDepositObservationReinstated    = "deposit.observation.reinstated"
	EventTypeDepositConfirmationConfirmed    = "deposit.confirmation.confirmed"
	EventTypeDepositRevisionApplied          = "deposit.revision.applied"
	EventTypeWithdrawalRequestReceived       = "withdrawal.request.received"
	EventTypeWithdrawalExecutionStateChanged = "withdrawal.execution.state_changed"
	EventTypeWithdrawalExecutionRevised      = "withdrawal.execution.revised"
)

// IdentityKind is the closed set of event identity kinds (FR-09;
// contracts/events.md §2).
type IdentityKind string

const (
	// IdentityKindEVMLog identifies an event by its source log
	// (chain_id, block_hash, tx_hash, log_index). Block height is never an
	// identity component by itself (003 contract).
	IdentityKindEVMLog IdentityKind = "evm_log"
	// IdentityKindBusinessObject identifies an event by its business object
	// and emission-stream version (aggregate_type, aggregate_id,
	// aggregate_version). Business identity never depends on delivery
	// attempts, timestamps or producer instances.
	IdentityKindBusinessObject IdentityKind = "business_object"
)

// Valid reports whether k is a catalog v1 identity kind.
func (k IdentityKind) Valid() bool {
	return k == IdentityKindEVMLog || k == IdentityKindBusinessObject
}

// Event is the catalog v1 envelope plus the outbox source columns
// (contracts/events.md §1; data-model §2 Table 1). Fields are caller input;
// AggregateVersion may be zero, in which case Append derives
// coalesce(max(aggregate_version),0)+1 inside the caller's transaction
// (data-model §3.2).
//
// Payload MUST NOT be mutated after NewEvent: the canonical bytes and hash are
// frozen there so retries and replays reuse the exact bytes and never
// re-serialize with drift (data-model §3.5).
type Event struct {
	EventType     string
	SchemaVersion int
	IdentityKind  IdentityKind

	AggregateType    string
	AggregateID      string
	AggregateVersion int64

	Payload    map[string]any
	OccurredAt time.Time

	// Chain identity: required for evm_log events and for chain-derived
	// revision events (contracts/events.md §1).
	ChainID     int64
	BlockNumber int64
	BlockHash   string
	TxHash      string
	LogIndex    int

	// Revision envelope: required for the two revision event types (FR-11;
	// contracts/events.md §5).
	RecoveryVersion int64
	RevisesEventID  uuid.UUID

	// Source watermark for the reconciliation audit (optional; data-model §4).
	SourceKind    string
	SourceID      string
	SourceVersion int64

	// Canonical payload bytes/hash frozen by NewEvent (or Append). Unexported
	// on purpose: callers never hand-serialize payloads.
	payloadJSON []byte
	payloadHash string
}

// EventSpec is one catalog v1 declaration.
type EventSpec struct {
	Type                string
	IdentityKind        IdentityKind
	RequiredPayloadKeys []string
	// RevisionRequired marks the two revision types: the envelope MUST carry
	// revises_event_id, recovery_version and the chain identity (FR-11;
	// contracts/events.md §5).
	RevisionRequired bool
	// SchemaVersions is the compatibility window: every listed version stays
	// routable for this type. Removing a version is a breaking change (FR-12).
	SchemaVersions []int
}

// SupportsSchemaVersion reports whether v is inside the type's compatibility
// window.
func (s EventSpec) SupportsSchemaVersion(v int) bool {
	for _, sv := range s.SchemaVersions {
		if sv == v {
			return true
		}
	}
	return false
}

// catalogV1 is the frozen catalog v1 declaration, in contract order
// (contracts/events.md §3). CatalogV1 returns copies; this table is never
// mutated after init.
var catalogV1 = []EventSpec{
	{
		Type:                EventTypeDepositObservationCreated,
		IdentityKind:        IdentityKindEVMLog,
		RequiredPayloadKeys: []string{"observation_id", "state"},
		SchemaVersions:      []int{SchemaVersionV1},
	},
	{
		Type:                EventTypeDepositObservationStatusChanged,
		IdentityKind:        IdentityKindBusinessObject,
		RequiredPayloadKeys: []string{"from_state", "to_state", "reason"},
		SchemaVersions:      []int{SchemaVersionV1},
	},
	{
		Type:                EventTypeDepositObservationReinstated,
		IdentityKind:        IdentityKindBusinessObject,
		RequiredPayloadKeys: []string{"observation_id", "reason"},
		SchemaVersions:      []int{SchemaVersionV1},
	},
	{
		Type:                EventTypeDepositConfirmationConfirmed,
		IdentityKind:        IdentityKindBusinessObject,
		RequiredPayloadKeys: []string{"policy_version", "confirmed_block_number", "confirmed_block_hash"},
		SchemaVersions:      []int{SchemaVersionV1},
	},
	{
		Type:                EventTypeDepositRevisionApplied,
		IdentityKind:        IdentityKindBusinessObject,
		RequiredPayloadKeys: []string{"superseded_identity", "to_state", "reason"},
		RevisionRequired:    true,
		SchemaVersions:      []int{SchemaVersionV1},
	},
	{
		Type:                EventTypeWithdrawalRequestReceived,
		IdentityKind:        IdentityKindBusinessObject,
		RequiredPayloadKeys: []string{"request_id", "caller", "state"},
		SchemaVersions:      []int{SchemaVersionV1},
	},
	{
		Type:                EventTypeWithdrawalExecutionStateChanged,
		IdentityKind:        IdentityKindBusinessObject,
		RequiredPayloadKeys: []string{"from_state", "to_state", "intent_id"},
		SchemaVersions:      []int{SchemaVersionV1},
	},
	{
		Type:                EventTypeWithdrawalExecutionRevised,
		IdentityKind:        IdentityKindBusinessObject,
		RequiredPayloadKeys: []string{"superseded_identity", "to_state", "reason"},
		RevisionRequired:    true,
		SchemaVersions:      []int{SchemaVersionV1},
	},
}

// CatalogV1 returns a deep copy of the frozen catalog v1 declaration.
func CatalogV1() []EventSpec {
	out := make([]EventSpec, len(catalogV1))
	for i, spec := range catalogV1 {
		out[i] = spec
		out[i].RequiredPayloadKeys = append([]string(nil), spec.RequiredPayloadKeys...)
		out[i].SchemaVersions = append([]int(nil), spec.SchemaVersions...)
	}
	return out
}

// LookupEventSpec returns the catalog declaration for eventType.
func LookupEventSpec(eventType string) (EventSpec, bool) {
	for _, spec := range catalogV1 {
		if spec.Type == eventType {
			return spec, true
		}
	}
	return EventSpec{}, false
}

// ResolveRoute resolves (event_type, schema_version) to its declaration
// (FR-12; contracts/events.md §4.1). Unknown types and versions outside the
// compatibility window are not routable; callers MUST handle that fail-closed.
func ResolveRoute(eventType string, schemaVersion int) (EventSpec, bool) {
	spec, ok := LookupEventSpec(eventType)
	if !ok || !spec.SupportsSchemaVersion(schemaVersion) {
		return EventSpec{}, false
	}
	return spec, true
}

// RouteEvent is the fail-closed routing form for consumers: unknown event
// types return ErrContract and unsupported versions return
// ErrSchemaUnsupported, so callers can quarantine with the right reason
// (contracts/events.md §4.5).
func RouteEvent(eventType string, schemaVersion int) (EventSpec, error) {
	spec, ok := LookupEventSpec(eventType)
	if !ok {
		return EventSpec{}, contractErrorf("unknown event_type %q", eventType)
	}
	if !spec.SupportsSchemaVersion(schemaVersion) {
		return EventSpec{}, fmt.Errorf("%w: %s schema_version %d", ErrSchemaUnsupported, eventType, schemaVersion)
	}
	return spec, nil
}

// EventFamily returns the <domain> segment of an event type, used as the
// capacity/metric family label. It never carries business identity.
func EventFamily(eventType string) string {
	if i := strings.IndexByte(eventType, '.'); i > 0 {
		return eventType[:i]
	}
	return eventType
}

// eventIDNamespace freezes the UUIDv5 namespace for catalog v1 identities.
// uuid.NameSpaceURL is used so derivation depends only on the documented
// natural-key strings below. Changing this value changes every derived
// event_id and is a catalog-breaking change (FR-12).
var eventIDNamespace = uuid.NameSpaceURL

// EVMLogNaturalKey builds the documented evm_log natural key
// "evm_log|chain|block_hash|tx_hash|log_index" (contracts/events.md §2).
func EVMLogNaturalKey(chainID int64, blockHash, txHash string, logIndex int) string {
	return fmt.Sprintf("evm_log|%d|%s|%s|%d", chainID, blockHash, txHash, logIndex)
}

// BusinessObjectNaturalKey builds the documented business_object natural key
// "business_object|type|id|version".
func BusinessObjectNaturalKey(aggregateType, aggregateID string, aggregateVersion int64) string {
	return fmt.Sprintf("business_object|%s|%s|%d", aggregateType, aggregateID, aggregateVersion)
}

// NewEventID derives the deterministic UUIDv5 event_id of a natural key: the
// same fact always yields the same identity (FR-09; research R2).
func NewEventID(naturalKey string) uuid.UUID {
	return uuid.NewSHA1(eventIDNamespace, []byte(naturalKey))
}

// CanonicalPayload returns the canonical JSON bytes of a payload. Go's
// encoding/json sorts object keys, so the same logical payload always yields
// identical bytes regardless of map iteration order (data-model §3.5). The
// payload must be a non-nil JSON object.
func CanonicalPayload(payload map[string]any) ([]byte, error) {
	if payload == nil {
		return nil, contractErrorf("payload is required")
	}
	return json.Marshal(payload)
}

// PayloadHash returns the lowercase hex sha256 of canonical payload bytes.
func PayloadHash(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// pemMarker is the PEM header prefix that marks raw key material; payload
// string values carrying it are rejected (data-model §9).
const pemMarker = "-----BEGIN"

// forbiddenPayloadKeyTokens are the credential/key-material key-name tokens a
// payload MUST NOT carry (data-model §9; research R18). Keys are compared
// after lowercasing and stripping '_'/'-', so privateKey, private_key and
// private-key are all rejected.
var forbiddenPayloadKeyTokens = []string{
	"privatekey", "privkey", "secret", "mnemonic", "seedphrase",
	"password", "passwd", "apikey", "credential", "signature",
	"signedtx", "rawtx", "rawtransaction", "keystore",
}

// ForbiddenPayload reports the first forbidden key or secret marker found in
// payload (recursing through nested objects and arrays) and whether one
// exists. String values carrying a PEM marker are reported as
// "<value:PEM>". This is the runtime half of the T016 payload scan.
func ForbiddenPayload(payload map[string]any) (string, bool) {
	return scanForbidden(payload)
}

func scanForbidden(value any) (string, bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, nested := range v {
			if isForbiddenPayloadKey(key) {
				return key, true
			}
			if found, ok := scanForbidden(nested); ok {
				return found, true
			}
		}
	case []any:
		for _, nested := range v {
			if found, ok := scanForbidden(nested); ok {
				return found, true
			}
		}
	case string:
		if strings.Contains(v, pemMarker) {
			return "<value:PEM>", true
		}
	}
	return "", false
}

func isForbiddenPayloadKey(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(key))
	for _, token := range forbiddenPayloadKeyTokens {
		if strings.Contains(normalized, token) {
			return true
		}
	}
	return false
}

// ValidateEnvelope checks every catalog v1 envelope invariant on a fully
// materialized event (aggregate_version > 0). It is the transport-side
// contract check (contracts/events.md §1–§2, §4–§5).
func ValidateEnvelope(ev Event) error { return validateEvent(ev, true) }

// validateEvent is the single envelope validator; requireVersion is false on
// the Append path, where the version is derived inside the transaction.
func validateEvent(ev Event, requireVersion bool) error {
	spec, ok := LookupEventSpec(ev.EventType)
	if !ok {
		return contractErrorf("unknown event_type %q", ev.EventType)
	}
	if !spec.SupportsSchemaVersion(ev.SchemaVersion) {
		return contractErrorf("event_type %s does not support schema_version %d", ev.EventType, ev.SchemaVersion)
	}
	if ev.IdentityKind != spec.IdentityKind {
		return contractErrorf("event_type %s requires identity_kind %s, got %s", ev.EventType, spec.IdentityKind, ev.IdentityKind)
	}
	if ev.AggregateType == "" || ev.AggregateID == "" {
		return contractErrorf("aggregate_type and aggregate_id are required")
	}
	if requireVersion && ev.AggregateVersion <= 0 {
		return contractErrorf("aggregate_version must be > 0")
	}
	if ev.OccurredAt.IsZero() {
		return contractErrorf("occurred_at is required")
	}
	if ev.Payload == nil {
		return contractErrorf("payload is required")
	}
	if ev.IdentityKind == IdentityKindEVMLog {
		if ev.ChainID <= 0 || ev.BlockNumber <= 0 || ev.BlockHash == "" || ev.TxHash == "" || ev.LogIndex < 0 {
			return contractErrorf("evm_log event %s requires chain_id, block_number, block_hash, tx_hash and log_index", ev.EventType)
		}
	}
	if spec.RevisionRequired {
		if ev.RevisesEventID == uuid.Nil {
			return contractErrorf("revision event %s requires revises_event_id", ev.EventType)
		}
		if ev.RecoveryVersion <= 0 {
			return contractErrorf("revision event %s requires recovery_version > 0", ev.EventType)
		}
		if ev.ChainID <= 0 || ev.BlockNumber <= 0 || ev.BlockHash == "" {
			return contractErrorf("chain-derived revision %s requires chain_id, block_number and block_hash", ev.EventType)
		}
	}
	// Mirror of the outbox_events_revision_shape CHECK: a revision reference
	// always carries a recovery version.
	if ev.RevisesEventID != uuid.Nil && ev.RecoveryVersion <= 0 {
		return contractErrorf("revises_event_id requires recovery_version")
	}
	for _, key := range spec.RequiredPayloadKeys {
		value, present := ev.Payload[key]
		if !present || value == nil {
			return contractErrorf("payload of %s is missing required key %q", ev.EventType, key)
		}
	}
	if key, found := ForbiddenPayload(ev.Payload); found {
		return contractErrorf("payload of %s carries forbidden material %q", ev.EventType, key)
	}
	return nil
}

// NewEvent validates and canonicalizes an event. The canonical payload bytes
// and hash are frozen on the returned event so retries and replays reuse the
// exact bytes (data-model §3.5). AggregateVersion may be zero; Append derives
// it inside the caller's transaction.
func NewEvent(ev Event) (Event, error) {
	if err := validateEvent(ev, false); err != nil {
		return Event{}, err
	}
	canonical, err := CanonicalPayload(ev.Payload)
	if err != nil {
		return Event{}, contractErrorf("canonicalize payload: %v", err)
	}
	ev.payloadJSON = canonical
	ev.payloadHash = PayloadHash(canonical)
	return ev, nil
}

// PayloadBytes returns the frozen canonical payload bytes (nil before
// NewEvent/Append).
func (ev Event) PayloadBytes() []byte { return ev.payloadJSON }

// PayloadHash returns the frozen canonical payload hash (empty before
// NewEvent/Append).
func (ev Event) PayloadHash() string { return ev.payloadHash }

// prepare re-validates an event and freezes canonical payload bytes when the
// caller did not go through NewEvent. Append is the only caller.
func (ev Event) prepare() (Event, error) {
	if err := validateEvent(ev, false); err != nil {
		return Event{}, err
	}
	if ev.payloadJSON == nil {
		canonical, err := CanonicalPayload(ev.Payload)
		if err != nil {
			return Event{}, contractErrorf("canonicalize payload: %v", err)
		}
		ev.payloadJSON = canonical
		ev.payloadHash = PayloadHash(canonical)
	}
	return ev, nil
}
