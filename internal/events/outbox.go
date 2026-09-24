package events

import (
	"math"
	"time"
)

// PublishState is the outbox publish state machine (data-model §8).
type PublishState string

const (
	// PublishStatePending: awaiting publication; the publisher's only input.
	PublishStatePending PublishState = "pending"
	// PublishStatePublished: broker-acknowledged; retained until the
	// retention prune. Never means "consumed".
	PublishStatePublished PublishState = "published"
	// PublishStateBlocked: a permanent failure, visible and auditable; never
	// dropped. Only an audited unblock returns it to pending.
	PublishStateBlocked PublishState = "blocked"
)

// Valid reports whether s is a publish state.
func (s PublishState) Valid() bool {
	switch s {
	case PublishStatePending, PublishStatePublished, PublishStateBlocked:
		return true
	}
	return false
}

// CanTransition reports whether the publish-state machine allows from -> to
// (data-model §8): pending -> published (claim + ack), pending -> blocked
// (permanent class) and blocked -> pending (audited unblock). Every other
// transition — including published -> pending/blocked, blocked -> published,
// self transitions and unknown states — is refused. The guarded UPDATEs below
// enforce the same rule: an illegal transition updates zero rows.
func CanTransition(from, to PublishState) bool {
	switch from {
	case PublishStatePending:
		return to == PublishStatePublished || to == PublishStateBlocked
	case PublishStateBlocked:
		return to == PublishStatePending
	}
	return false
}

// ClaimPendingSQL selects the next claimable batch for the publisher
// (contracts/outbox-publisher.md §1.1; research R6). Parameters: $1 = batch
// size. The caller runs it inside the claim transaction; the following claim
// UPDATE and the commit are part of the same transaction, and the network
// publish happens strictly after the commit.
const ClaimPendingSQL = `
SELECT id
FROM outbox_events
WHERE publish_state = 'pending' AND next_attempt_at <= now()
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`

// ClaimMarkSQL stamps the claimed rows with the owner and a bounded lease
// (contracts/outbox-publisher.md §1.1). Parameters: $1 = id array, $2 = owner
// instance id, $3 = lease seconds. A row that is no longer pending updates
// zero rows.
const ClaimMarkSQL = `
UPDATE outbox_events
SET claim_owner = $2, claim_expires_at = now() + make_interval(secs => $3::double precision)
WHERE id = ANY($1) AND publish_state = 'pending'
RETURNING id`

// AckPublishedSQL marks broker-acknowledged rows published (T3;
// contracts/outbox-publisher.md §1.3). Parameters: $1 = id array, $2 = owner
// instance id. The claim_owner guard makes a stale or wrong owner update zero
// rows; pending is required, so published -> published is refused.
const AckPublishedSQL = `
UPDATE outbox_events
SET publish_state = 'published', published_at = now(),
    claim_owner = NULL, claim_expires_at = NULL
WHERE id = ANY($1) AND claim_owner = $2 AND publish_state = 'pending'
RETURNING id`

// ReleaseClaimSQL returns transient-failure rows to the retry queue with a
// bounded backoff (contracts/outbox-publisher.md §4). Parameters: $1 = id
// array, $2 = owner instance id, $3 = backoff seconds, $4 = error class. The
// row stays pending and is never dropped.
const ReleaseClaimSQL = `
UPDATE outbox_events
SET claim_owner = NULL, claim_expires_at = NULL,
    next_attempt_at = now() + make_interval(secs => $3::double precision),
    last_error_class = $4
WHERE id = ANY($1) AND claim_owner = $2 AND publish_state = 'pending'
RETURNING id`

// BlockClaimSQL blocks claimed rows on a permanent class (contracts/
// outbox-publisher.md §4): the row becomes visible/auditable and is never
// dropped. Parameters: $1 = id array, $2 = owner instance id, $3 = error
// class. pending -> blocked is the only allowed direction.
const BlockClaimSQL = `
UPDATE outbox_events
SET publish_state = 'blocked', last_error_class = $3,
    claim_owner = NULL, claim_expires_at = NULL
WHERE id = ANY($1) AND claim_owner = $2 AND publish_state = 'pending'
RETURNING id`

// UnblockSQL returns a blocked row to pending after an audited operator
// decision (T045 records event_ops_audit in the same transaction; PD-4).
// Parameters: $1 = outbox id. A row that is not blocked updates zero rows, so
// blocked -> published cannot happen and published rows cannot be reopened.
const UnblockSQL = `
UPDATE outbox_events
SET publish_state = 'pending', last_error_class = NULL, next_attempt_at = now()
WHERE id = $1 AND publish_state = 'blocked'
RETURNING id`

// CapacitySQL observes the pending backlog by event family for
// outbox_pending_count / outbox_pending_oldest_age_seconds
// (verification.md §1; data-model §6). It reads the PG partial index only —
// Redis never participates. Rows: (event_family, pending_count,
// oldest_age_seconds).
const CapacitySQL = `
SELECT split_part(event_type, '.', 1) AS event_family,
       count(*)::bigint AS pending_count,
       coalesce(extract(epoch FROM now() - min(created_at)), 0)::double precision AS oldest_age_seconds
FROM outbox_events
WHERE publish_state = 'pending'
GROUP BY 1
ORDER BY 1`

// PruneWatermarkSQL reads the retention watermark before a prune so
// events-admin can record it in event_ops_audit (T045). Parameters: $1 =
// retention seconds. Read-only; rows: (prunable_count, oldest_published_at).
const PruneWatermarkSQL = `
SELECT count(*)::bigint,
       coalesce(min(published_at), now())
FROM outbox_events
WHERE publish_state = 'published'
  AND published_at < now() - make_interval(secs => $1::double precision)`

// PruneSQL deletes only published rows older than the retention window
// (data-model §5 T7; contracts/outbox-publisher.md §6). Parameters: $1 =
// retention seconds. The publish_state filter is the red line: pending and
// blocked rows are NEVER deleted or overwritten by retention.
const PruneSQL = `
DELETE FROM outbox_events
WHERE publish_state = 'published'
  AND published_at < now() - make_interval(secs => $1::double precision)
RETURNING id`

// RetryDelay computes the bounded exponential backoff for one publish retry:
// base * 2^(attempt-1), capped at max, then multiplied by a ±20% jitter
// (contracts/outbox-publisher.md §4; base/cap are configuration values — the
// initial 1s/60s are calibrated by measurement, never treated as business
// thresholds). jitter returns a value in [-1,1); nil means no jitter. attempt
// is the 1-based attempt number after the failed one.
func RetryDelay(attempt int, base, max time.Duration, jitter func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = base
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= max {
			delay = max
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	if jitter != nil {
		delay = time.Duration(math.Round(float64(delay) * (1 + 0.2*jitter())))
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}
