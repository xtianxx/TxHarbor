// capacity.go owns the T089 production assembly of the 013 capacity guard
// (PD-2; contracts/capacity.md): the serve process constructs one
// events.CapacityGuard from the T001 configuration, wires the metrics registry
// as its observer, passes it to the 007 withdrawal receive path as the
// admission gate for new controllable work (T070) and to the 003/004 streams
// as the hard-boundary pause trigger (T090), and refreshes the observation on
// the serve metric cadence (lifecycle: started with serve, stopped with the
// run context).
//
// Boundary rules (MUST NOT be weakened):
//
//   - The guard only decides about admitting NEW controllable work and about
//     pausing chain processing when persistence is not safe. It never gates an
//     accepted unit, never authenticates, never authorizes and never
//     participates in any funding decision for an existing request.
//   - Redis never participates: the observation reads PostgreSQL only.
//   - A configured but invalid limit set refuses startup (fail closed); an
//     unconfigured limit set leaves the PG-only baseline unchanged (no guard,
//     pre-013 behavior).
package app

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/events"
)

// buildCapacityGuard constructs the single production capacity guard from the
// T001 configuration. It returns (nil, nil) when no capacity limit is
// configured (PG-only baseline: the withdrawal path stays exactly as before).
// A fully/partially configured set is validated fail-closed: an invalid
// formula or a missing pool refuses construction and the caller must refuse
// startup.
func buildCapacityGuard(pool *pgxpool.Pool, cfg *config.Config, observer events.CapacityObserver) (*events.CapacityGuard, error) {
	limits := events.CapacityLimits{
		Reserve:   cfg.Capacity.Reserve,
		SoftLimit: cfg.Capacity.SoftLimit,
		HardLimit: cfg.Capacity.HardLimit,
	}
	if !limits.Configured() {
		return nil, nil
	}
	return events.NewCapacityGuard(pool, limits, observer)
}
