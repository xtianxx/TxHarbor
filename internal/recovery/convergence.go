package recovery

// convergence.go implements the R2 limited-ruling deployment-lane step
// (2026-10-04): after the restricted recovery role restores objects
// natively (--no-owner --no-privileges), an independent deployment-management
// identity converges object ownership to the original writer role and records
// an audited prerequisite. The coordinator accepts restore evidence and
// resolves the target guard to clean only when that audited convergence row
// exists for the exact operation and target key (see
// requireDeploymentConvergenceAudit in targetwriter_linux.go).
//
// Boundaries:
//   - This step never runs as the recovery role and never inside the restore
//     child session; it is a separate privileged connection.
//   - It is a fail-closed verification+repair: every public relation must end
//     owned by the expected original writer role, otherwise the step refuses
//     and no audit row is written.
//   - It carries no credential material into audit/log output; only relation
//     counts and the target guard key are recorded.
//   - Production wiring of the admin DSN is a deployment prerequisite; an
//     unconfigured bound restore refuses at the coordinator (no bypass).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ActionDeploymentConvergence is the recovery_audit action of the R2
// deployment-lane ownership convergence step.
const ActionDeploymentConvergence = "deployment_convergence"

// ConvergenceRequest is the coordinator-supplied binding for one convergence
// step: the exact attempt operation, instance and target guard key the
// acceptance transaction will verify.
type ConvergenceRequest struct {
	OperationID    string
	InstanceID     string
	TargetGuardKey string
}

// ConvergenceStep is the deployment-lane convergence hook of the target
// writer coordinator (R2 limited ruling).
type ConvergenceStep func(context.Context, ConvergenceRequest) error

// DeploymentConvergence is one deployment-lane convergence step for a bound
// restore attempt. AdminDSN must address the same PostgreSQL cluster as
// TargetDSN (host/port equality) with a privileged deployment-management
// identity; the step alters ownership of every public relation in the target
// database to OriginalRole and then verifies the result.
type DeploymentConvergence struct {
	AdminDSN     string
	TargetDSN    string
	OriginalRole string
	Actor        string
	Store        *controlstore.Store
	Timeout      time.Duration
}

// Converge executes the convergence and records the audited prerequisite.
func (c DeploymentConvergence) Converge(ctx context.Context, req ConvergenceRequest) error {
	if c.AdminDSN == "" || c.TargetDSN == "" || c.OriginalRole == "" || req.OperationID == "" ||
		req.TargetGuardKey == "" || c.Store == nil {
		return errors.New("deployment convergence requires admin/target DSNs, role, operation, guard key and control store")
	}
	adminURL, err := url.Parse(c.AdminDSN)
	if err != nil || (adminURL.Scheme != "postgres" && adminURL.Scheme != "postgresql") || adminURL.User == nil {
		return errors.New("deployment convergence admin DSN must be a postgres URI with credentials")
	}
	targetURL, err := url.Parse(c.TargetDSN)
	if err != nil || (targetURL.Scheme != "postgres" && targetURL.Scheme != "postgresql") || targetURL.User == nil {
		return errors.New("deployment convergence target DSN must be a postgres URI")
	}
	if !strings.EqualFold(adminURL.Hostname(), targetURL.Hostname()) || adminURL.Port() != targetURL.Port() {
		return errors.New("deployment convergence admin connection must address the same cluster as the target")
	}
	targetDB := strings.Trim(targetURL.Path, "/")
	if targetDB == "" || strings.ContainsAny(targetDB, " =\t\r\n") {
		return errors.New("deployment convergence target database name is unusable")
	}
	// The admin connection acts on the target database, never on whatever
	// database the admin DSN names by default.
	convergeURL := *adminURL
	convergeURL.Path = "/" + targetDB
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := pgx.Connect(runCtx, convergeURL.String())
	if err != nil {
		return errors.New("deployment convergence could not open the deployment-admin connection")
	}
	defer func() { _ = conn.Close(context.Background()) }()

	type relation struct {
		name  string
		kind  string
		owner string
	}
	read := func() ([]relation, error) {
		rows, err := conn.Query(runCtx, `
SELECT c.relname, c.relkind::text, pg_get_userbyid(c.relowner)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','p','S','v','m','f')
ORDER BY c.relname`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []relation
		for rows.Next() {
			var r relation
			if err := rows.Scan(&r.name, &r.kind, &r.owner); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, rows.Err()
	}
	before, err := read()
	if err != nil {
		return errors.New("deployment convergence could not enumerate target relations")
	}
	altered := 0
	for _, r := range before {
		if r.owner == c.OriginalRole {
			continue
		}
		kind := map[string]string{"r": "TABLE", "p": "TABLE", "S": "SEQUENCE", "v": "VIEW", "m": "MATERIALIZED VIEW", "f": "FOREIGN TABLE"}[r.kind]
		if kind == "" {
			return errors.New("deployment convergence met an unsupported relation kind")
		}
		stmt := fmt.Sprintf("ALTER %s %s OWNER TO %s", kind, pgx.Identifier{r.name}.Sanitize(), pgx.Identifier{c.OriginalRole}.Sanitize())
		if _, err := conn.Exec(runCtx, stmt); err != nil {
			return fmt.Errorf("deployment convergence could not reassign ownership of %s", pgx.Identifier{r.name}.Sanitize())
		}
		altered++
	}
	after, err := read()
	if err != nil {
		return errors.New("deployment convergence could not re-verify target relations")
	}
	for _, r := range after {
		if r.owner != c.OriginalRole {
			return errors.New("deployment convergence left a relation owned by another role")
		}
	}
	targetJSON, _ := json.Marshal(map[string]any{"target_guard_key": req.TargetGuardKey})
	detailJSON, _ := json.Marshal(map[string]any{
		"relations": len(after), "altered": altered, "original_role": c.OriginalRole,
	})
	return controlstore.WriteAudit(ctx, c.Store.Pool(), controlstore.AuditRecord{
		InstanceID:  req.InstanceID,
		Actor:       c.Actor,
		Action:      ActionDeploymentConvergence,
		Target:      targetJSON,
		Detail:      detailJSON,
		Result:      controlstore.AuditOK,
		OperationID: req.OperationID,
	})
}
