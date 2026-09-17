// withdrawalexec.go owns the 011 privileged operator surface
// (`txharbor withdrawal-exec`, contracts/api.md §3). Every mutation is
// deduplicated by execution_ops_audit.operation_id: a reused operation_id with
// the same input reports the recorded outcome, a different input is
// operation_conflict with zero writes (008 R7 protocol). Operator identity and
// reason are audit fields, required for mutations. Read-only inspections write
// no audit row.
package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
)

// errOperationConflict means an operation_id was reused with different input;
// no write was performed.
var errOperationConflict = errors.New("operation_conflict")

// WithdrawalExec runs the operator subcommand. It is the only CLI that writes
// 011's operator-managed rows; every mutation is audited and deduplicated.
func WithdrawalExec(ctx context.Context, args []string, d Deps) int {
	if len(args) == 0 {
		withdrawalExecUsage(d.stderr())
		return 2
	}
	switch args[0] {
	case "permission-set":
		return withdrawalExecPermissionSet(ctx, args[1:], d)
	case "permission-revoke":
		return withdrawalExecPermissionRevoke(ctx, args[1:], d)
	default:
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor withdrawal-exec: unknown action %q\n", args[0])
		withdrawalExecUsage(stderr)
		return 2
	}
}

// operatorOp is one audited operator mutation. Detail carries the canonical
// business input used for dedup comparison.
type operatorOp struct {
	OperationID    string
	Action         string
	IntentID       string
	CallerID       *int64
	SubjectVersion *int64
	Operator       string
	Reason         string
	Evidence       string
	Detail         string
	Outcome        string
}

// execOperatorOp runs one audited mutation atomically: apply performs the
// mutation and returns its outcome; the audit row is inserted in the same
// transaction. A 23505 on execution_ops_audit_operation_id_uniq rolls the
// mutation back and re-reads the recorded row: same input reports the recorded
// outcome, different input is errOperationConflict with zero writes.
func execOperatorOp(ctx context.Context, pool *pgxpool.Pool, op operatorOp, apply func(context.Context, pgx.Tx) (string, error)) (string, bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", false, fmt.Errorf("begin operator op: %w", err)
	}
	outcome, err := apply(ctx, tx)
	if err != nil {
		_ = tx.Rollback(ctx)
		op.Outcome = "refused"
		op.Detail = "refused=" + sanitizeAuditDetail(err.Error())
		_ = insertExecAudit(ctx, pool, op)
		return "", false, err
	}
	if err := insertExecAuditTx(ctx, tx, op, outcome); err != nil {
		_ = tx.Rollback(ctx)
		if !isUniqueViolation(err, "execution_ops_audit_operation_id_uniq") {
			return "", false, fmt.Errorf("record operator audit: %w", err)
		}
		existing, found, readErr := readExecAudit(ctx, pool, op.OperationID)
		if readErr != nil {
			return "", false, readErr
		}
		if !found {
			return "", false, fmt.Errorf("operation_id %q conflicted but no audit row exists", op.OperationID)
		}
		if !sameOperatorInput(existing, op) {
			return "", false, errOperationConflict
		}
		return existing.Outcome, true, nil
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(ctx)
		return "", false, fmt.Errorf("commit operator op: %w", err)
	}
	return outcome, false, nil
}

// insertExecAudit writes one audit row on the pool (best-effort refusal path).
func insertExecAudit(ctx context.Context, pool *pgxpool.Pool, op operatorOp) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := insertExecAuditTx(ctx, tx, op, op.Outcome); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

const insertExecAuditSQL = `INSERT INTO execution_ops_audit
  (operation_id, action, intent_id, caller_id, subject_version, outcome, operator, reason, evidence, detail)
  VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, $7, $8, $9, $10)`

func insertExecAuditTx(ctx context.Context, tx pgx.Tx, op operatorOp, outcome string) error {
	_, err := tx.Exec(ctx, insertExecAuditSQL, op.OperationID, op.Action, op.IntentID,
		op.CallerID, op.SubjectVersion, outcome, op.Operator, op.Reason, op.Evidence, op.Detail)
	return err
}

type execAuditRow struct {
	Action         string
	IntentID       string
	CallerID       *int64
	SubjectVersion *int64
	Outcome        string
	Operator       string
	Reason         string
	Evidence       string
	Detail         string
}

const readExecAuditSQL = `SELECT action, intent_id, caller_id, subject_version, outcome, operator, reason, evidence, detail
  FROM execution_ops_audit WHERE operation_id = $1`

func readExecAudit(ctx context.Context, q *pgxpool.Pool, operationID string) (execAuditRow, bool, error) {
	var a execAuditRow
	err := q.QueryRow(ctx, readExecAuditSQL, operationID).Scan(
		&a.Action, &a.IntentID, &a.CallerID, &a.SubjectVersion, &a.Outcome,
		&a.Operator, &a.Reason, &a.Evidence, &a.Detail)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, false, nil
	}
	if err != nil {
		return a, false, fmt.Errorf("read operator audit: %w", err)
	}
	return a, true, nil
}

// sameOperatorInput compares the canonical business input fields (not the
// operator/reason audit metadata).
func sameOperatorInput(existing execAuditRow, op operatorOp) bool {
	return existing.Action == op.Action &&
		existing.IntentID == op.IntentID &&
		ptrEq(existing.CallerID, op.CallerID) &&
		ptrEq(existing.SubjectVersion, op.SubjectVersion) &&
		existing.Detail == op.Detail
}

func ptrEq(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}

// sanitizeAuditDetail bounds an evidence string to a single short line; it
// carries only classes/identities, never secrets.
func sanitizeAuditDetail(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 256 {
		return s[:256]
	}
	return s
}

// withdrawalExecPermissionSet upserts the fixed interface permission. Already
// at the requested value is a recorded nop; a missing caller row refuses.
func withdrawalExecPermissionSet(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("withdrawal-exec permission-set", flag.ContinueOnError)
	fs.SetOutput(stderr)
	callerID := fs.Int64("caller-id", 0, "caller identity (required, positive)")
	canExecute := fs.Bool("can-execute", false, "desired fixed interface permission")
	operationID := fs.String("operation-id", "", "idempotent operation identity (required)")
	operator := fs.String("operator", "", "declared operator identity (required)")
	reason := fs.String("reason", "", "operator reason (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || *callerID <= 0 || *operationID == "" || *operator == "" || *reason == "" {
		withdrawalExecUsage(stderr)
		return 2
	}

	pool, code := withdrawalExecOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()

	op := operatorOp{
		OperationID: *operationID,
		Action:      "permission_set",
		CallerID:    callerID,
		Operator:    *operator,
		Reason:      *reason,
		Detail:      fmt.Sprintf("can_execute=%t", *canExecute),
	}
	outcome, recorded, err := execOperatorOp(ctx, pool, op, func(ctx context.Context, tx pgx.Tx) (string, error) {
		var current bool
		err := tx.QueryRow(ctx, `SELECT can_execute FROM execution_caller_permission WHERE caller_id = $1`, *callerID).Scan(&current)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			current = false
		case err != nil:
			return "", err
		}
		if current == *canExecute {
			return "nop", nil
		}
		_, err = tx.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
			VALUES ($1, $2, $3)
			ON CONFLICT (caller_id) DO UPDATE
			  SET can_execute = EXCLUDED.can_execute, updated_by = EXCLUDED.updated_by, updated_at = now()`,
			*callerID, *canExecute, *operator)
		if err != nil {
			return "", err
		}
		return "applied", nil
	})
	if err != nil {
		if errors.Is(err, errOperationConflict) {
			fmt.Fprintf(stderr, "txharbor withdrawal-exec: operation_conflict operation_id=%s\n", *operationID)
			return 1
		}
		fmt.Fprintf(stderr, "txharbor withdrawal-exec: permission-set failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor withdrawal-exec: permission-set %s caller_id=%d can_execute=%t operation_id=%s recorded=%t\n",
		outcome, *callerID, *canExecute, *operationID, recorded)
	return 0
}

// withdrawalExecPermissionRevoke sets can_execute=FALSE. An absent row or an
// already-false row is a recorded nop.
func withdrawalExecPermissionRevoke(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("withdrawal-exec permission-revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	callerID := fs.Int64("caller-id", 0, "caller identity (required, positive)")
	operationID := fs.String("operation-id", "", "idempotent operation identity (required)")
	operator := fs.String("operator", "", "declared operator identity (required)")
	reason := fs.String("reason", "", "operator reason (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || *callerID <= 0 || *operationID == "" || *operator == "" || *reason == "" {
		withdrawalExecUsage(stderr)
		return 2
	}

	pool, code := withdrawalExecOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()

	op := operatorOp{
		OperationID: *operationID,
		Action:      "permission_revoke",
		CallerID:    callerID,
		Operator:    *operator,
		Reason:      *reason,
		Detail:      "can_execute=false",
	}
	outcome, recorded, err := execOperatorOp(ctx, pool, op, func(ctx context.Context, tx pgx.Tx) (string, error) {
		tag, err := tx.Exec(ctx, `UPDATE execution_caller_permission
			SET can_execute = FALSE, updated_by = $2, updated_at = now()
			WHERE caller_id = $1 AND can_execute = TRUE`, *callerID, *operator)
		if err != nil {
			return "", err
		}
		if tag.RowsAffected() == 0 {
			return "nop", nil
		}
		return "applied", nil
	})
	if err != nil {
		if errors.Is(err, errOperationConflict) {
			fmt.Fprintf(stderr, "txharbor withdrawal-exec: operation_conflict operation_id=%s\n", *operationID)
			return 1
		}
		fmt.Fprintf(stderr, "txharbor withdrawal-exec: permission-revoke failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor withdrawal-exec: permission-revoke %s caller_id=%d operation_id=%s recorded=%t\n",
		outcome, *callerID, *operationID, recorded)
	return 0
}

// withdrawalExecOpenPool loads the serve config and opens the operator pool.
func withdrawalExecOpenPool(ctx context.Context, d Deps) (*pgxpool.Pool, int) {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-exec: configuration error: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-exec: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	return pool, 0
}

// withdrawalExecUsage prints the accepted action forms.
func withdrawalExecUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor withdrawal-exec permission-set --caller-id C --can-execute=true|false --operation-id OP --operator NAME --reason R")
	fmt.Fprintln(w, "       txharbor withdrawal-exec permission-revoke --caller-id C --operation-id OP --operator NAME --reason R")
	fmt.Fprintln(w, "       txharbor withdrawal-exec claim-revoke --intent-id I --expected-lease-version V --operation-id OP --operator NAME --reason R --evidence E")
	fmt.Fprintln(w, "       txharbor withdrawal-exec projection-refresh --request-id R --operation-id OP --operator NAME")
	fmt.Fprintln(w, "       txharbor withdrawal-exec claim-show --intent-id I")
	fmt.Fprintln(w, "       txharbor withdrawal-exec step-list --intent-id I")
	fmt.Fprintln(w, "       txharbor withdrawal-exec event-list --intent-id I")
}
