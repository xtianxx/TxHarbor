//go:build linux && integration

package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestTargetLockWithTransactionUsesOwnerSessionAndRollsBack(t *testing.T) {
	f := newTargetWriterFixture(t)
	targetDSN := f.createTarget(t)
	target, err := controlstore.ParseDSNTarget(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CanonicalTargetKey(target)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireTargetLock(f.ctx, f.ctrlDSN, key, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(context.Background())

	callbackErr := errors.New("intentional acceptance rollback")
	checked := make(chan struct{})
	releaseCallback := make(chan struct{})
	transactionDone := make(chan error, 1)
	go func() {
		transactionDone <- lock.WithTransaction(f.ctx, func(ctx context.Context, tx pgx.Tx) error {
			var pid int32
			var ownsLock bool
			if err := tx.QueryRow(ctx, `SELECT pg_backend_pid(), EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
    AND objsubid = 2 AND classid = $1::oid AND objid = $2::oid
)`, uint32(lock.key1), uint32(lock.key2)).Scan(&pid, &ownsLock); err != nil {
				return err
			}
			if pid <= 0 || !ownsLock {
				return errors.New("acceptance transaction is not on the lock-owning backend")
			}
			if _, err := tx.Exec(ctx, `CREATE TABLE targetlock_tx_rollback_probe (id integer)`); err != nil {
				return err
			}
			close(checked)
			<-releaseCallback
			return callbackErr
		})
	}()
	<-checked

	// Monitor SQL must not use the dedicated pgx.Conn while acceptance work is
	// running on it. Health therefore waits until the transaction has rolled back.
	healthStarted := make(chan struct{})
	healthDone := make(chan error, 1)
	go func() {
		close(healthStarted)
		healthDone <- lock.Health(f.ctx)
	}()
	<-healthStarted
	select {
	case err := <-healthDone:
		t.Fatalf("Health did not serialize behind acceptance transaction: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseCallback)
	if err := <-transactionDone; err == nil || !strings.Contains(err.Error(), callbackErr.Error()) {
		t.Fatalf("transaction error = %v, want callback rollback error", err)
	}
	if err := <-healthDone; err != nil {
		t.Fatalf("Health after transaction: %v", err)
	}
	var exists bool
	if err := f.ctrl.QueryRow(f.ctx, `SELECT to_regclass('public.targetlock_tx_rollback_probe') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("callback's transactional write survived rollback")
	}
}
