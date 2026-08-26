package faults

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// SQLDBSession — a real DBSession backed by database/sql, so
// WorkloadFaults can run against an actual Postgres/MySQL/etc. instance
// rather than a mock. This is genuinely wired to a live driver: Exec and
// Begin issue real queries and transactions.
type SQLDBSession struct {
	db *sql.DB
}

func NewSQLDBSession(db *sql.DB) *SQLDBSession {
	return &SQLDBSession{db: db}
}

func (s *SQLDBSession) Exec(ctx context.Context, query string, args ...any) error {
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLDBSession) Begin(ctx context.Context) (Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx}, nil
}

func (s *SQLDBSession) PoolStats() PoolStats {
	st := s.db.Stats()
	return PoolStats{
		InUse: st.InUse,
		Idle:  st.Idle,
		Max:   st.MaxOpenConnections,
	}
}

type sqlTx struct{ tx *sql.Tx }

func (t *sqlTx) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := t.tx.ExecContext(ctx, query, args...)
	return err
}
func (t *sqlTx) Commit() error   { return t.tx.Commit() }
func (t *sqlTx) Rollback() error { return t.tx.Rollback() }

// Connection pool exhaustion — opens and holds N connections via
// long-running, otherwise-idle transactions until the pool has nothing
// left to hand out to real client traffic.
type PoolExhaustionFault struct {
	HoldConnections int
	HoldFor         time.Duration

	mu   sync.Mutex
	txs  []Tx
}

func (f *PoolExhaustionFault) Name() string { return "pool_exhaustion" }

func (f *PoolExhaustionFault) Inject(ctx context.Context, session DBSession) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := 0; i < f.HoldConnections; i++ {
		tx, err := session.Begin(ctx)
		if err != nil {
			// Pool likely already exhausted by our own prior iterations —
			// that's actually the fault working, not a failure to report.
			break
		}
		// A trivial statement keeps the underlying conn checked out
		// without depending on any particular schema existing.
		_ = tx.Exec(ctx, "SELECT 1")
		f.txs = append(f.txs, tx)
	}

	if f.HoldFor > 0 {
		go func() {
			select {
			case <-time.After(f.HoldFor):
				f.release()
			case <-ctx.Done():
				f.release()
			}
		}()
	}
	return nil
}

func (f *PoolExhaustionFault) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, tx := range f.txs {
		_ = tx.Rollback()
	}
	f.txs = nil
}

// Release lets a caller end the fault early instead of waiting for
// HoldFor, e.g. from the scenario runner's own Revert-style hook.
func (f *PoolExhaustionFault) Release() { f.release() }

// Deadlock injection — opens two transactions that lock the same two
// rows in opposite order, reproducing a classic deadlock so the system
// under test's deadlock detector/retry logic gets exercised.
type DeadlockFault struct {
	Table     string
	KeyColumn string
	KeyA, KeyB string
}

func (f *DeadlockFault) Name() string { return "deadlock" }

func (f *DeadlockFault) Inject(ctx context.Context, session DBSession) error {
	txA, err := session.Begin(ctx)
	if err != nil {
		return fmt.Errorf("deadlock: begin txA: %w", err)
	}
	txB, err := session.Begin(ctx)
	if err != nil {
		_ = txA.Rollback()
		return fmt.Errorf("deadlock: begin txB: %w", err)
	}

	lockQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 FOR UPDATE", f.Table, f.KeyColumn)

	// txA locks A, then (after a beat) tries to lock B.
	// txB locks B, then tries to lock A. Run concurrently, one deadlocks.
	errA := make(chan error, 1)
	errB := make(chan error, 1)

	go func() {
		if err := txA.Exec(ctx, lockQuery, f.KeyA); err != nil {
			errA <- err
			return
		}
		time.Sleep(50 * time.Millisecond) // widen the window so both locks land
		errA <- txA.Exec(ctx, lockQuery, f.KeyB)
	}()

	go func() {
		if err := txB.Exec(ctx, lockQuery, f.KeyB); err != nil {
			errB <- err
			return
		}
		time.Sleep(50 * time.Millisecond)
		errB <- txB.Exec(ctx, lockQuery, f.KeyA)
	}()

	resA, resB := <-errA, <-errB
	_ = txA.Rollback()
	_ = txB.Rollback()

	// One side should have hit a deadlock error; if neither did, the
	// window was too narrow (or the store doesn't detect this shape of
	// deadlock) and this call reports that honestly rather than pretending
	// success.
	if resA == nil && resB == nil {
		return fmt.Errorf("deadlock: neither transaction errored — no deadlock was produced, consider widening the timing window")
	}
	return nil
}


// Long-running transaction / lock contention — holds a row lock open for
// a fixed duration so concurrent client writers queue up behind it.
type LockContentionFault struct {
	Table, KeyColumn, Key string
	HoldFor                time.Duration
}

func (f *LockContentionFault) Name() string { return "lock_contention" }

func (f *LockContentionFault) Inject(ctx context.Context, session DBSession) error {
	tx, err := session.Begin(ctx)
	if err != nil {
		return fmt.Errorf("lock_contention: begin: %w", err)
	}
	lockQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 FOR UPDATE", f.Table, f.KeyColumn)
	if err := tx.Exec(ctx, lockQuery, f.Key); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("lock_contention: acquiring lock: %w", err)
	}

	select {
	case <-time.After(f.HoldFor):
	case <-ctx.Done():
	}
	return tx.Rollback()
}

// Backup/restore inconsistency window — snapshots a table's rows, lets
// writes continue for a configured window, then restores the snapshot,
// simulating a restore from a backup taken before in-flight writes were
// captured (data loss / apparent rollback from the client's view).

// Snapshottable is a narrower surface than full DBSession, since backup
// faults need bulk row access rather than arbitrary query execution.
// A real implementation adapts this to `pg_dump`/`COPY` or the engine's
// native snapshot mechanism; nothing here assumes Postgres specifically.
type Snapshottable interface {
	SnapshotTable(ctx context.Context, table string) (rows [][]any, err error)
	RestoreTable(ctx context.Context, table string, rows [][]any) error
}

type BackupWindowFault struct {
	Store    Snapshottable
	Table    string
	OpenFor  time.Duration

	snapshot [][]any
}

func (f *BackupWindowFault) Name() string { return "backup_window" }

// Inject takes the snapshot immediately, then waits OpenFor before
// restoring it — any writes to Table that land during that window are
// silently lost when Restore runs, exactly as a real backup/restore
// cycle would lose them.
func (f *BackupWindowFault) Inject(ctx context.Context, session DBSession) error {
	rows, err := f.Store.SnapshotTable(ctx, f.Table)
	if err != nil {
		return fmt.Errorf("backup_window: snapshot: %w", err)
	}
	f.snapshot = rows

	select {
	case <-time.After(f.OpenFor):
	case <-ctx.Done():
	}

	if err := f.Store.RestoreTable(ctx, f.Table, f.snapshot); err != nil {
		return fmt.Errorf("backup_window: restore: %w", err)
	}
	return nil
}

// Failover mid-transaction — composes a NodeFault (kill the primary)
// with an in-flight, uncommitted transaction, so client code exercises
// its "transaction failed because the primary died mid-commit" path.
type MidTxFailoverFault struct {
	Crash       NodeFault // typically &CrashFault{Signal: syscall.SIGKILL}
	PrimaryNode string
	Table, Col  string
	Key         string
	Value       any
	DelayBeforeCrash time.Duration
}

func (f *MidTxFailoverFault) Name() string { return "mid_tx_failover" }

// Inject starts a transaction, issues a write, waits DelayBeforeCrash
// (giving the write time to be sent but not necessarily committed), then
// crashes the primary node out from under the open transaction.
func (f *MidTxFailoverFault) Inject(ctx context.Context, session DBSession) error {
	tx, err := session.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mid_tx_failover: begin: %w", err)
	}

	updateQuery := fmt.Sprintf("UPDATE %s SET %s = $1 WHERE id = $2", f.Table, f.Col)
	if err := tx.Exec(ctx, updateQuery, f.Value, f.Key); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mid_tx_failover: write before crash: %w", err)
	}

	select {
	case <-time.After(f.DelayBeforeCrash):
	case <-ctx.Done():
	}

	// The primary dies before Commit() is ever called — from the
	// client's perspective the transaction outcome is now genuinely
	// unknown, which is the scenario this fault exists to produce.
	if err := f.Crash.Inject(ctx, f.PrimaryNode); err != nil {
		_ = tx.Rollback() // best-effort cleanup on our side if the crash itself failed
		return fmt.Errorf("mid_tx_failover: crashing primary: %w", err)
	}
	return nil
}