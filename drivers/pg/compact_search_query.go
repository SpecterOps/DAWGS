package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
)

// compactSearchQueryScope keeps property resolution, discovery, and hydration
// in one snapshot without changing an explicitly selected caller transaction.
type compactSearchQueryScope struct {
	// owner temporarily uses the query-owned transaction until result finalization.
	owner *transaction

	// tx is the PostgreSQL transaction committed only after a successful drain.
	tx pgx.Tx

	// previousIsolation is restored when the query releases its snapshot.
	previousIsolation pgx.TxIsoLevel

	// result owns the rows that must close before transaction finalization.
	result graph.Result

	// done prevents duplicate commit, rollback, and cleanup operations.
	done bool

	// err retains stream and cleanup failures after the snapshot closes.
	err error
}

// beginCompactSearchQuery reuses explicit stable transactions and owns only
// the otherwise-autocommit query. Temporary tables are initialized before BEGIN.
func (s *transaction) beginCompactSearchQuery() (*compactSearchQueryScope, error) {
	if s.tx != nil {
		if !stableSnapshotIsolation(s.isolation) {
			return nil, fmt.Errorf("compact all-shortest search requires a stable transaction snapshot")
		}
		return nil, nil
	}

	if provider, ok := s.schemaManager.translationCacheProvider.(StableSnapshotTraversalWorkspaceProvider); ok {
		if err := provider.EnsureStableSnapshotTraversalWorkspaces(s.ctx, s.conn); err != nil {
			return nil, err
		}
	} else if err := EnsureStableSnapshotTraversalWorkspaces(s.ctx, s.conn); err != nil {
		return nil, err
	}

	if owned, err := s.conn.BeginTx(s.ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadWrite,
	}); err != nil {
		return nil, fmt.Errorf("begin compact-search snapshot: %w", err)
	} else {
		scope := &compactSearchQueryScope{
			owner:             s,
			tx:                owned,
			previousIsolation: s.isolation,
		}

		s.tx = owned
		s.isolation = pgx.RepeatableRead
		s.compactSearch = scope
		return scope, nil
	}
}

// wrapResult binds query lifetime to result lifetime, including dispatch and
// compilation failures that occur after the owned snapshot has begun.
func (s *compactSearchQueryScope) wrapResult(result graph.Result) graph.Result {
	if result == nil {
		s.finish(false)
		return graph.NewErrorResult(errors.Join(fmt.Errorf("compact search returned no result"), s.err))
	}

	s.result = result
	if err := result.Error(); err != nil {
		s.closeResult(false)
		return graph.NewErrorResult(s.err)
	}

	return &compactSearchQueryResult{
		Result: result,
		scope:  s,
	}
}

// closeResult releases the row stream before transaction finalization so pgx
// cannot return a busy connection to the pool or hide a late server error.
func (s *compactSearchQueryScope) closeResult(drained bool) {
	if s.done {
		return
	}

	if s.result != nil {
		s.result.Close()
		s.err = errors.Join(s.err, s.result.Error())
	}
	s.finish(drained && s.err == nil)
}

// finish commits only a fully consumed successful read. Rollback cleanup uses
// an independent bounded context when the original query has been canceled.
func (s *compactSearchQueryScope) finish(success bool) {
	if s.done {
		return
	}

	shouldCommit := success
	s.done = true
	if s.owner.ctx.Err() != nil {
		s.err = errors.Join(s.err, s.owner.ctx.Err())
		shouldCommit = false
	}

	if shouldCommit {
		if err := s.tx.Commit(s.owner.ctx); err != nil {
			s.err = errors.Join(s.err, fmt.Errorf("commit compact-search snapshot: %w", err))
			shouldCommit = false
		}
	}

	if !shouldCommit {
		var (
			cleanupCtx, cancel = context.WithTimeout(context.WithoutCancel(s.owner.ctx), traversalLimitCleanupTimeout)
		)
		defer cancel()

		if err := s.tx.Rollback(cleanupCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			s.err = errors.Join(s.err, fmt.Errorf("rollback compact-search snapshot: %w", err))
			if s.owner.conn != nil {
				s.err = errors.Join(s.err, s.owner.conn.Conn().Close(cleanupCtx))
			}
		}
	}

	s.owner.tx = nil
	s.owner.isolation = s.previousIsolation
	s.owner.compactSearch = nil
}

// compactSearchQueryResult retains the snapshot until EOF; Close before EOF
// rolls it back even when pgx can drain the remaining server response normally.
type compactSearchQueryResult struct {
	// Result supplies rows and graph-value mapping while the snapshot remains leased.
	graph.Result

	// scope finalizes the query-owned snapshot when this result completes.
	scope *compactSearchQueryScope
}

// Next commits the snapshot only after the upstream result reaches successful EOF.
func (s *compactSearchQueryResult) Next() bool {
	if s.scope.done {
		return false
	}

	if s.Result.Next() {
		return true
	}
	s.scope.closeResult(true)
	return false
}

// Scan retains decoding failures and rolls back the owned snapshot before returning them.
func (s *compactSearchQueryResult) Scan(targets ...any) error {
	if err := s.Result.Scan(targets...); err != nil {
		s.scope.err = errors.Join(s.scope.err, err)
		s.scope.closeResult(false)
		return err
	}

	return nil
}

// Error reports upstream or finalization failures without duplicating an already retained cause.
func (s *compactSearchQueryResult) Error() error {
	upstream := s.Result.Error()
	if errors.Is(s.scope.err, upstream) {
		return s.scope.err
	}

	return errors.Join(s.scope.err, upstream)
}

// Close releases unread rows and rolls back a snapshot that has not reached EOF.
func (s *compactSearchQueryResult) Close() {
	s.scope.closeResult(false)
}
