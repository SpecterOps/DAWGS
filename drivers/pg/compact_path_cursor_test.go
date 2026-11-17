package pg

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/stretchr/testify/require"
)

// compactCursorBarrierTimeout bounds test synchronization when a worker fails to enter or finish rollback.
const compactCursorBarrierTimeout = time.Second

// TestCompactPathCursorWorkerOwnsSnapshotFinalization checks that full consumption commits and early Close waits for rollback before lease reuse.
func TestCompactPathCursorWorkerOwnsSnapshotFinalization(t *testing.T) {
	synctest.Test(t, testCompactPathCursorSnapshotFinalization)
}

// testCompactPathCursorSnapshotFinalization synchronizes worker cleanup within a deterministic scheduling bubble.
func testCompactPathCursorSnapshotFinalization(t *testing.T) {
	for _, earlyClose := range []bool{false, true} {
		mock, err := pgxmock.NewConn()
		require.NoError(t, err)
		var (
			rollbackBarrier *compactRollbackObserver
			cursorTx        pgx.Tx = mock
		)
		if earlyClose {
			rollbackBarrier = &compactRollbackObserver{
				Tx:      mock,
				started: make(chan struct{}),
				release: make(chan struct{}),
			}
			cursorTx = rollbackBarrier
			mock.ExpectRollback()
		} else {
			mock.ExpectCommit()
		}
		var (
			scope = newCompactScopeForTest(context.Background(), cursorTx)
			owner = scope.owner
		)
		cursor := newCompactPathCursor(context.Background(), scope.wrapResult(&compactScopeResult{
			remaining: 3,
		}))
		if earlyClose {
			var (
				closeReturned   = make(chan struct{})
				releaseRollback = sync.OnceFunc(func() { close(rollbackBarrier.release) })
			)
			t.Cleanup(releaseRollback)
			<-cursor.Chan()
			go func() {
				cursor.Close()
				close(closeReturned)
			}()

			select {
			case <-rollbackBarrier.started:

			case <-time.After(compactCursorBarrierTimeout):
				t.Fatal("cursor worker did not enter rollback")
			}
			require.Same(t, cursorTx, owner.tx)
			require.Same(t, scope, owner.compactSearch)

			synctest.Wait()
			select {
			case <-closeReturned:
				t.Fatal("cursor Close returned before rollback completed")

			default:
			}

			releaseRollback()
			select {
			case <-closeReturned:

			case <-time.After(compactCursorBarrierTimeout):
				t.Fatal("cursor Close did not return after rollback completed")
			}
		} else {
			count := 0
			for range cursor.Chan() {
				count++
			}
			require.Equal(t, 3, count)
			require.NoError(t, cursor.Error())
			cursor.Close()
		}
		cursor.Close()
		require.Nil(t, owner.tx)
		require.Nil(t, owner.compactSearch)
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

// TestCompactPathCursorCancellationFinishesBeforeLeaseReuse checks that canceled delivery completes rollback before the caller regains the connection.
func TestCompactPathCursorCancellationFinishesBeforeLeaseReuse(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	var (
		scope = newCompactScopeForTest(ctx, mock)
		owner = scope.owner
	)
	mock.ExpectRollback()
	cursor := newCompactPathCursor(ctx, scope.wrapResult(&compactScopeResult{
		remaining: 100,
	}))
	<-cursor.Chan()
	cancel()
	cursor.Close()
	require.ErrorIs(t, cursor.Error(), context.Canceled)
	require.Nil(t, owner.tx)
	require.NoError(t, mock.ExpectationsWereMet())
}
