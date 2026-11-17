package pg

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v6"
	"github.com/stretchr/testify/require"
)

// TestCompactPathCursorWorkerOwnsSnapshotFinalization checks that full consumption commits and early Close waits for rollback before lease reuse.
func TestCompactPathCursorWorkerOwnsSnapshotFinalization(t *testing.T) {
	for _, earlyClose := range []bool{false, true} {
		mock, err := pgxmock.NewConn()
		require.NoError(t, err)
		var (
			scope = newCompactScopeForTest(context.Background(), mock)
			owner = scope.owner
		)
		if earlyClose {
			mock.ExpectRollback()
		} else {
			mock.ExpectCommit()
		}
		cursor := newCompactPathCursor(context.Background(), scope.wrapResult(&compactScopeResult{
			remaining: 3,
		}))
		if earlyClose {
			<-cursor.Chan()
			cursor.Close()
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
