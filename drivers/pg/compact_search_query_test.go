package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// compactScopeResult isolates stream lifetime from the transaction mock.
type compactScopeResult struct {
	// remaining counts rows still available to the stream consumer.
	remaining int

	// err is the upstream error exposed by Error.
	err error

	// scanError forces a decoding failure independently of row iteration.
	scanError error

	// lateError becomes visible only when the result is closed.
	lateError error

	// closed prevents further row delivery after cleanup.
	closed bool
}

// Next delivers one fixture row until the stream closes or exhausts its row count.
func (s *compactScopeResult) Next() bool {
	if s.closed || s.remaining == 0 {
		return false
	}
	s.remaining--
	return true
}

// Close releases fixture rows and publishes a configured late upstream error.
func (s *compactScopeResult) Close() {
	s.closed = true
	if s.lateError != nil {
		s.err = s.lateError
	}
}

// Error returns the fixture's currently published upstream failure.
func (s *compactScopeResult) Error() error {
	return s.err
}

// Keys supplies the single logical column used by lifecycle assertions.
func (s *compactScopeResult) Keys() []string {
	return []string{"value"}
}

// Values supplies the current fixture row for graph-value mapping checks.
func (s *compactScopeResult) Values() []any {
	return []any{int64(1)}
}

// Mapper retains the normal graph scalar mapping behavior for fixture rows.
func (s *compactScopeResult) Mapper() graph.ValueMapper {
	return graph.NewValueMapper()
}

// Scan returns a configured decoding error without advancing the fixture stream.
func (s *compactScopeResult) Scan(...any) error {
	return s.scanError
}

// newCompactScopeForTest binds a transaction mock to one query-owned snapshot for result-lifetime tests.
func newCompactScopeForTest(ctx context.Context, tx pgx.Tx) *compactSearchQueryScope {
	owner := &transaction{
		ctx:       ctx,
		tx:        tx,
		isolation: pgx.RepeatableRead,
	}
	scope := &compactSearchQueryScope{
		owner: owner,
		tx:    tx,
	}
	owner.compactSearch = scope
	return scope
}

// TestCompactSearchResultCommitsOnlyAfterDrain checks that rows retain their snapshot until successful EOF and finalize it only once.
func TestCompactSearchResultCommitsOnlyAfterDrain(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	var (
		scope = newCompactScopeForTest(context.Background(), mock)
		owner = scope.owner
	)
	upstream := &compactScopeResult{
		remaining: 2,
	}
	result := scope.wrapResult(upstream)
	mock.ExpectCommit()
	require.True(t, result.Next())
	require.Same(t, mock, owner.tx)
	require.NotNil(t, owner.compactSearch)
	require.Equal(t, []string{"value"}, result.Keys())
	var value int64
	require.NoError(t, result.Scan(&value))
	require.NoError(t, graph.ScanNextResult(result, &value))
	require.EqualValues(t, 1, value)
	require.True(t, result.Next())
	require.False(t, result.Next())
	require.NoError(t, result.Error())
	require.Nil(t, owner.tx)
	require.Nil(t, owner.compactSearch)
	require.Empty(t, owner.isolation)
	require.True(t, upstream.closed)
	result.Close()
	require.False(t, result.Next())
	require.NoError(t, mock.ExpectationsWereMet())
}

// compactSearchCloseCase describes an early-close scenario and the failure published during cleanup.
type compactSearchCloseCase struct {
	// name identifies the cleanup outcome under test.
	name string

	// late is the upstream failure published only during Close.
	late error
}

// TestCompactSearchResultRollsBackEarlyCloseAndLateErrors checks that early Close rolls back and retains errors discovered while closing rows.
func TestCompactSearchResultRollsBackEarlyCloseAndLateErrors(t *testing.T) {
	failure := errors.New("late traversal resource error")
	for _, test := range []compactSearchCloseCase{
		{
			name: "early close",
		},
		{
			name: "late error",
			late: failure,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)
			var (
				scope = newCompactScopeForTest(context.Background(), mock)
				owner = scope.owner
			)
			result := scope.wrapResult(&compactScopeResult{
				remaining: 2,
				lateError: test.late,
			})
			mock.ExpectRollback()
			require.True(t, result.Next())
			result.Close()
			require.ErrorIs(t, result.Error(), test.late)
			require.Nil(t, owner.tx)
			owner.Close()
			result.Close()
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestCompactSearchOwnerCloseReleasesActiveRows verifies wrapper cleanup closes unread rows before rolling back an owned snapshot.
func TestCompactSearchOwnerCloseReleasesActiveRows(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	var (
		scope = newCompactScopeForTest(context.Background(), mock)
		owner = scope.owner
	)
	upstream := &compactScopeResult{
		remaining: 1,
	}
	result := scope.wrapResult(upstream)
	mock.ExpectRollback()
	require.ErrorContains(t, owner.Commit(), "consume or close")
	owner.Close()
	require.True(t, upstream.closed)
	require.False(t, result.Next())
	require.NoError(t, result.Error())
	require.Nil(t, owner.compactSearch)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCompactSearchResultPreservesDispatchScanAndCommitErrors checks that execution, decoding, and commit failures remain observable after cleanup.
func TestCompactSearchResultPreservesDispatchScanAndCommitErrors(t *testing.T) {
	failure := errors.New("compact failure")
	for _, phase := range []string{"dispatch", "scan", "commit"} {
		t.Run(phase, func(t *testing.T) {
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)
			var (
				scope = newCompactScopeForTest(context.Background(), mock)
				owner = scope.owner
			)
			if phase == "commit" {
				mock.ExpectCommit().WillReturnError(failure)
			}
			mock.ExpectRollback()
			var result graph.Result
			switch phase {
			case "dispatch":
				result = scope.wrapResult(graph.NewErrorResult(failure))
			case "scan":
				result = scope.wrapResult(&compactScopeResult{
					remaining: 1,
					scanError: failure,
				})
				require.True(t, result.Next())
				require.ErrorIs(t, result.Scan(), failure)
			case "commit":
				result = scope.wrapResult(&compactScopeResult{})
				require.False(t, result.Next())
			}
			require.ErrorIs(t, result.Error(), failure)
			require.Nil(t, owner.tx)
			require.Nil(t, owner.compactSearch)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// compactRollbackObserver records the cleanup context without altering mocked transaction behavior.
type compactRollbackObserver struct {
	// Tx delegates rollback execution after its context is inspected.
	pgx.Tx

	// contextError records whether cancellation leaked into cleanup.
	contextError error

	// deadline records the independently bounded rollback timeout.
	deadline time.Time
}

// Rollback captures the cleanup deadline and forwards rollback to the transaction mock.
func (s *compactRollbackObserver) Rollback(ctx context.Context) error {
	s.contextError = ctx.Err()
	s.deadline, _ = ctx.Deadline()
	return s.Tx.Rollback(ctx)
}

// TestCompactSearchCancellationUsesIndependentRollbackContext checks that canceled operations still roll back with a bounded uncanceled context.
func TestCompactSearchCancellationUsesIndependentRollbackContext(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	observer := &compactRollbackObserver{
		Tx: mock,
	}
	var (
		scope = newCompactScopeForTest(ctx, observer)
		owner = scope.owner
	)
	result := scope.wrapResult(&compactScopeResult{})
	mock.ExpectRollback()
	cancel()
	require.False(t, result.Next())
	require.ErrorIs(t, result.Error(), context.Canceled)
	require.NoError(t, observer.contextError)
	require.WithinDuration(t, time.Now().Add(traversalLimitCleanupTimeout), observer.deadline, time.Second)
	require.Nil(t, owner.tx)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCompactSearchReusesOnlyCallerOwnedStableTransactions verifies that borrowed snapshots remain caller-owned and unsupported isolation is rejected.
func TestCompactSearchReusesOnlyCallerOwnedStableTransactions(t *testing.T) {
	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable, pgx.ReadCommitted, pgx.ReadUncommitted, ""} {
		t.Run(string(isolation), func(t *testing.T) {
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)
			owner := &transaction{
				ctx:       context.Background(),
				tx:        mock,
				isolation: isolation,
			}
			scope, err := owner.beginCompactSearchQuery()
			require.Nil(t, scope)
			if stableSnapshotIsolation(isolation) {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "stable transaction snapshot")
			}
			require.Same(t, mock, owner.tx)
			require.Nil(t, owner.compactSearch)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestCompactSearchSnapshotQualificationExcludesWritesAndUnsupportedShapes checks that mutation and unsupported path forms cannot acquire a read-only query snapshot.
func TestCompactSearchSnapshotQualificationExcludesWritesAndUnsupportedShapes(t *testing.T) {
	for _, query := range []string{
		"MATCH (n) RETURN n",
		"MATCH p=allShortestPaths((s)-[*]->(t)) SET t.changed=true RETURN p",
		"MATCH p=allShortestPaths((s)-[*]->(t)) RETURN p UNION MATCH (n) RETURN n",
		"MATCH p=allShortestPaths((s)-[*]-(t)) RETURN p",
	} {
		parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
		if err != nil {
			continue // The parser rejecting an unsupported query cannot start a snapshot.
		}
		require.False(t, translate.UsesBudgetedAllShortestPaths(parsed), query)
	}
}
