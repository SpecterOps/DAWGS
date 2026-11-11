package pg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func newMultipartMockTransaction(t *testing.T, matchers ...pgxmock.QueryMatcher) (*transaction, pgxmock.PgxConnIface) {
	t.Helper()
	ctx := context.Background()
	var matcher pgxmock.QueryMatcher = pgxmock.QueryMatcherEqual
	if len(matchers) > 0 {
		matcher = matchers[0]
	}
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	mock.ExpectBegin()
	pgxTx, err := mock.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectRollback()
		require.NoError(t, pgxTx.Rollback(ctx))
		mock.ExpectClose()
		require.NoError(t, mock.Close(ctx))
		require.NoError(t, mock.ExpectationsWereMet())
	})
	manager := NewSchemaManager(nil, 0)
	t.Cleanup(func() {
		manager.parseCache.Close()
		manager.compilationCache.Close()
	})
	return &transaction{ctx: ctx, tx: pgxTx, schemaManager: manager}, mock
}

var multipartSQLMatcher = pgxmock.QueryMatcherFunc(func(expected, actual string) error {
	prefix := map[string]string{
		"materialize": "create temporary table pg_temp.",
		"final":       "with ",
		"cleanup":     "drop table ",
	}[expected]
	if prefix == "" {
		return pgxmock.QueryMatcherEqual.Match(expected, actual)
	}
	if !strings.HasPrefix(strings.ToLower(actual), prefix) {
		return fmt.Errorf("expected %s SQL, received %q", expected, actual)
	}
	return nil
})

// multipartObservedTx inspects arguments before pgxmock expands named bindings
// and consumes execution options, preserving coverage of the caller's protocol.
type multipartObservedTx struct {
	// Tx provides transaction operations delegated by the observation wrapper.
	pgx.Tx
	// inspect checks the SQL statement and bound arguments before execution.
	inspect func(string, []any)
}

// Exec observes the statement and arguments before delegating execution to the wrapped transaction.
func (s multipartObservedTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	s.inspect(sql, arguments)
	return s.Tx.Exec(ctx, sql, arguments...)
}

// Query observes the statement and arguments before delegating a row-producing query.
func (s multipartObservedTx) Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error) {
	s.inspect(sql, arguments)
	return s.Tx.Query(ctx, sql, arguments...)
}

// multipartAnyBindings expands an expectation to the generated SQL's positional
// parameters. Semantic values and map ownership are checked before rewriting.
type multipartAnyBindings struct{}

// RewriteQuery exercises the multipart test's query-rewriting protocol.
func (s multipartAnyBindings) RewriteQuery(ctx context.Context, conn *pgx.Conn, sql string, arguments []any) (string, []any, error) {
	rewrittenSQL, bindings, err := (pgx.NamedArgs{}).RewriteQuery(ctx, conn, sql, arguments)
	for index := range bindings {
		bindings[index] = pgxmock.AnyArg()
	}
	return rewrittenSQL, bindings, err
}

// multipartBindingMatcher checks semantic values without depending on generated
// parameter names, and keeps the actual map to detect reuse by later stages.
type multipartBindingMatcher struct {
	literal       string
	otherLiterals []string
	callerValue   string
	observed      pgx.NamedArgs
	snapshot      pgx.NamedArgs
}

func (s *multipartBindingMatcher) Match(value any) bool {
	bindings, ok := value.(pgx.NamedArgs)
	if !ok {
		return false
	}
	foundLiteral, foundCaller := false, false
	for _, bound := range bindings {
		text, isString := bound.(string)
		if !isString {
			continue
		}
		foundLiteral = foundLiteral || text == s.literal
		foundCaller = foundCaller || text == s.callerValue
		for _, forbidden := range s.otherLiterals {
			if text == forbidden {
				return false
			}
		}
	}
	if !foundLiteral || !foundCaller {
		return false
	}
	s.observed = bindings
	s.snapshot = maps.Clone(bindings)
	return true
}

const multipartBindingQuery = `CREATE (a {name: 'stage-alpha', payload: $value})
WITH a
CREATE (b {name: 'stage-beta', payload: $value})
WITH a, b RETURN a.name, b.name, 'stage-final' AS phase, $value AS value`

func TestMultipartWriteDetectionPreservesOrdinaryQueries(t *testing.T) {
	require.False(t, hasMultipartWrites(nil))
	for _, testCase := range []struct {
		query  string
		staged bool
	}{
		{"CREATE (n) RETURN n", false},
		{"MATCH (n) DELETE n RETURN count(*)", false},
		{"MATCH (n) DELETE n DETACH DELETE n", false},
		{"MATCH (n) SET n.x = 1 DELETE n RETURN count(*)", true},
		{"MATCH (n) REMOVE n.x DELETE n", true},
		{"CREATE (n) DELETE n", true},
		{"MATCH (n) WITH n SET n.x = 1 DELETE n", true},
		{"MATCH (n) WITH n RETURN n", false},
		{"MATCH (n) WITH n CREATE (m) RETURN m", false},
		{"CREATE (n) WITH n RETURN n", true},
		{"MATCH (n) SET n.name = 'updated' WITH n RETURN n", true},
	} {
		query, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
		require.NoError(t, err)
		require.Equal(t, testCase.staged, hasMultipartWrites(query), testCase.query)
	}
}

// TestMultipartExecutesDistinctStageBindingsAndPreservesProtocol checks stage-specific parameters across the configured query protocols.
func TestMultipartExecutesDistinctStageBindingsAndPreservesProtocol(t *testing.T) {
	for _, protocol := range []struct {
		// name identifies the protocol configuration in subtest output.
		name string
		// configured selects the query execution mode requested by the driver.
		configured pgx.QueryExecMode
		// executed contains the execution mode expected at the underlying transaction.
		executed pgx.QueryExecMode
	}{
		{
			name:       "default_avoids_prepared_temp_relation_oids",
			configured: pgx.QueryExecModeCacheStatement,
			executed:   pgx.QueryExecModeExec,
		},
		{
			name:       "simple_protocol",
			configured: pgx.QueryExecModeSimpleProtocol,
			executed:   pgx.QueryExecModeSimpleProtocol,
		},
	} {
		t.Run(protocol.name, func(t *testing.T) {
			owner, mock := newMultipartMockTransaction(t, multipartSQLMatcher)
			owner.queryExecMode = protocol.configured
			owner.queryResultsFormat = pgx.QueryResultFormats{pgx.BinaryFormatCode}
			query, err := frontend.ParseCypher(frontend.NewContext(), multipartBindingQuery)
			require.NoError(t, err)
			parameters := map[string]any{"value": "current-caller-value"}
			var matchers []*multipartBindingMatcher
			for _, literal := range []string{"stage-alpha", "stage-beta", "stage-final"} {
				matcher := &multipartBindingMatcher{
					literal:     literal,
					callerValue: parameters["value"].(string),
				}
				for _, other := range []string{"stage-alpha", "stage-beta", "stage-final"} {
					if other != literal {
						matcher.otherLiterals = append(matcher.otherLiterals, other)
					}
				}
				matchers = append(matchers, matcher)
			}
			stage := 0
			owner.tx = multipartObservedTx{
				Tx: owner.tx,
				inspect: func(sql string, arguments []any) {
					if strings.HasPrefix(sql, "drop table ") {
						return
					}
					require.Less(t, stage, len(matchers))
					require.NotEmpty(t, arguments)
					require.Equal(t, protocol.executed, arguments[0])
					if stage == len(matchers)-1 {
						require.Len(t, arguments, 3)
						require.Equal(t, owner.queryResultsFormat, arguments[1])
					} else {
						require.Len(t, arguments, 2)
					}
					require.True(t, matchers[stage].Match(arguments[len(arguments)-1]))
					stage++
				},
			}
			for range matchers[:2] {
				mock.ExpectExec("materialize").WithArgs(multipartAnyBindings{}).WillReturnResult(pgxmock.NewResult("SELECT", 1))
			}
			mock.ExpectQuery("final").WithArgs(multipartAnyBindings{}).WillReturnRows(
				pgxmock.NewRows([]string{"first", "second", "phase", "value"}).AddRow("stage-alpha", "stage-beta", "stage-final", "current-caller-value"),
			)
			mock.ExpectExec("cleanup").WillReturnResult(pgxmock.NewResult("DROP TABLE", 0))

			result := owner.queryMultipart(query, parameters, 0, SQLGenerationProfile{})
			require.IsType(t, &stagedQueryResult{}, result)
			require.True(t, result.Next(), "%v", result.Error())
			require.Equal(t, []any{"stage-alpha", "stage-beta", "stage-final", "current-caller-value"}, result.Values())
			var first, second, phase, caller string
			require.NoError(t, result.Scan(&first, &second, &phase, &caller))
			require.Equal(t, []string{"stage-alpha", "stage-beta", "stage-final", "current-caller-value"}, []string{first, second, phase, caller})
			require.False(t, result.Next())
			require.NoError(t, result.Error())
			require.Equal(t, len(matchers), stage)
			for _, matcher := range matchers {
				require.NotNil(t, matcher.observed)
				require.Equal(t, matcher.snapshot, matcher.observed, "later stages must not mutate earlier bindings")
			}
			matchers[0].observed["test_map_ownership"] = "changed"
			require.NotContains(t, matchers[1].observed, "test_map_ownership")
			require.NotContains(t, matchers[2].observed, "test_map_ownership")
			require.Equal(t, map[string]any{"value": "current-caller-value"}, parameters)
		})
	}
}

// TestMultipartExecutionFailuresPreventCommit checks that a failed multipart stage aborts the transaction.
func TestMultipartExecutionFailuresPreventCommit(t *testing.T) {
	for _, failingStage := range []string{"intermediate", "final"} {
		t.Run(failingStage, func(t *testing.T) {
			owner, mock := newMultipartMockTransaction(t, multipartSQLMatcher)
			query, err := frontend.ParseCypher(frontend.NewContext(), multipartBindingQuery)
			require.NoError(t, err)
			expected := errors.New("stage execution failed")
			mock.ExpectExec("materialize").WithArgs(multipartAnyBindings{}).WillReturnResult(pgxmock.NewResult("SELECT", 1))
			if failingStage == "intermediate" {
				mock.ExpectExec("materialize").WithArgs(multipartAnyBindings{}).WillReturnError(expected)
			} else {
				mock.ExpectExec("materialize").WithArgs(multipartAnyBindings{}).WillReturnResult(pgxmock.NewResult("SELECT", 1))
				mock.ExpectQuery("final").WithArgs(multipartAnyBindings{}).WillReturnError(expected)
			}
			result := owner.queryMultipart(query, map[string]any{"value": "caller"}, 0, SQLGenerationProfile{})
			require.False(t, result.Next())
			require.ErrorIs(t, result.Error(), expected)
			result.Close()
			require.ErrorIs(t, owner.Commit(), expected)
		})
	}
}

func TestMultipartCompileFailuresExecuteNoWrites(t *testing.T) {
	t.Run("parse_failure", func(t *testing.T) {
		owner, _ := newMultipartMockTransaction(t)
		result := owner.Query("CREATE (", nil)
		require.False(t, result.Next())
		require.Error(t, result.Error())
		require.Nil(t, owner.stagedError)
	})
	t.Run("translation_failure", func(t *testing.T) {
		owner, _ := newMultipartMockTransaction(t)
		query, err := frontend.ParseCypher(frontend.NewContext(), "CREATE (n) WITH n RETURN unresolved")
		require.NoError(t, err)
		result := owner.queryMultipart(query, nil, 0, SQLGenerationProfile{})
		require.False(t, result.Next())
		require.Error(t, result.Error())
		require.Nil(t, owner.stagedError)
	})
}

type multipartTestResult struct {
	remaining int
	closed    int
	err       error
	closeErr  error
	scanErr   error
}

func (s *multipartTestResult) Next() bool {
	if s.remaining == 0 {
		return false
	}
	s.remaining--
	return true
}
func (s *multipartTestResult) Keys() []string            { return []string{"value"} }
func (s *multipartTestResult) Values() []any             { return []any{int64(1)} }
func (s *multipartTestResult) Mapper() graph.ValueMapper { return graph.ValueMapper{} }
func (s *multipartTestResult) Scan(...any) error         { return s.scanErr }
func (s *multipartTestResult) Error() error              { return s.err }
func (s *multipartTestResult) Close() {
	s.closed++
	if s.closeErr != nil {
		s.err = s.closeErr
	}
}

func TestMultipartResultCleansUpOnExhaustionAndEarlyClose(t *testing.T) {
	for _, earlyClose := range []bool{false, true} {
		name := "exhaustion"
		if earlyClose {
			name = "early_close"
		}
		t.Run(name, func(t *testing.T) {
			owner, mock := newMultipartMockTransaction(t)
			source := &multipartTestResult{remaining: 2}
			result := &stagedQueryResult{Result: source, owner: owner, tables: []string{`"pg_temp"."stage0"`, `"pg_temp"."stage1"`}}
			mock.ExpectExec(`drop table "pg_temp"."stage0", "pg_temp"."stage1"`).WillReturnResult(pgxmock.NewResult("DROP TABLE", 0))
			require.True(t, result.Next())
			if earlyClose {
				result.Close()
			} else {
				require.True(t, result.Next())
				require.False(t, result.Next())
			}
			require.NoError(t, result.Error())
			require.Nil(t, owner.stagedError)
			require.Equal(t, 1, source.closed)
			result.Close()
			require.False(t, result.Next())
			require.Equal(t, 1, source.closed, "cleanup must be idempotent")
		})
	}
}

func TestMultipartResultPreservesLateFailureAndPreventsCommit(t *testing.T) {
	owner, _ := newMultipartMockTransaction(t)
	expected := errors.New("server failed while draining remaining rows")
	source := &multipartTestResult{remaining: 2, closeErr: expected}
	result := &stagedQueryResult{Result: source, owner: owner, tables: []string{"temporary_stage"}}
	require.True(t, result.Next())
	result.Close()
	require.ErrorIs(t, result.Error(), expected)
	require.ErrorIs(t, owner.Commit(), expected, "caught row errors must not allow earlier stages to commit")
	result.Close()
	require.Equal(t, 1, source.closed)
}

func TestMultipartResultScanFailurePreventsCommitAndSurvivesClose(t *testing.T) {
	owner, _ := newMultipartMockTransaction(t)
	expected := errors.New("decode failed after earlier stages completed")
	source := &multipartTestResult{remaining: 1, scanErr: expected, closeErr: errors.New("secondary drain error")}
	result := &stagedQueryResult{Result: source, owner: owner, tables: []string{"temporary_stage"}}
	require.True(t, result.Next())
	var value int64
	require.ErrorIs(t, result.Scan(&value), expected)
	require.ErrorIs(t, owner.Commit(), expected)
	result.Close()
	require.ErrorIs(t, result.Error(), expected, "closing must preserve the original decoding failure")
	require.ErrorIs(t, owner.Commit(), expected)
}

func TestMultipartResultCleanupFailurePreventsCommit(t *testing.T) {
	owner, mock := newMultipartMockTransaction(t)
	expected := errors.New("temporary relation cleanup failed")
	mock.ExpectExec("drop table temporary_stage").WillReturnError(expected)
	result := &stagedQueryResult{Result: &multipartTestResult{}, owner: owner, tables: []string{"temporary_stage"}}
	require.False(t, result.Next())
	require.ErrorIs(t, result.Error(), expected)
	require.ErrorIs(t, owner.Commit(), expected)
	result.Close()
}

func TestMultipartWriteRequiresTransactionBeforeExecutingSQL(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), "CREATE (n) WITH n RETURN n")
	require.NoError(t, err)
	owner := &transaction{ctx: context.Background(), schemaManager: &SchemaManager{}, queryExecMode: pgx.QueryExecModeExec}
	// No connection is supplied: any SQL execution before rejection would panic.
	result := owner.queryMultipart(query, nil, 0, SQLGenerationProfile{})
	require.False(t, result.Next())
	require.ErrorContains(t, result.Error(), "require an explicit PostgreSQL write transaction")
	result.Close()
}
