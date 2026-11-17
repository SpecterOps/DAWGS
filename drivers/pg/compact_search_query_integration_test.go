//go:build manual_integration

package pg

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const (
	// compactSearchDriverGraph isolates lifecycle fixtures from other integration graphs.
	compactSearchDriverGraph = "compact_search_driver"

	// compactSearchDriverQuery exercises open property anchors through the production text entry point.
	compactSearchDriverQuery = `MATCH p=allShortestPaths((s)-[:CompactSearchStep*]->(t)) WHERE s.name=$source AND t.objectid=$target RETURN p`

	// deleteCompactSearchDriverEdgesSQL removes only this fixture's relationships before its nodes.
	deleteCompactSearchDriverEdgesSQL = `delete from edge where graph_id=(select id from graph where name=@name)`

	// deleteCompactSearchDriverNodesSQL removes only nodes belonging to the lifecycle fixture.
	deleteCompactSearchDriverNodesSQL = `delete from node where graph_id=(select id from graph where name=@name)`

	// readCompactSearchBudgetTokenSQL reads the logical query identity and its consumed state budget.
	readCompactSearchBudgetTokenSQL = `select current_setting('dawgs.traversal_token'), current_setting('dawgs.traversal_states')`

	// readCompactSearchBudgetTotalsSQL reads exact state and estimated-byte admission totals.
	readCompactSearchBudgetTotalsSQL = `select current_setting('dawgs.traversal_states')::bigint, current_setting('dawgs.traversal_bytes')::bigint`

	// setCompactSearchStateLimitSQL changes only the current transaction's admitted-state limit.
	setCompactSearchStateLimitSQL = `select set_config('dawgs.traversal_state_limit', @limit, true)`
)

// compactSearchDriverParameters bind the two logical property anchors used by lifecycle tests.
var compactSearchDriverParameters = map[string]any{"source": "source", "target": "target"}

// setupCompactSearchDriver creates and cleans an isolated diamond graph for production driver lifecycle checks.
func setupCompactSearchDriver(t *testing.T, connections int32) *Driver {
	t.Helper()
	driver := newV2IntegrationDriver(t, connections, 16, nil)
	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name: compactSearchDriverGraph,
		}},
		DefaultGraph: graph.Graph{
			Name: compactSearchDriverGraph,
		},
	}
	require.NoError(t, driver.AssertSchema(context.Background(), schema))
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, statement := range []string{deleteCompactSearchDriverEdgesSQL, deleteCompactSearchDriverNodesSQL} {
			require.NoError(t, driver.Run(ctx, statement, map[string]any{"name": compactSearchDriverGraph}))
		}
	}
	t.Cleanup(cleanup)
	cleanup()
	require.NoError(t, driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		source, err := tx.CreateNode(graph.NewProperties().Set("name", "source"), graph.StringKind("CompactSearchNode"))
		if err != nil {
			return err
		}
		target, err := tx.CreateNode(graph.NewProperties().Set("objectid", "target").Set("marker", "before"), graph.StringKind("CompactSearchNode"))
		if err != nil {
			return err
		}
		for index := 0; index < 2; index++ {
			middle, err := tx.CreateNode(graph.NewProperties().Set("branch", index), graph.StringKind("CompactSearchNode"))
			if err != nil {
				return err
			}
			for _, pair := range [][2]graph.ID{{source.ID, middle.ID}, {middle.ID, target.ID}} {
				if _, err := tx.CreateRelationshipByIDs(pair[0], pair[1], graph.StringKind("CompactSearchStep"), graph.NewProperties()); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	return driver
}

// consumeCompactSearchPaths drains and validates the two complete diamond paths and their hydrated target marker.
func consumeCompactSearchPaths(t *testing.T, result graph.Result, marker string) {
	t.Helper()
	defer result.Close()
	count := 0
	for result.Next() {
		var path graph.Path
		require.NoError(t, graph.ScanNextResult(result, &path))
		require.Len(t, path.Edges, 2)
		require.Equal(t, marker, path.Terminal().Properties.Map["marker"])
		count++
	}
	require.NoError(t, result.Error())
	require.Equal(t, 2, count)
}

// TestCompactSearchDriverOwnsSnapshotUntilDrainOrClose checks automatic snapshot ownership, finalization, and repeated use of one physical connection.
func TestCompactSearchDriverOwnsSnapshotUntilDrainOrClose(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 1)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		pid := owner.conn.Conn().PgConn().PID()
		for _, earlyClose := range []bool{false, true, false} {
			require.Nil(t, owner.tx)
			result := tx.Query(compactSearchDriverQuery, compactSearchDriverParameters)
			require.IsType(t, &compactSearchQueryResult{}, result)
			require.NotNil(t, owner.compactSearch)
			require.Equal(t, pgx.RepeatableRead, owner.isolation)
			if earlyClose {
				require.True(t, result.Next(), "%v", result.Error())
				result.Close()
				require.NoError(t, result.Error())
			} else {
				consumeCompactSearchPaths(t, result, "before")
			}
			require.Nil(t, owner.tx)
			require.Nil(t, owner.compactSearch)
			require.Empty(t, owner.isolation)
			require.Equal(t, byte('I'), owner.conn.Conn().PgConn().TxStatus())
			require.Equal(t, pid, owner.conn.Conn().PgConn().PID())
		}
		return nil
	}))
}

// recordingCompactTranslationCache captures the SQL selected for each context
// while preserving the real physical connection's production cache behavior.
type recordingCompactTranslationCache struct {
	// CypherTranslationCache delegates to the real physical connection cache.
	CypherTranslationCache

	// sql retains the statement selected for the latest execution context.
	sql string
}

// TranslateWithPolicy records selected SQL while preserving production cache and parameter binding behavior.
func (s *recordingCompactTranslationCache) TranslateWithPolicy(query string, graphID int32, parameters map[string]any, identity string, build func() (translate.Result, string, error)) (string, map[string]any, error) {
	sql, bindings, err := s.CypherTranslationCache.TranslateWithPolicy(query, graphID, parameters, identity, build)
	s.sql = sql
	return sql, bindings, err
}

// TestCompactSearchDriverReusesExplicitStableSnapshotAndSeparatesReadCommittedCache checks borrowed transaction ownership and isolation-specific compilation cache entries.
func TestCompactSearchDriverReusesExplicitStableSnapshotAndSeparatesReadCommittedCache(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 1)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.ReadCommitted, pgx.Serializable, pgx.ReadCommitted} {
		require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
			owner := tx.(*transaction)
			callerTx := owner.tx
			cache := &recordingCompactTranslationCache{
				CypherTranslationCache: owner.translationCache,
			}
			owner.translationCache = cache
			consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "before")
			require.Same(t, callerTx, owner.tx)
			require.Nil(t, owner.compactSearch)
			require.Equal(t, isolation, owner.isolation)
			if stableSnapshotIsolation(isolation) {
				require.Contains(t, cache.sql, "all_shortest_paths_dag_v2")
			} else {
				require.NotContains(t, cache.sql, "all_shortest_paths_dag_v2")
				require.Contains(t, cache.sql, "unidirectional_asp_harness_v2")
			}
			return nil
		}, OptionSetTransactionIsolation(isolation)))
	}
}

// TestCompactSearchDriverBudgetTokenIsFreshWithinBorrowedSnapshot verifies that independent compact queries do not share consumed admission counters.
func TestCompactSearchDriverBudgetTokenIsFreshWithinBorrowedSnapshot(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 1)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "before")
		var previousToken, admitted string
		stats := tx.Raw(readCompactSearchBudgetTokenSQL, nil)
		require.True(t, stats.Next(), "%v", stats.Error())
		require.NoError(t, graph.ScanNextResult(stats, &previousToken, &admitted))
		stats.Close()
		require.NoError(t, stats.Error())
		states, err := strconv.ParseInt(admitted, 10, 64)
		require.NoError(t, err)
		require.Positive(t, states)
		setting := tx.Raw(setCompactSearchStateLimitSQL, map[string]any{"limit": admitted})
		setting.Close()
		require.NoError(t, setting.Error())
		consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "before")
		stats = tx.Raw(readCompactSearchBudgetTokenSQL, nil)
		defer stats.Close()
		require.True(t, stats.Next(), "%v", stats.Error())
		var nextToken, nextStates string
		require.NoError(t, graph.ScanNextResult(stats, &nextToken, &nextStates))
		require.NotEqual(t, previousToken, nextToken)
		require.Equal(t, admitted, nextStates)
		return stats.Error()
	}, OptionSetTransactionIsolation(pgx.RepeatableRead)))
}

// TestCompactSearchDriverAccountsEveryResolvedEndpointPair checks five endpoint, six pair, and six root admissions.
func TestCompactSearchDriverAccountsEveryResolvedEndpointPair(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 1)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for index := 0; index < 5; index++ {
			properties := graph.NewProperties()
			if index < 2 {
				properties.Set("name", "isolated-pair-source")
			} else {
				properties.Set("objectid", "isolated-pair-target")
			}
			if _, err := tx.CreateNode(properties, graph.StringKind("CompactSearchNode")); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(
			"MATCH p=allShortestPaths((s)-[*]->(t)) WHERE s.name=$source AND t.objectid=$target RETURN p",
			map[string]any{"source": "isolated-pair-source", "target": "isolated-pair-target"},
		)
		require.False(t, result.Next())
		result.Close()
		require.NoError(t, result.Error())
		stats := tx.Raw(readCompactSearchBudgetTotalsSQL, nil)
		defer stats.Close()
		require.True(t, stats.Next(), "%v", stats.Error())
		var states, bytes int64
		require.NoError(t, graph.ScanNextResult(stats, &states, &bytes))
		require.EqualValues(t, 5+6+6, states, "five endpoints, six pairs, and six compact roots")
		require.EqualValues(t, 5*64+6*64+6*32, bytes)
		return stats.Error()
	}, OptionSetTransactionIsolation(pgx.RepeatableRead)))
}

// TestCompactSearchDriverPreparesReadonlySnapshotBeforeBegin checks that cold read-only snapshots can reuse traversal workspace without schema writes.
func TestCompactSearchDriverPreparesReadonlySnapshotBeforeBegin(t *testing.T) {
	driver := setupCompactSearchDriver(t, 1)
	driver.pool.Reset()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		callerTx := owner.tx
		consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "before")
		require.Same(t, callerTx, owner.tx)
		require.Nil(t, owner.compactSearch)
		return nil
	}, func(config *graph.TransactionConfig) {
		options := config.DriverConfig.(*Config)
		options.Options.IsoLevel = pgx.RepeatableRead
		options.Options.AccessMode = pgx.ReadOnly
	}))
}

// TestCompactSearchDriverCancellationReleasesOwnedSnapshot checks that cancellation releases the owned snapshot and permits a later successful query.
func TestCompactSearchDriverCancellationReleasesOwnedSnapshot(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 1)
		ctx, cancel = context.WithCancel(context.Background())
	)
	err := driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		result := tx.Query(compactSearchDriverQuery, compactSearchDriverParameters)
		require.IsType(t, &compactSearchQueryResult{}, result)
		require.True(t, result.Next(), "%v", result.Error())
		cancel()
		result.Close()
		require.ErrorIs(t, result.Error(), context.Canceled)
		require.Nil(t, owner.tx)
		require.Nil(t, owner.compactSearch)
		return result.Error()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "before")
		return nil
	}))
}

// TestCompactSearchDriverSnapshotRetainsHydratedValuesAcrossConcurrentWrite verifies consistent path properties during one query and fresh properties in the next query.
func TestCompactSearchDriverSnapshotRetainsHydratedValuesAcrossConcurrentWrite(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 2)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		result := tx.Query(compactSearchDriverQuery, compactSearchDriverParameters)
		require.IsType(t, &compactSearchQueryResult{}, result)
		require.True(t, result.Next(), "%v", result.Error())
		var first graph.Path
		require.NoError(t, graph.ScanNextResult(result, &first))
		require.Equal(t, "before", first.Terminal().Properties.Map["marker"])
		require.NoError(t, driver.WriteTransaction(ctx, func(writer graph.Transaction) error {
			rows := writer.Query("MATCH (n:CompactSearchNode) WHERE n.objectid=$target SET n.marker='after' RETURN n", map[string]any{"target": "target"})
			defer rows.Close()
			for rows.Next() {
			}
			return rows.Error()
		}))
		require.NotNil(t, owner.compactSearch)
		require.True(t, result.Next(), "%v", result.Error())
		var second graph.Path
		require.NoError(t, graph.ScanNextResult(result, &second))
		require.Equal(t, "before", second.Terminal().Properties.Map["marker"])
		require.False(t, result.Next())
		result.Close()
		require.NoError(t, result.Error())
		require.Nil(t, owner.tx)
		consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "after")
		return nil
	}))
}

// TestCompactSearchLegacyBuilderCompilationDeclinesSnapshotOwnedExecutor checks that compilation without execution-owned snapshot context preserves legacy SQL.
func TestCompactSearchLegacyBuilderCompilationDeclinesSnapshotOwnedExecutor(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 1)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	selectedGraph, found := driver.DefaultGraph()
	require.True(t, found)
	sql, _, compileErr := driver.compileText(ctx, compactSearchDriverQuery, compactSearchDriverParameters, selectedGraph.ID)
	require.NoError(t, compileErr)
	require.False(t, strings.Contains(sql, "all_shortest_paths_dag_v2"))
	require.Contains(t, sql, "unidirectional_asp_harness_v2")
}

// TestCompactSearchDriverResourceFailureCanReuseLease checks that admission failure releases its snapshot and preserves subsequent operation settings.
func TestCompactSearchDriverResourceFailureCanReuseLease(t *testing.T) {
	var (
		driver      = setupCompactSearchDriver(t, 1)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	)
	defer cancel()
	driver.traversalLimits.stateLimit = 1
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		result := tx.Query(compactSearchDriverQuery, compactSearchDriverParameters)
		for result.Next() {
		}
		result.Close()
		require.ErrorContains(t, result.Error(), "54000")
		require.Nil(t, owner.tx)
		require.Nil(t, owner.compactSearch)
		return nil
	}))
	driver.traversalLimits.stateLimit = 1000
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "before")
		return nil
	}))
}

// TestCompactSearchDriverRawCypherStaysOptimizedWhenBuilderCompilerDisabled checks the separate raw-Cypher and builder-compiler optimization contracts.
func TestCompactSearchDriverRawCypherStaysOptimizedWhenBuilderCompilerDisabled(t *testing.T) {
	driver := setupCompactSearchDriver(t, 1)
	previous := SetOptimizedTranslation(false)
	defer SetOptimizedTranslation(previous)
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		result := tx.Query(compactSearchDriverQuery, compactSearchDriverParameters)
		require.IsType(t, &compactSearchQueryResult{}, result)
		require.NotNil(t, owner.tx)
		consumeCompactSearchPaths(t, result, "before")
		require.Nil(t, owner.compactSearch)
		return nil
	}))
	selectedGraph, found := driver.DefaultGraph()
	require.True(t, found)
	sql, _, err := driver.compileText(context.Background(), compactSearchDriverQuery, compactSearchDriverParameters, selectedGraph.ID)
	require.NoError(t, err)
	require.NotContains(t, sql, "all_shortest_paths_dag_v2")
}

// compactFailedRollback injects a cleanup failure while retaining the real transaction connection.
type compactFailedRollback struct {
	// Tx exposes the real transaction used before cleanup failure is injected.
	pgx.Tx

	// err is the transport failure returned instead of completing rollback.
	err error
}

// Rollback returns the injected transport failure without touching the real session.
func (s compactFailedRollback) Rollback(context.Context) error {
	return s.err
}

// TestCompactSearchDriverFailedRollbackDiscardsConnection verifies that failed rollback discards its physical session before another query executes.
func TestCompactSearchDriverFailedRollbackDiscardsConnection(t *testing.T) {
	driver := setupCompactSearchDriver(t, 1)
	failure := errors.New("rollback transport failure")
	var discardedPID uint32
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		discardedPID = owner.conn.Conn().PgConn().PID()
		result := tx.Query(compactSearchDriverQuery, compactSearchDriverParameters)
		require.IsType(t, &compactSearchQueryResult{}, result)
		require.True(t, result.Next(), "%v", result.Error())
		owner.compactSearch.tx = compactFailedRollback{
			Tx:  owner.tx,
			err: failure,
		}
		result.Close()
		require.ErrorIs(t, result.Error(), failure)
		require.True(t, owner.conn.Conn().IsClosed())
		require.Nil(t, owner.tx)
		return nil
	}))
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		owner := tx.(*transaction)
		require.NotEqual(t, discardedPID, owner.conn.Conn().PgConn().PID())
		consumeCompactSearchPaths(t, tx.Query(compactSearchDriverQuery, compactSearchDriverParameters), "before")
		return nil
	}))
}
