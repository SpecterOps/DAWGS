//go:build manual_integration

package v2

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

var (
	v2IntegrationNodeKind = graph.StringKind("PGV2IntegrationNode")
	v2IntegrationEdgeKind = graph.StringKind("PGV2IntegrationEdge")
	v2IntegrationSchema   = graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "pg_v2_integration",
			Nodes: graph.Kinds{v2IntegrationNodeKind},
			Edges: graph.Kinds{v2IntegrationEdgeKind},
		}},
		DefaultGraph: graph.Graph{Name: "pg_v2_integration"},
	}
)

type v2IntegrationFixture struct {
	start graph.ID
	end   graph.ID
}

func postgresV2IntegrationConnectionString(t *testing.T) string {
	t.Helper()
	connectionString := os.Getenv("CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	normalized := strings.ToLower(connectionString)
	if !strings.HasPrefix(normalized, "postgres://") && !strings.HasPrefix(normalized, "postgresql://") {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	if err := databaseguard.ValidateEnvironment(connectionString); err != nil {
		t.Fatalf("integration database safety check failed: %v", err)
	}
	return connectionString
}

// newV2IntegrationDriver creates a deterministic test pool after exercising
// the same production hook composition used by NewPool. Production continues
// to mirror v1's fixed pool sizing; this test helper uses a bounded pool only
// to prove physical-connection lifecycle behavior.
func newV2IntegrationDriver(t *testing.T, maxConns int32, capacity int, afterRelease func(*pgx.Conn) bool) *Driver {
	t.Helper()
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(postgresV2IntegrationConnectionString(t))
	require.NoError(t, err)
	poolConfig.MinConns = 0
	poolConfig.MaxConns = maxConns
	poolConfig.AfterRelease = afterRelease

	config := Config{
		TranslationCacheEntries: capacity,
		Pool:                    &PoolConfig{MinConnections: 0, MaxConnections: maxConns},
	}
	provider, err := newConnectionCacheProvider(config)
	require.NoError(t, err)
	configuredPool, err := composePoolConfig(poolConfig, config, provider, productionPoolLifecycleHooks())
	require.NoError(t, err)
	underlying, err := pgxpool.NewWithConfig(ctx, configuredPool)
	require.NoError(t, err)
	driver := NewDriver(0, &Pool{pool: underlying, provider: provider})
	t.Cleanup(func() {
		require.NoError(t, driver.Close(context.Background()))
	})
	return driver
}

func newV1IntegrationDriver(t *testing.T) *pg.Driver {
	t.Helper()
	poolConfig, err := pgxpool.ParseConfig(postgresV2IntegrationConnectionString(t))
	require.NoError(t, err)
	pool, err := pg.NewPool(poolConfig)
	require.NoError(t, err)
	driver := pg.NewDriver(0, pool)
	t.Cleanup(func() {
		require.NoError(t, driver.Close(context.Background()))
	})
	return driver
}

func requireEventually(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func setUpV2IntegrationGraph(t *testing.T, driver *Driver) v2IntegrationFixture {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, driver.AssertSchema(ctx, v2IntegrationSchema))
	t.Cleanup(func() {
		_ = driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
			return tx.Nodes().Delete()
		})
	})
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	}))

	fixture := v2IntegrationFixture{}
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		start, err := tx.CreateNode(graph.NewProperties().Set("name", "start"), v2IntegrationNodeKind)
		if err != nil {
			return err
		}
		end, err := tx.CreateNode(graph.NewProperties().Set("name", "end"), v2IntegrationNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(start.ID, end.ID, v2IntegrationEdgeKind, graph.NewProperties().Set("name", "edge")); err != nil {
			return err
		}
		fixture.start = start.ID
		fixture.end = end.ID
		return nil
	}))
	return fixture
}

func snapshotQuery(ctx context.Context, database graph.Database, query string, parameters map[string]any) ([][]any, error) {
	var rows [][]any
	err := database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(query, parameters)
		defer result.Close()
		for result.Next() {
			rows = append(rows, append([]any(nil), result.Values()...))
		}
		return result.Error()
	})
	return rows, err
}

func rawExecutionError(ctx context.Context, database graph.Database) error {
	return database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw("select 1 / 0", nil)
		defer result.Close()
		for result.Next() {
		}
		return result.Error()
	})
}

func v2ScalarQuery() string {
	return "MATCH (n:PGV2IntegrationNode) RETURN count(n)"
}

func TestV2NewPoolConstructsAnExplicitOptInDriver(t *testing.T) {
	poolConfig, err := pgxpool.ParseConfig(postgresV2IntegrationConnectionString(t))
	require.NoError(t, err)
	pool, err := NewPool(context.Background(), poolConfig, Config{
		TranslationCacheEntries: 2,
		Pool:                    &PoolConfig{MinConnections: 0, MaxConnections: 1},
	})
	require.NoError(t, err)
	driver := NewDriver(0, pool)
	t.Cleanup(func() {
		require.NoError(t, driver.Close(context.Background()))
	})
	setUpV2IntegrationGraph(t, driver)

	_, err = snapshotQuery(context.Background(), driver, v2ScalarQuery(), nil)
	require.NoError(t, err)
	stats := driver.TranslationCacheStats()
	require.Equal(t, 2, stats.CapacityPerConnection)
	require.Equal(t, int32(0), stats.MinConnections)
	require.Equal(t, int32(1), stats.MaxConnections)
	require.NotEmpty(t, stats.Connections)
}

func TestV2TranslationCacheSurvivesLeaseReleaseAndReacquisition(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	_, err := snapshotQuery(context.Background(), driver, v2ScalarQuery(), nil)
	require.NoError(t, err)
	first := driver.TranslationCacheStats()
	require.Len(t, first.Connections, 1)
	require.Equal(t, uint64(1), first.Aggregate.Misses)
	connectionID := first.Connections[0].ID

	_, err = snapshotQuery(context.Background(), driver, v2ScalarQuery(), nil)
	require.NoError(t, err)
	second := driver.TranslationCacheStats()
	require.Len(t, second.Connections, 1)
	require.Equal(t, connectionID, second.Connections[0].ID)
	require.Equal(t, uint64(1), second.Aggregate.Hits)
}

// TestV2StableSnapshotTraversalWorkspaceReadiness verifies that the v2
// provider avoids redundant setup only for the same live connection and
// schema generation.
func TestV2StableSnapshotTraversalWorkspaceReadiness(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	stableSnapshot := func() error {
		return driver.ReadTransaction(ctx, func(graph.Transaction) error { return nil }, pg.OptionSetTransactionIsolation(pgx.RepeatableRead))
	}

	require.NoError(t, stableSnapshot())
	require.NoError(t, stableSnapshot())
	stats := driver.TranslationCacheStats()
	require.Equal(t, uint64(1), stats.TraversalWorkspace.Initializations)
	require.Equal(t, uint64(1), stats.TraversalWorkspace.Reuses)
	require.True(t, stats.Connections[0].TraversalWorkspace.Ready)

	require.NoError(t, driver.RefreshKinds(ctx))
	stats = driver.TranslationCacheStats()
	require.False(t, stats.Connections[0].TraversalWorkspace.Ready)
	require.NoError(t, stableSnapshot())
	stats = driver.TranslationCacheStats()
	require.Equal(t, uint64(2), stats.TraversalWorkspace.Initializations)
}

// TestV2StatementWarmupUsesPooledPGXCacheNames verifies that an opt-in warmup
// creates one server statement per physical connection and leaves only a
// query-text-free identity in V2 lifecycle state.
func TestV2StatementWarmupUsesPooledPGXCacheNames(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	require.NoError(t, driver.WarmStatements(ctx, "select 1", "select 1"))

	connection, err := driver.pool.pool.Acquire(ctx)
	require.NoError(t, err)
	identity := sha256.Sum256([]byte("select 1"))
	var prepared int
	require.NoError(t, connection.QueryRow(ctx, "select count(*) from pg_prepared_statements where name = $1", pgxStatementCacheName(identity)).Scan(&prepared))
	require.Equal(t, 1, prepared)
	var value int
	require.NoError(t, connection.QueryRow(ctx, "select 1", pgx.QueryExecModeCacheStatement).Scan(&value))
	require.Equal(t, 1, value)

	stats := driver.TranslationCacheStats()
	require.Equal(t, uint64(1), stats.PreparedStatements.Prepared)
	require.Equal(t, 1, stats.PreparedStatements.Entries)

	connection.Release()
	driver.pool.Reset()
	requireEventually(t, func() bool {
		stats := driver.TranslationCacheStats()
		return stats.LiveConnections == 0 && stats.PreparedStatements.Entries == 0
	}, "prepared statement retirement")
}

func TestV2PhysicalConnectionsHaveIndependentCaches(t *testing.T) {
	driver := newV2IntegrationDriver(t, 2, 2, nil)
	ctx := context.Background()
	first, err := driver.pool.pool.Acquire(ctx)
	require.NoError(t, err)
	defer first.Release()
	second, err := driver.pool.pool.Acquire(ctx)
	require.NoError(t, err)
	defer second.Release()
	require.NotSame(t, first.Conn(), second.Conn())

	firstCache, ok := driver.pool.provider.CacheForConnection(first.Conn()).(*connectionTranslationCache)
	require.True(t, ok)
	secondCache, ok := driver.pool.provider.CacheForConnection(second.Conn()).(*connectionTranslationCache)
	require.True(t, ok)
	_, _, err = firstCache.TranslateWithPolicy("RETURN 1", 1, nil, "integration", func() (translate.Result, string, error) {
		return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, firstCache.statsSnapshot().Entries)
	require.Zero(t, secondCache.statsSnapshot().Entries)
}

func TestV2ConnectionCloseAndRejectedReleaseDestroyCaches(t *testing.T) {
	t.Run("pool reset retires state and replacement starts empty", func(t *testing.T) {
		driver := newV2IntegrationDriver(t, 1, 2, nil)
		setUpV2IntegrationGraph(t, driver)
		_, err := snapshotQuery(context.Background(), driver, v2ScalarQuery(), nil)
		require.NoError(t, err)
		before := driver.TranslationCacheStats()
		require.Len(t, before.Connections, 1)
		oldID := before.Connections[0].ID

		driver.pool.Reset()
		requireEventually(t, func() bool {
			stats := driver.TranslationCacheStats()
			return stats.LiveConnections == 0 && stats.RetiredConnections >= 1
		}, "physical connection cache retirement")

		_, err = snapshotQuery(context.Background(), driver, v2ScalarQuery(), nil)
		require.NoError(t, err)
		after := driver.TranslationCacheStats()
		require.Len(t, after.Connections, 1)
		require.NotEqual(t, oldID, after.Connections[0].ID)
		require.GreaterOrEqual(t, after.Aggregate.Misses, uint64(2))
	})

	t.Run("rejected release closes registered cache", func(t *testing.T) {
		driver := newV2IntegrationDriver(t, 1, 2, func(*pgx.Conn) bool { return false })
		setUpV2IntegrationGraph(t, driver)
		conn, err := driver.pool.pool.Acquire(context.Background())
		require.NoError(t, err)
		physical := conn.Conn()
		cache, ok := driver.pool.provider.CacheForConnection(physical).(*connectionTranslationCache)
		require.True(t, ok)
		_, _, err = cache.TranslateWithPolicy("RETURN 1", 1, nil, "integration", func() (translate.Result, string, error) {
			return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
		})
		require.NoError(t, err)
		conn.Release()
		requireEventually(t, func() bool {
			return driver.pool.provider.CacheForConnection(physical) == nil
		}, "rejected-release cache cleanup")
	})
}

func TestV2SchemaGenerationAndCapacityPreventStaleTranslationReuse(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 1, nil)
	fixture := setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	_, err := snapshotQuery(ctx, driver, v2ScalarQuery(), nil)
	require.NoError(t, err)
	before := driver.TranslationCacheStats()
	require.Len(t, before.Connections, 1)
	connectionID := before.Connections[0].ID
	generation := before.SchemaGeneration

	require.NoError(t, driver.RefreshKinds(ctx))
	_, err = snapshotQuery(ctx, driver, v2ScalarQuery(), nil)
	require.NoError(t, err)
	afterGeneration := driver.TranslationCacheStats()
	require.Equal(t, connectionID, afterGeneration.Connections[0].ID)
	require.Greater(t, afterGeneration.SchemaGeneration, generation)
	require.GreaterOrEqual(t, afterGeneration.Aggregate.Misses, uint64(2))

	_, err = snapshotQuery(ctx, driver, "MATCH (n:PGV2IntegrationNode) WHERE id(n) = $id RETURN n", map[string]any{"id": fixture.start})
	require.NoError(t, err)
	stats := driver.TranslationCacheStats()
	require.LessOrEqual(t, stats.Connections[0].Translation.Entries, 1)
	require.Equal(t, 1, stats.CapacityPerConnection)
}

func TestV2MatchesV1ForCoreResultsAndFailureBoundaries(t *testing.T) {
	v2Driver := newV2IntegrationDriver(t, 1, 4, nil)
	fixture := setUpV2IntegrationGraph(t, v2Driver)
	v1Driver := newV1IntegrationDriver(t)
	require.NoError(t, v1Driver.AssertSchema(context.Background(), v2IntegrationSchema))

	for _, testCase := range []struct {
		name       string
		query      string
		parameters map[string]any
	}{
		{"scalar", v2ScalarQuery(), nil},
		{"node", "MATCH (n:PGV2IntegrationNode) WHERE id(n) = $id RETURN n", map[string]any{"id": fixture.start}},
		{"relationship", "MATCH ()-[r:PGV2IntegrationEdge]->() RETURN r", nil},
		{"path", "MATCH p = (a:PGV2IntegrationNode)-[:PGV2IntegrationEdge]->(b:PGV2IntegrationNode) WHERE id(a) = $start_id AND id(b) = $end_id RETURN p", map[string]any{"start_id": fixture.start, "end_id": fixture.end}},
		{"no rows", "MATCH (n:PGV2IntegrationNode) WHERE id(n) = $id RETURN n", map[string]any{"id": graph.ID(0)}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			v1Rows, v1Err := snapshotQuery(context.Background(), v1Driver, testCase.query, testCase.parameters)
			v2Rows, v2Err := snapshotQuery(context.Background(), v2Driver, testCase.query, testCase.parameters)
			require.NoError(t, v1Err)
			require.NoError(t, v2Err)
			require.True(t, reflect.DeepEqual(v1Rows, v2Rows), "v1 rows %v differ from v2 rows %v", v1Rows, v2Rows)
		})
	}

	_, v1TranslationErr := snapshotQuery(context.Background(), v1Driver, "MATCH (", nil)
	_, v2TranslationErr := snapshotQuery(context.Background(), v2Driver, "MATCH (", nil)
	require.Error(t, v1TranslationErr)
	require.Error(t, v2TranslationErr)
	require.Error(t, rawExecutionError(context.Background(), v1Driver))
	require.Error(t, rawExecutionError(context.Background(), v2Driver))

	rollback := errors.New("force rollback")
	for _, database := range []graph.Database{v1Driver, v2Driver} {
		err := database.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties(), v2IntegrationNodeKind); err != nil {
				return err
			}
			return rollback
		})
		require.ErrorIs(t, err, rollback)
		rows, err := snapshotQuery(context.Background(), database, v2ScalarQuery(), nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(2)}}, rows)
	}

	cancelCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	cancelErr := v2Driver.ReadTransaction(cancelCtx, func(tx graph.Transaction) error {
		result := tx.Raw("select pg_sleep(1)", nil)
		defer result.Close()
		for result.Next() {
		}
		return result.Error()
	})
	require.Error(t, cancelErr)
	rows, err := snapshotQuery(context.Background(), v2Driver, v2ScalarQuery(), nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(2)}}, rows)
}
