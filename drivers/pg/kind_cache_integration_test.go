//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// kindCatalogTrace counts actual catalog statements, independently of cache
// bookkeeping. It is enabled only after fixture/schema initialization.
type kindCatalogTrace struct {
	sync.Mutex
	statements []string
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (s *kindCatalogTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(strings.ToLower(data.SQL), "from kind") || strings.Contains(strings.ToLower(data.SQL), "into kind") {
		s.Lock()
		s.statements = append(s.statements, data.SQL)
		s.Unlock()
		if s.entered != nil {
			s.once.Do(func() { close(s.entered); <-s.release })
		}
	}
	return ctx
}
func (s *kindCatalogTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (s *kindCatalogTrace) reset()                                                          { s.Lock(); defer s.Unlock(); s.statements = nil }
func (s *kindCatalogTrace) count() int                                                      { s.Lock(); defer s.Unlock(); return len(s.statements) }

func traceKindCatalog(t *testing.T, driver *Driver) *kindCatalogTrace {
	t.Helper()
	trace := &kindCatalogTrace{}
	// Retain the fixture's pool bound while replacing its connections with traced ones.
	config, err := pgxpool.ParseConfig(postgresV2IntegrationConnectionString(t))
	require.NoError(t, err)
	config.ConnConfig.Tracer = trace
	pool, err := NewPoolWithRuntimeConfig(context.Background(), config, RuntimeConfig{TranslationCacheEntries: 8, Pool: &PoolConfig{MinConnections: 0, MaxConnections: driver.pool.Stat().MaxConns()}})
	require.NoError(t, err)
	original, originalRuntime, originalProvider := driver.pool, driver.runtime, driver.translationCacheProvider
	driver.pool = pool
	driver.SchemaManager.pool = pool
	driver.runtime = poolRuntimeFor(pool)
	driver.translationCacheProvider = driver.runtime.provider
	t.Cleanup(func() {
		driver.runtime.close()
		driver.pool = original
		driver.SchemaManager.pool = original
		driver.runtime = originalRuntime
		driver.translationCacheProvider = originalProvider
	})
	return trace
}

func TestKindCacheWarmQueriesDoNotReadCatalog(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	fixture := setUpV2IntegrationGraph(t, driver)
	trace := traceKindCatalog(t, driver)
	ctx := context.Background()
	for pass := 0; pass < 2; pass++ {
		require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, err := tx.Nodes().Filter(query.Kind(query.Node(), v2IntegrationNodeKind)).Count()
			if err != nil {
				return err
			}
			for _, source := range []string{"RETURN 1 AS value", "MATCH (n) RETURN count(n)", "MATCH (n:PGV2IntegrationNode) RETURN n", "MATCH (n:PGV2IntegrationNode) RETURN collect(n)", "MATCH p=(s)-[:PGV2IntegrationEdge]->(e) RETURN p"} {
				result := tx.Query(source, nil)
				for result.Next() {
					switch {
					case strings.Contains(source, "RETURN n"):
						var node graph.Node
						require.NoError(t, graph.ScanNextResult(result, &node))
						require.Contains(t, node.Kinds, v2IntegrationNodeKind)
					case strings.Contains(source, "collect"):
						var nodes []*graph.Node
						require.NoError(t, graph.ScanNextResult(result, &nodes))
						require.NotEmpty(t, nodes)
					case strings.Contains(source, "RETURN p"):
						var path graph.Path
						require.NoError(t, graph.ScanNextResult(result, &path))
						require.NotEmpty(t, path.Nodes)
					}
				}
				result.Close()
				if err := result.Error(); err != nil {
					return err
				}
			}
			raw := tx.Raw("select (id, kind_ids, properties)::nodecomposite from node where id = @id", map[string]any{"id": fixture.start})
			defer raw.Close()
			require.True(t, raw.Next(), "%v", raw.Error())
			var node graph.Node
			require.NoError(t, graph.ScanNextResult(raw, &node))
			require.Contains(t, node.Kinds, v2IntegrationNodeKind)
			return raw.Error()
		}))
	}
	require.Zero(t, trace.count())
}

func TestKindCacheExternalRegistrationSignalsLaterRefresh(t *testing.T) {
	for _, method := range []string{"Scan", "ScanNextResult", "Mapper"} {
		t.Run(method, func(t *testing.T) {
			driver := newV2IntegrationDriver(t, 1, 8, nil)
			setUpV2IntegrationGraph(t, driver)
			other := newV2IntegrationDriver(t, 1, 8, nil)
			require.NoError(t, other.Fetch(context.Background()))
			trace := traceKindCatalog(t, driver)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fresh := graph.StringKind(fmt.Sprintf("P1External%s%d", method, time.Now().UnixNano()))
			ids, err := other.AssertKinds(ctx, graph.Kinds{fresh})
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := other.pool.Exec(context.Background(), "delete from kind where id=$1", ids[0])
				require.NoError(t, err)
			})
			// Result decoding itself must not attempt catalog SQL on its busy connection.
			require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result := tx.Raw("select array[@id]::int2[]", map[string]any{"id": ids[0]})
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				var kinds graph.Kinds
				switch method {
				case "Scan":
					require.Error(t, result.Scan(&kinds))
				case "ScanNextResult":
					require.Error(t, graph.ScanNextResult(result, &kinds))
				case "Mapper":
					require.False(t, result.Mapper().Map(result.Values()[0], &kinds))
				}
				require.Zero(t, trace.count())
				require.True(t, driver.kindCache.pending())
				return nil // deliberately close before exhausting the stream
			}))
			require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				// A pending request is serviced before BEGIN, even when every read uses an
				// explicit transaction. The failed first query was never retried.
				require.False(t, driver.kindCache.pending())
				result := tx.Raw("select array[@id]::int2[]", map[string]any{"id": ids[0]})
				defer result.Close()
				require.True(t, result.Next())
				var kinds graph.Kinds
				require.NoError(t, graph.ScanNextResult(result, &kinds))
				require.Equal(t, graph.Kinds{fresh}, kinds)
				return result.Error()
			}, OptionSetTransactionIsolation(pgx.RepeatableRead)))
			require.Equal(t, 1, trace.count())
		})
	}
}

func TestKindCacheRefreshDefersActiveTransactionAndRetainsFailures(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	trace := traceKindCatalog(t, driver)
	ctx := context.Background()
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		driver.kindCache.invalidate()
		require.NoError(t, tx.(*transaction).serviceKinds())
		require.True(t, driver.kindCache.pending())
		require.Zero(t, trace.count())
		return nil
	}, OptionSetTransactionIsolation(pgx.RepeatableRead)))
	// A broken private catalog view simulates a failing metadata read without
	// touching the shared schema or other drivers.
	conn, err := driver.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, "create temporary table kind (id int2)")
	require.NoError(t, err)
	defer conn.Exec(ctx, "drop table pg_temp.kind")
	wrapper := &transaction{ctx: ctx, conn: conn, schemaManager: driver.SchemaManager, queryExecMode: pgx.QueryExecModeCacheStatement}
	wrapper.kindMapper = &transactionKindMapper{tx: wrapper, snapshot: driver.kindCache.current.Load()}
	before := driver.kindCache.current.Load()
	require.Error(t, wrapper.serviceKinds())
	require.Same(t, before, driver.kindCache.current.Load())
	require.True(t, driver.kindCache.pending())
	_, err = conn.Exec(ctx, "drop table pg_temp.kind")
	require.NoError(t, err)
	require.NoError(t, wrapper.serviceKinds())
	require.False(t, driver.kindCache.pending())
}

func TestKindCacheDeletionInvalidatesBothTranslationLayers(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(fmt.Sprintf("automatic=%t", automatic), func(t *testing.T) {
			driver := newV2IntegrationDriver(t, 1, 8, nil)
			setUpV2IntegrationGraph(t, driver)
			ctx := context.Background()
			fresh := graph.StringKind(fmt.Sprintf("P1Delete%d", time.Now().UnixNano()))
			ids, err := driver.AssertKinds(ctx, graph.Kinds{fresh})
			require.NoError(t, err)
			source := fmt.Sprintf("MATCH (n:%s) RETURN count(n)", fresh)
			target, _ := driver.DefaultGraph()
			_, _, err = driver.compileText(ctx, source, nil, target.ID)
			require.NoError(t, err)
			require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				r := tx.Query(source, nil)
				defer r.Close()
				for r.Next() {
				}
				return r.Error()
			}))
			cacheGeneration := driver.runtime.provider.stats().SchemaGeneration
			compilerGeneration := driver.compilationCache.schemaGeneration.Load()
			_, err = driver.pool.Exec(ctx, "delete from kind where id=$1", ids[0])
			require.NoError(t, err)
			if automatic {
				driver.kindCache.invalidate()
				require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error { return nil }))
			} else {
				require.NoError(t, driver.RefreshKinds(ctx))
			}
			require.NotContains(t, driver.kindCache.current.Load().ids, ids[0])
			require.Greater(t, driver.runtime.provider.stats().SchemaGeneration, cacheGeneration)
			require.Greater(t, driver.compilationCache.schemaGeneration.Load(), compilerGeneration)
			_, err = driver.MapKinds(ctx, graph.Kinds{fresh})
			require.Error(t, err)
		})
	}
}

func TestKindCacheConcurrentRefreshCoalescesWithoutAcknowledgingNewSignal(t *testing.T) {
	driver := newV2IntegrationDriver(t, 3, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	trace := traceKindCatalog(t, driver)
	trace.entered = make(chan struct{})
	trace.release = make(chan struct{})
	defer func() {
		select {
		case <-trace.release:
		default:
			close(trace.release)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	driver.kindCache.invalidate()
	done := make(chan error, 1)
	go func() { done <- driver.ReadTransaction(ctx, func(tx graph.Transaction) error { return nil }) }()
	select {
	case <-trace.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The other callers keep using committed metadata rather than waiting for
	// the refreshing caller or leasing a second connection for metadata.
	for i := 0; i < 5; i++ {
		require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			r := tx.Raw("select 1::int8", nil)
			defer r.Close()
			require.True(t, r.Next())
			return r.Error()
		}))
	}
	require.Equal(t, 1, trace.count())
	driver.kindCache.invalidate()
	close(trace.release)
	require.NoError(t, <-done)
	require.True(t, driver.kindCache.pending(), "the in-flight fetch must not clear a later signal")
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error { return nil }))
	require.False(t, driver.kindCache.pending())
	require.Equal(t, 2, trace.count())
	// Explicit reload bypasses servicing and does just one full catalog query.
	driver.kindCache.invalidate()
	trace.reset()
	require.NoError(t, driver.RefreshKinds(ctx))
	require.Equal(t, 1, trace.count())
}

func TestKindCacheAutocommitBoundaryServicesNewSignal(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	trace := traceKindCatalog(t, driver)
	ctx := context.Background()
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		driver.kindCache.invalidate()
		result := tx.Query("RETURN 1", nil)
		defer result.Close()
		require.True(t, result.Next())
		require.False(t, driver.kindCache.pending())
		require.Equal(t, 1, trace.count())
		return result.Error()
	}))
	stats := driver.TranslationCacheStats().SQLGeneration
	require.EqualValues(t, 1, stats.KindRefresh.Count)
	require.Positive(t, stats.KindRefresh.Dispatch)
}

func TestKindCacheConcurrentSameKindRegistration(t *testing.T) {
	driver := newV2IntegrationDriver(t, 2, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fresh := graph.StringKind(fmt.Sprintf("P1Concurrent%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		_, err := driver.pool.Exec(context.Background(), "delete from kind where name=$1", fresh.String())
		require.NoError(t, err)
	})
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan error, 2)
	ids := make(chan int16, 2)
	for i := 0; i < 2; i++ {
		go func() {
			done <- driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
				ready <- struct{}{}
				<-release
				mapped, err := tx.(*transaction).kindMapper.AssertKinds(ctx, graph.Kinds{fresh})
				if err == nil {
					ids <- mapped[0]
				}
				return err
			})
		}()
	}
	<-ready
	<-ready
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	first, second := <-ids, <-ids
	require.Equal(t, first, second)
	require.Equal(t, first, driver.kindCache.current.Load().kinds[fresh])
}

func TestKindCacheSchemaAssertionRecreatesDeletedDeclaredKind(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	fresh := graph.StringKind(fmt.Sprintf("P1SchemaReset%d", time.Now().UnixNano()))
	ids, err := driver.AssertKinds(ctx, graph.Kinds{fresh})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := driver.pool.Exec(context.Background(), "delete from kind where name=$1", fresh.String())
		require.NoError(t, err)
	})
	_, err = driver.pool.Exec(ctx, "delete from kind where id=$1", ids[0])
	require.NoError(t, err)
	schema := v2IntegrationSchema
	schema.Graphs = append([]graph.Graph{}, schema.Graphs...)
	schema.Graphs[0].Nodes = append(append(graph.Kinds{}, schema.Graphs[0].Nodes...), fresh)
	require.NoError(t, driver.AssertSchema(ctx, schema))
	next, err := driver.MapKind(ctx, fresh)
	require.NoError(t, err)
	require.NotEqual(t, ids[0], next)
	require.NotContains(t, driver.kindCache.current.Load().ids, ids[0])
}

func TestKindCacheLargeBatchFailureRetainsOnlyCommittedRegistration(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	fixture := setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	fresh := graph.StringKind(fmt.Sprintf("P1LargeBatch%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		_, err := driver.pool.Exec(context.Background(), "delete from kind where name=$1", fresh.String())
		require.NoError(t, err)
	})
	node := graph.PrepareNode(graph.AsProperties(map[string]any{"invalid": make(chan int)}), fresh)
	node.ID = fixture.start
	require.Error(t, driver.BatchOperation(ctx, func(b graph.Batch) error { return b.(*batch).largeUpdate([]*graph.Node{node}) }))
	// Batches register before the independently committed graph flush. Even if
	// row encoding fails, any published ID must actually exist in PostgreSQL.
	var id int16
	require.NoError(t, driver.pool.QueryRow(ctx, "select id from kind where name=$1", fresh.String()).Scan(&id))
	require.Equal(t, id, driver.kindCache.current.Load().kinds[fresh])
}

func TestKindCacheExplicitRefreshFailureKeepsRequest(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	before := driver.GetKindIDsByKind()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, driver.RefreshKinds(ctx))
	require.Equal(t, before, driver.GetKindIDsByKind())
	require.True(t, driver.kindCache.pending())
	require.NoError(t, driver.RefreshKinds(context.Background()))
	require.False(t, driver.kindCache.pending())
}

func TestKindCacheRawCatalogRollbackDoesNotPoisonTranslationLayers(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	fresh := graph.StringKind(fmt.Sprintf("P1RawRollback%d", time.Now().UnixNano()))
	target, _ := driver.DefaultGraph()
	source := fmt.Sprintf("MATCH (n:%s) RETURN count(n)", fresh)
	t.Cleanup(func() {
		_, err := driver.pool.Exec(ctx, "delete from node where graph_id=$1 and exists(select 1 from kind where kind.name=$2 and kind.id=ANY(node.kind_ids))", target.ID, fresh.String())
		require.NoError(t, err)
		_, err = driver.pool.Exec(ctx, "delete from kind where name=$1", fresh.String())
		require.NoError(t, err)
	})
	check := func(tx graph.Transaction, want int64) error {
		result := tx.Query(source, nil)
		require.True(t, result.Next(), "%v", result.Error())
		var count int64
		err := graph.ScanNextResult(result, &count)
		result.Close()
		if err != nil {
			return err
		}
		if err := result.Error(); err != nil {
			return err
		}
		require.Equal(t, want, count)
		count, err = tx.Nodes().Filter(query.Kind(query.Node(), fresh)).Count()
		require.NoError(t, err)
		require.Equal(t, want, count)
		return err
	}
	rollback := fmt.Errorf("roll back raw metadata")
	require.ErrorIs(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw("insert into kind(name) values (@name) returning id", map[string]any{"name": fresh.String()})
		require.True(t, result.Next(), "%v", result.Error())
		result.Close()
		if err := result.Error(); err != nil {
			return err
		}
		if err := check(tx, 0); err != nil {
			return err
		}
		require.False(t, tx.(*transaction).kindMapper.hasUncommittedKinds, "raw SQL is opaque; the lookup's private view is sufficient to bypass retention")
		require.NotContains(t, driver.kindCache.current.Load().kinds, fresh)
		return rollback
	}), rollback)
	require.NotContains(t, driver.kindCache.current.Load().kinds, fresh)
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error { _, err := tx.CreateNode(graph.NewProperties(), fresh); return err }))
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error { return check(tx, 1) }))
}

func TestKindCacheRefreshPreservesRepeatableReadDecoderSnapshot(t *testing.T) {
	driver := newV2IntegrationDriver(t, 2, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	fresh := graph.StringKind(fmt.Sprintf("P1StableSnapshot%d", time.Now().UnixNano()))
	var node *graph.Node
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		node, err = tx.CreateNode(graph.NewProperties(), fresh)
		return err
	}))
	id := driver.kindCache.current.Load().kinds[fresh]
	t.Cleanup(func() {
		_, err := driver.pool.Exec(ctx, "delete from node where id=$1", node.ID)
		require.NoError(t, err)
		_, err = driver.pool.Exec(ctx, "delete from kind where id=$1", id)
		require.NoError(t, err)
	})
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		check := func(cypher bool) error {
			var result graph.Result
			if cypher {
				result = tx.Query("MATCH (n) WHERE id(n)=$id RETURN n", map[string]any{"id": int64(node.ID)})
			} else {
				result = tx.Raw("select (id,kind_ids,properties)::nodecomposite from node where id=@id", map[string]any{"id": node.ID})
			}
			defer result.Close()
			require.True(t, result.Next(), "%v", result.Error())
			var got graph.Node
			if err := graph.ScanNextResult(result, &got); err != nil {
				return err
			}
			require.Equal(t, graph.Kinds{fresh}, got.Kinds)
			return result.Error()
		}
		if err := check(false); err != nil {
			return err
		}
		before := tx.(*transaction).kindMapper.snapshot
		_, err := driver.pool.Exec(ctx, "delete from node where id=$1", node.ID)
		if err != nil {
			return err
		}
		_, err = driver.pool.Exec(ctx, "delete from kind where id=$1", id)
		if err != nil {
			return err
		}
		if err := driver.RefreshKinds(ctx); err != nil {
			return err
		}
		require.NotContains(t, driver.kindCache.current.Load().ids, id)
		if err := check(false); err != nil {
			return err
		}
		if err := check(true); err != nil {
			return err
		}
		require.Equal(t, before, tx.(*transaction).kindMapper.snapshot)
		require.True(t, tx.(*transaction).kindMapper.privateCatalog)
		require.True(t, tx.(*transaction).kindMapper.bypassTranslations())
		return nil
	}, OptionSetTransactionIsolation(pgx.RepeatableRead)))
	require.NotContains(t, driver.kindCache.current.Load().ids, id, "the older reader must not resurrect deleted metadata")
}
