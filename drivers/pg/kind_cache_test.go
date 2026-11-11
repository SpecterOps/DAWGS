package pg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func testKindManager(t *testing.T) *SchemaManager {
	t.Helper()
	manager := NewSchemaManager(nil, 0)
	t.Cleanup(func() { manager.parseCache.Close(); manager.compilationCache.Close() })
	return manager
}

func TestKindCachePublicationInterleavings(t *testing.T) {
	cache := newKindCache()
	a, b := graph.StringKind("A"), graph.StringKind("B")
	original := cache.current.Load()
	require.True(t, cache.merge(nil, 0))
	require.Same(t, original, cache.current.Load())
	var wg sync.WaitGroup
	for kind, id := range map[graph.Kind]int16{a: 1, b: 2} {
		wg.Add(1)
		go func() { defer wg.Done(); cache.merge(map[graph.Kind]int16{kind: id}, 0) }()
	}
	wg.Wait()
	both := cache.current.Load()
	require.Len(t, both.kinds, 2)
	require.Empty(t, original.kinds)
	require.True(t, cache.merge(map[graph.Kind]int16{a: 1}, 0))
	require.Same(t, both, cache.current.Load())
	cache.invalidate()
	generation := cache.requested.Load()
	// A concurrent add fences an older complete fetch.
	require.True(t, cache.merge(map[graph.Kind]int16{graph.StringKind("C"): 3}, 0))
	require.False(t, cache.replace(both, generation, map[graph.Kind]int16{a: 1}, false, func() { t.Fatal("raced fetch invalidated translations") }))
	current := cache.current.Load()
	cache.invalidate()
	require.False(t, cache.replace(current, generation, map[graph.Kind]int16{}, false, func() {}))
	require.True(t, cache.pending(), "a newer request cannot be acknowledged")
	invalidations := 0
	require.True(t, cache.replace(current, cache.requested.Load(), map[graph.Kind]int16{b: 2}, false, func() {
		invalidations++
		require.NotContains(t, cache.current.Load().kinds, a)
		require.Contains(t, cache.current.Load().kinds, b)
	}))
	require.Equal(t, 1, invalidations)
	require.False(t, cache.pending())
	require.NotContains(t, cache.current.Load().kinds, a)
	require.False(t, cache.merge(map[graph.Kind]int16{a: 1}, 0), "old overlays cannot resurrect deleted mappings")
	require.True(t, cache.pending())
	require.NotContains(t, cache.current.Load().kinds, a)
	cache.fence()
	require.EqualValues(t, 2, cache.current.Load().epoch)
}

func TestKindCacheAddOnlyReloadKeepsTranslationGeneration(t *testing.T) {
	cache := newKindCache()
	base := cache.current.Load()
	require.True(t, cache.replace(base, cache.requested.Load(), map[graph.Kind]int16{graph.StringKind("A"): 1}, false, func() { t.Fatal("add-only reload invalidated translations") }))
	require.Zero(t, cache.current.Load().epoch)
	require.True(t, cache.replace(cache.current.Load(), cache.requested.Load(), map[graph.Kind]int16{graph.StringKind("A"): 2}, false, func() {}))
	require.EqualValues(t, 1, cache.current.Load().epoch)
	require.NotContains(t, cache.current.Load().ids, int16(1))
}

// TestKindMapperTargetedMisses checks targeted kind resolution without unnecessary full catalog reloads.
func TestKindMapperTargetedMisses(t *testing.T) {
	manager := testKindManager(t)
	known, found, missing := graph.StringKind("Known"), graph.StringKind("Found"), graph.StringKind("Missing")
	manager.kindCache.merge(map[graph.Kind]int16{known: 1}, 0)
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	tx := &transaction{
		ctx:           context.Background(),
		tx:            mock,
		schemaManager: manager,
	}
	mapper := &transactionKindMapper{
		tx:       tx,
		snapshot: manager.kindCache.current.Load(),
	}
	tx.kindMapper = mapper
	before := mapper.snapshot
	ids, err := mapper.MapKinds(tx.ctx, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Nil(t, mapper.kinds)
	mock.ExpectQuery(`select id, name from kind where name = ANY`).WithArgs(pgx.NamedArgs{"names": []string{"Found", "Missing"}}).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(int16(2), "Found"))
	_, err = mapper.MapKinds(tx.ctx, graph.Kinds{known, found, missing, found, missing, known})
	var absent *pgsql.MissingKindsError
	require.ErrorAs(t, err, &absent)
	require.Equal(t, []int16{1, 2, 2, 1}, absent.KindIDs)
	require.Equal(t, graph.Kinds{missing, missing}, absent.Kinds)
	require.False(t, mapper.hasUncommittedKinds)
	require.True(t, mapper.bypassTranslations(), "transaction-visible lookups must remain private in both translation layers")
	require.Same(t, before, manager.kindCache.current.Load(), "transaction-visible reads stay private")
	mock.ExpectQuery(`select id, name from kind where name = ANY`).WithArgs(pgx.NamedArgs{"names": []string{"Missing"}}).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}))
	_, err = mapper.MapKind(tx.ctx, missing)
	require.Error(t, err, "negative resolutions are not retained")
	mapper.publish()
	require.Contains(t, manager.kindCache.current.Load().kinds, found)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestKindMapperResultMissIsMemoryOnly(t *testing.T) {
	manager := testKindManager(t)
	tx := &transaction{schemaManager: manager}
	mapper := &transactionKindMapper{tx: tx, snapshot: manager.kindCache.current.Load()}
	ctx := context.Background()
	_, err := mapper.MapKindID(ctx, 7)
	require.ErrorContains(t, err, "unable to map kind id: 7")
	require.True(t, manager.kindCache.pending())
	require.Nil(t, mapper.kinds)
	known := graph.StringKind("Known")
	manager.kindCache.merge(map[graph.Kind]int16{known: 7}, 0)
	kind, err := mapper.MapKindID(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, known, kind)
	require.Nil(t, mapper.kinds)
	kinds, err := manager.MapKindIDs(ctx, []int16{7, 7})
	require.NoError(t, err)
	require.Equal(t, graph.Kinds{known, known}, kinds)
}

// TestKindMapperCommitFailureDoesNotPublish checks that failed transactions cannot publish their private kind mappings.
func TestKindMapperCommitFailureDoesNotPublish(t *testing.T) {
	manager := testKindManager(t)
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	tx := &transaction{
		ctx:           context.Background(),
		tx:            mock,
		schemaManager: manager,
	}
	tx.kindMapper = &transactionKindMapper{
		tx:       tx,
		snapshot: manager.kindCache.current.Load(),
	}
	failure := errors.New("commit failed")
	mock.ExpectQuery("with").WithArgs(pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int16(7)))
	_, err = tx.kindMapper.AssertKinds(tx.ctx, graph.Kinds{graph.StringKind("Private")})
	require.NoError(t, err)
	// Refresh cannot accidentally publish the transaction's private catalog view.
	cache := manager.kindCache
	require.True(t, cache.replace(cache.current.Load(), cache.requested.Load(), map[graph.Kind]int16{}, false, func() {}))
	require.Empty(t, cache.current.Load().kinds)
	mock.ExpectCommit().WillReturnError(failure)
	require.ErrorIs(t, tx.Commit(), failure)
	require.Empty(t, cache.current.Load().kinds)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestKindMapperUnchangedCommitRetainsSnapshot(t *testing.T) {
	manager := testKindManager(t)
	before := manager.kindCache.current.Load()
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		tx, err := newTransactionWrapper(context.Background(), nil, manager, &Config{}, false)
		require.NoError(t, err)
		require.Same(t, before, tx.kindMapper.snapshot)
		tx.tx = mock
		mock.ExpectCommit()
		require.NoError(t, tx.Commit())
		require.Nil(t, tx.kindMapper.kinds)
	}
	require.NoError(t, mock.ExpectationsWereMet())
	require.Same(t, before, manager.kindCache.current.Load())
}

// TestKindReloadFailureRetainsSnapshotAndRequest checks that reload failures preserve the last snapshot and pending invalidation.
func TestKindReloadFailureRetainsSnapshotAndRequest(t *testing.T) {
	manager := testKindManager(t)
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	tx := &transaction{
		ctx:           context.Background(),
		tx:            mock,
		schemaManager: manager,
	}
	tx.kindMapper = &transactionKindMapper{
		tx: tx,
	}
	manager.kindCache.invalidate()
	before := manager.kindCache.current.Load()
	failure := errors.New("catalog unavailable")
	mock.ExpectQuery("select id, name").WillReturnError(failure)
	installed, err := tx.reloadKinds(false)
	require.False(t, installed)
	require.ErrorIs(t, err, failure)
	require.Same(t, before, manager.kindCache.current.Load())
	require.True(t, manager.kindCache.pending())
	require.NoError(t, mock.ExpectationsWereMet())
}

var benchmarkKindMapper *transactionKindMapper
var benchmarkKinds graph.Kinds
var benchmarkKindID int16
var benchmarkKindIDs map[int16]graph.Kind

// benchmarkKindTransaction supplies transaction behavior needed by the kind-mapping benchmarks.
type benchmarkKindTransaction struct {
	// Tx provides the delegated transaction operations.
	pgx.Tx
}

func (benchmarkKindTransaction) Commit(context.Context) error { return nil }

func BenchmarkKindMetadata(b *testing.B) {
	for _, size := range []int{32, 715, 7150} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			manager := NewSchemaManager(nil, 0)
			defer manager.parseCache.Close()
			defer manager.compilationCache.Close()
			kinds := make(map[graph.Kind]int16, size)
			for i := 0; i < size; i++ {
				kinds[graph.StringKind(fmt.Sprint("K", i))] = int16(i + 1)
			}
			cache := manager.kindCache
			cache.replace(cache.current.Load(), cache.requested.Load(), kinds, false, func() {})
			tx := &transaction{schemaManager: manager}
			mapper := &transactionKindMapper{tx: tx, snapshot: manager.kindCache.current.Load()}
			b.Run("wrapper", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					wrapper, _ := newTransactionWrapper(context.Background(), nil, manager, &Config{}, false)
					benchmarkKindMapper = wrapper.kindMapper
				}
			})
			b.Run("overlay_lookup", func(b *testing.B) {
				mapper.learn(graph.StringKind("Private"), 30000)
				kind := graph.StringKind("K1")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchmarkKindID, _ = mapper.kindID(kind)
				}
			})
			b.Run("unchanged_commit", func(b *testing.B) {
				b.ReportAllocs()
				owner := &transaction{tx: benchmarkKindTransaction{}, kindMapper: &transactionKindMapper{tx: tx, snapshot: manager.kindCache.current.Load()}}
				for i := 0; i < b.N; i++ {
					owner.Commit()
				}
			})
			b.Run("result_lookup", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					benchmarkKinds, _ = mapper.MapKindIDs(context.Background(), []int16{1})
				}
			})
			b.Run("publication", func(b *testing.B) {
				base := manager.kindCache.current.Load()
				changes := map[graph.Kind]int16{graph.StringKind("Publish"): 30001}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					manager.kindCache.current.Store(base)
					manager.kindCache.merge(changes, base.epoch)
				}
				b.StopTimer()
				manager.kindCache.current.Store(base)
			})

			b.Run("owned_copy", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					benchmarkKindIDs = manager.GetKindIDsByKind()
				}
			})
		})
	}
}

func TestKindMapperOverlaySurvivesCompatibleAdoptionAndFencesDestruction(t *testing.T) {
	manager := testKindManager(t)
	tx := &transaction{schemaManager: manager}
	mapper := &transactionKindMapper{tx: tx, snapshot: manager.kindCache.current.Load()}
	tx.kindMapper = mapper
	private := graph.StringKind("Private")
	mapper.learn(private, 7)
	mapper.hasUncommittedKinds = true
	manager.kindCache.merge(map[graph.Kind]int16{graph.StringKind("Committed"): 8}, 0)
	mapper.adopt()
	require.EqualValues(t, 7, mapper.kinds[private])
	require.True(t, mapper.bypassTranslations())
	mapper.hasUncommittedKinds = false
	require.False(t, mapper.bypassTranslations())
	manager.kindCache.fence()
	mapper.adopt()
	require.Empty(t, mapper.kinds, "incompatible lookups must be resolved again after adoption")
	require.Same(t, manager.kindCache.current.Load(), mapper.snapshot)
	mapper.publish()
	require.NotContains(t, manager.kindCache.current.Load().kinds, private)
	require.True(t, manager.kindCache.pending())
	// An ID miss can still recheck newer published metadata without SQL.
	known := graph.StringKind("NewEpoch")
	manager.kindCache.merge(map[graph.Kind]int16{known: 9}, 1)
	kind, err := mapper.MapKindID(context.Background(), 9)
	require.NoError(t, err)
	require.Equal(t, known, kind)
}

func TestKindCacheConflictingMappingsRequestReload(t *testing.T) {
	for _, changes := range []map[graph.Kind]int16{
		{graph.StringKind("A"): 2},
		{graph.StringKind("B"): 1},
	} {
		cache := newKindCache()
		cache.merge(map[graph.Kind]int16{graph.StringKind("A"): 1}, 0)
		before := cache.current.Load()
		require.False(t, cache.merge(changes, 0))
		require.True(t, cache.pending())
		require.Same(t, before, cache.current.Load())
	}
}

func TestKindMapperSchemaCatalogDoesNotTrustSharedPositives(t *testing.T) {
	manager := testKindManager(t)
	kind := graph.StringKind("Deleted")
	manager.kindCache.merge(map[graph.Kind]int16{kind: 7}, 0)
	mapper := &transactionKindMapper{tx: &transaction{schemaManager: manager}, snapshot: &kindSnapshot{}, privateCatalog: true}
	_, ok := mapper.kindID(kind)
	require.False(t, ok)
}

// TestKindMapperStableSnapshotDoesNotAdoptDestructiveReload checks that stable transactions retain their owned mappings across destructive refreshes.
func TestKindMapperStableSnapshotDoesNotAdoptDestructiveReload(t *testing.T) {
	manager := testKindManager(t)
	known, learned, private := graph.StringKind("Known"), graph.StringKind("Learned"), graph.StringKind("Private")
	require.True(t, manager.kindCache.merge(map[graph.Kind]int16{known: 1}, 0))
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	tx := &transaction{
		ctx:           context.Background(),
		schemaManager: manager,
		tx:            mock,
		isolation:     pgx.RepeatableRead,
	}
	mapper := &transactionKindMapper{
		tx:       tx,
		snapshot: manager.kindCache.current.Load(),
	}
	tx.kindMapper = mapper
	mapper.learn(learned, 2)
	mapper.register(private, 3)
	mapper.hasUncommittedKinds = true
	before := mapper.snapshot
	manager.kindCache.fence()
	mock.ExpectQuery("select id, name from kind").
		WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(int16(1), known.String()).AddRow(int16(3), private.String()))
	require.NoError(t, mapper.adopt())
	require.Equal(t, before.epoch, mapper.snapshot.epoch)
	require.Equal(t, map[graph.Kind]int16{known: 1, private: 3}, mapper.snapshot.kinds)
	require.Empty(t, mapper.kinds, "the authoritative catalog replaces earlier private lookups")
	require.EqualValues(t, 3, mapper.registeredKinds[private])
	require.True(t, mapper.privateCatalog)
	require.True(t, mapper.bypassTranslations())
	// Later refreshes cannot change this transaction's authoritative view or
	// trigger another catalog read. Its registrations remain fenced on commit.
	manager.kindCache.fence()
	require.NoError(t, mapper.adopt())
	mock.ExpectCommit()
	require.NoError(t, tx.Commit())
	require.NotContains(t, manager.kindCache.current.Load().kinds, private)
	require.NoError(t, mock.ExpectationsWereMet())
}
