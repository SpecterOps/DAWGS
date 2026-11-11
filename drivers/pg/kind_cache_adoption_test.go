package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestKindMapperReadCommittedAdoptsAcrossPrivateLookups checks that read-committed private lookups adopt newly published kinds.
func TestKindMapperReadCommittedAdoptsAcrossPrivateLookups(t *testing.T) {
	manager := testKindManager(t)
	known, learned, private := graph.StringKind("Known"), graph.StringKind("Learned"), graph.StringKind("Private")
	require.True(t, manager.kindCache.merge(map[graph.Kind]int16{known: 1}, 0))
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	tx := &transaction{
		ctx:           context.Background(),
		schemaManager: manager,
		tx:            mock,
		isolation:     pgx.ReadCommitted,
	}
	mapper := &transactionKindMapper{
		tx:       tx,
		snapshot: manager.kindCache.current.Load(),
	}
	mapper.learn(learned, 2)
	mapper.register(private, 3)
	mapper.hasUncommittedKinds = true

	require.True(t, manager.kindCache.replace(manager.kindCache.current.Load(), manager.kindCache.requested.Load(), map[graph.Kind]int16{known: 4, learned: 5}, false, func() {}))
	mock.ExpectQuery(`select id, name from kind where name = ANY`).WithArgs(pgx.NamedArgs{"names": []string{"Private"}}).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(int16(3), "Private"))
	require.NoError(t, mapper.adopt())

	require.Same(t, manager.kindCache.current.Load(), mapper.snapshot)
	require.Nil(t, mapper.kinds)
	require.Nil(t, mapper.ids)
	ids, missing := mapper.mapKinds(graph.Kinds{known, learned, private})
	require.Empty(t, missing)
	require.Equal(t, []int16{4, 5, 3}, ids)
	kinds, err := mapper.MapKindIDs(context.Background(), []int16{4, 5, 3})
	require.NoError(t, err)
	require.Equal(t, graph.Kinds{known, learned, private}, kinds)

	mapper.publish()
	require.NotContains(t, manager.kindCache.current.Load().kinds, private, "old registrations cannot cross the publication fence")
	require.True(t, manager.kindCache.pending())
	require.True(t, mapper.bypassTranslations(), "retained private registrations must stay out of compiler caches")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestKindMapperReadCommittedRevalidatesExistingRegistration checks an existing registration against the current catalog before reuse.
func TestKindMapperReadCommittedRevalidatesExistingRegistration(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(map[bool]string{false: "remapped", true: "removed"}[removed], func(t *testing.T) {
			manager := testKindManager(t)
			known, registered := graph.StringKind("Known"), graph.StringKind("Registered")
			require.True(t, manager.kindCache.merge(map[graph.Kind]int16{known: 1}, 0))
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)
			tx := &transaction{
				ctx:           context.Background(),
				schemaManager: manager,
				tx:            mock,
				isolation:     pgx.ReadCommitted,
			}
			mapper := &transactionKindMapper{
				tx:       tx,
				snapshot: manager.kindCache.current.Load(),
			}
			mapper.register(registered, 2)
			mapper.hasUncommittedKinds = true
			refreshed := map[graph.Kind]int16{known: 3}
			rows := pgxmock.NewRows([]string{"id", "name"})
			if !removed {
				refreshed[registered] = 4
				rows.AddRow(int16(4), "Registered")
			}
			require.True(t, manager.kindCache.replace(manager.kindCache.current.Load(), manager.kindCache.requested.Load(), refreshed, false, func() {}))
			mock.ExpectQuery(`select id, name from kind where name = ANY`).WithArgs(pgx.NamedArgs{"names": []string{"Registered"}}).WillReturnRows(rows)
			require.NoError(t, mapper.adopt())
			id, ok := mapper.kindID(registered)
			require.Equal(t, !removed, ok)
			if !removed {
				require.EqualValues(t, 4, id)
			}
			require.NotContains(t, mapper.registeredIDs, int16(2))
			mapper.publish()
			require.NotContains(t, manager.kindCache.current.Load().ids, int16(2))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestKindMapperReadCommittedRegistrationRevalidationFailure checks that registration lookup failures prevent cache publication.
func TestKindMapperReadCommittedRegistrationRevalidationFailure(t *testing.T) {
	manager := testKindManager(t)
	known, private := graph.StringKind("Known"), graph.StringKind("Private")
	require.True(t, manager.kindCache.merge(map[graph.Kind]int16{known: 1}, 0))
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	tx := &transaction{
		ctx:           context.Background(),
		schemaManager: manager,
		tx:            mock,
		isolation:     pgx.ReadCommitted,
	}
	mapper := &transactionKindMapper{
		tx:       tx,
		snapshot: manager.kindCache.current.Load(),
	}
	tx.kindMapper = mapper
	mapper.register(private, 2)
	mapper.hasUncommittedKinds = true
	before := mapper.snapshot
	require.True(t, manager.kindCache.replace(before, manager.kindCache.requested.Load(), map[graph.Kind]int16{known: 3}, false, func() {}))
	failure := errors.New("registration lookup failed")
	mock.ExpectQuery(`select id, name from kind where name = ANY`).WithArgs(pgx.NamedArgs{"names": []string{"Private"}}).WillReturnError(failure)
	require.ErrorIs(t, tx.serviceKinds(), failure)
	require.Same(t, before, mapper.snapshot, "a failed validation cannot adopt the newer shared catalog")
	require.EqualValues(t, 2, mapper.registeredKinds[private])
	mapper.publish()
	require.NotContains(t, manager.kindCache.current.Load().kinds, private)
	require.True(t, manager.kindCache.pending())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestKindMapperReadCommittedDiscardsRemovedLookup(t *testing.T) {
	manager := testKindManager(t)
	known, removed := graph.StringKind("Known"), graph.StringKind("Removed")
	require.True(t, manager.kindCache.merge(map[graph.Kind]int16{known: 1}, 0))
	mapper := &transactionKindMapper{
		tx:       &transaction{schemaManager: manager, tx: benchmarkKindTransaction{}, isolation: pgx.ReadCommitted},
		snapshot: manager.kindCache.current.Load(),
	}
	mapper.learn(removed, 2)
	require.True(t, manager.kindCache.replace(manager.kindCache.current.Load(), manager.kindCache.requested.Load(), map[graph.Kind]int16{known: 3}, false, func() {}))
	require.NoError(t, mapper.adopt())
	_, ok := mapper.kindID(removed)
	require.False(t, ok, "a removed private lookup must be resolved again against the current database snapshot")
	mapper.publish()
	require.NotContains(t, manager.kindCache.current.Load().kinds, removed)
}

// TestKindMapperStableSnapshotDoesNotUseNewEpochFallback checks that stable snapshots cannot adopt kinds from a newer epoch.
func TestKindMapperStableSnapshotDoesNotUseNewEpochFallback(t *testing.T) {
	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		t.Run(string(isolation), func(t *testing.T) {
			manager := testKindManager(t)
			original, replacement, added := graph.StringKind("Original"), graph.StringKind("Replacement"), graph.StringKind("Added")
			require.True(t, manager.kindCache.merge(map[graph.Kind]int16{original: 1}, 0))
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)
			mapper := &transactionKindMapper{
				tx: &transaction{
					ctx:           context.Background(),
					schemaManager: manager,
					tx:            mock,
					isolation:     isolation,
				},
				snapshot: manager.kindCache.current.Load(),
			}
			mapper.tx.kindMapper = mapper
			require.True(t, manager.kindCache.replace(manager.kindCache.current.Load(), manager.kindCache.requested.Load(), map[graph.Kind]int16{replacement: 1, added: 2}, false, func() {}))

			// Fallback lookups must be fenced even before the next query boundary.
			require.Same(t, mapper.snapshot, mapper.latest())
			_, ok := mapper.kindID(replacement)
			require.False(t, ok, "a new name for an old ID cannot match the older snapshot's rows")
			_, err = mapper.MapKindID(context.Background(), 2)
			require.ErrorContains(t, err, "unable to map kind id: 2")
			mock.ExpectQuery("select id, name from kind").
				WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(int16(1), original.String()))
			require.NoError(t, mapper.adopt())
			id, ok := mapper.kindID(original)
			require.True(t, ok)
			require.EqualValues(t, 1, id)
			kind, err := mapper.MapKindID(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, original, kind)
			require.True(t, mapper.bypassTranslations())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestKindMapperPrivateCatalogNeverAdoptsSharedSnapshot(t *testing.T) {
	manager := testKindManager(t)
	shared := graph.StringKind("Shared")
	mapper := &transactionKindMapper{tx: &transaction{schemaManager: manager}, snapshot: &kindSnapshot{}, privateCatalog: true}
	before := mapper.snapshot
	require.True(t, manager.kindCache.merge(map[graph.Kind]int16{shared: 1}, 0))
	require.NoError(t, mapper.adopt())
	require.Same(t, before, mapper.snapshot)
	_, ok := mapper.kindID(shared)
	require.False(t, ok)
	manager.kindCache.fence()
	require.NoError(t, mapper.adopt())
	require.Same(t, before, mapper.snapshot)
}
