package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestTransactionKindMapperOwnership(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	manager := NewSchemaManager(nil, 0)
	t.Cleanup(func() { manager.parseCache.Close(); manager.compilationCache.Close() })
	tx := &transaction{ctx: ctx, tx: mock, schemaManager: manager}
	mapper := &transactionKindMapper{tx: tx}
	tx.kindMapper = mapper
	known, fresh := graph.StringKind("Known"), graph.StringKind("Fresh")
	mock.ExpectQuery("select id, name").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(int16(1), "Known"))
	ids, err := mapper.MapKinds(ctx, graph.Kinds{known, known})
	require.NoError(t, err)
	require.Equal(t, []int16{1, 1}, ids)
	id, err := mapper.MapKind(ctx, known)
	require.NoError(t, err)
	require.EqualValues(t, 1, id)
	kinds, err := mapper.MapKindIDs(ctx, []int16{1, 1})
	require.NoError(t, err)
	require.Equal(t, graph.Kinds{known, known}, kinds)
	kind, err := mapper.MapKindID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, known, kind)
	_, err = mapper.MapKindID(ctx, 3)
	require.ErrorContains(t, err, "unable to map kind id")
	mock.ExpectQuery("select id, name").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(int16(1), "Known"))
	_, err = mapper.MapKind(ctx, fresh)
	require.ErrorContains(t, err, "unable to map kinds")
	mock.ExpectQuery("with").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int16(2)))
	ids, err = mapper.AssertKinds(ctx, graph.Kinds{fresh, known, fresh})
	require.NoError(t, err)
	require.Equal(t, []int16{2, 1, 2}, ids)
	require.True(t, mapper.hasUncommittedKinds)
	require.Empty(t, manager.GetKindIDsByKind(), "uncommitted IDs stay private")
	mapper.publish()
	require.Equal(t, map[int16]graph.Kind{1: known, 2: fresh}, manager.GetKindIDsByKind())
	snapshot := manager.GetKindIDsByKind()
	delete(snapshot, 1)
	require.Len(t, manager.GetKindIDsByKind(), 2)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionKindMapperErrors(t *testing.T) {
	for _, op := range []string{"map", "assert", "ids", "retry"} {
		t.Run(op, func(t *testing.T) {
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)
			tx := &transaction{ctx: context.Background(), tx: mock}
			mapper := &transactionKindMapper{tx: tx}
			tx.kindMapper = mapper
			failure := errors.New("metadata failure")
			if op == "retry" {
				mapper.kinds = map[graph.Kind]int16{}
				mapper.ids = map[int16]graph.Kind{}
				mock.ExpectQuery("with").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"id"}))
				mock.ExpectQuery("with").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(failure)
			} else {
				mock.ExpectQuery("select id, name").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(failure)
			}
			switch op {
			case "map":
				_, err = mapper.MapKinds(tx.ctx, graph.Kinds{graph.StringKind("Missing")})
			case "ids":
				_, err = mapper.MapKindIDs(tx.ctx, []int16{1})
			default:
				_, err = mapper.AssertKinds(tx.ctx, graph.Kinds{graph.StringKind("Missing")})
			}
			require.ErrorIs(t, err, failure)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

}

func TestRawKindMappingUsesOwnedSnapshotWithoutNestedQuery(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	manager := NewSchemaManager(nil, 0)
	t.Cleanup(func() { manager.parseCache.Close(); manager.compilationCache.Close() })
	known := graph.StringKind("Known")
	manager.kindIDsByKind[7] = known
	tx, err := newTransactionWrapper(context.Background(), nil, manager, &Config{}, false)
	require.NoError(t, err)
	tx.tx = mock
	mock.ExpectQuery("select kinds").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"kinds"}).AddRow([]int16{7}))
	// Schema reconciliation owns the metadata lock while reading raw rows.
	// The wrapper must already own its snapshot before this lock is acquired.
	manager.lock.Lock()
	defer manager.lock.Unlock()
	ready := make(chan graph.Result, 1)
	go func() { ready <- tx.Raw("select kinds", nil) }()
	var result graph.Result
	select {
	case result = <-ready:
	case <-time.After(time.Second):
		t.Fatal("raw mapping attempted to reacquire the metadata lock")
	}
	defer result.Close()
	require.True(t, result.Next())
	var kinds graph.Kinds
	require.NoError(t, result.Scan(&kinds))
	require.Equal(t, graph.Kinds{known}, kinds)
	require.False(t, result.Next())
	require.NoError(t, result.Error())
	require.NoError(t, mock.ExpectationsWereMet())
}
