package changelog

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/specterops/dawgs/drivers/pg/mocks"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestFlushNodeChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	ctrl := gomock.NewController(t)
	mockKindMapper := mocks.NewMockKindMapper(ctrl)
	t.Cleanup(ctrl.Finish)

	db := newDB(mockDB, mockKindMapper)

	mockDB.ExpectExec(updateNodesLastSeenSQL).
		WithArgs(pgxmock.AnyArg(), []string{"123", "456"}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	rowsAffected, err := db.flushNodeChanges(ctx, []NodeChange{
		{NodeID: "123", Properties: graph.NewProperties()},
		{NodeID: "456", Properties: graph.NewProperties()}})

	require.NoError(t, err)
	require.Equal(t, int64(2), rowsAffected)
	require.NoError(t, mockDB.ExpectationsWereMet())
}

func TestFlushEdgeChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	ctrl := gomock.NewController(t)
	mockKindMapper := mocks.NewMockKindMapper(ctrl)
	t.Cleanup(ctrl.Finish)

	db := newDB(mockDB, mockKindMapper)

	rawKinds := []graph.Kind{graph.StringKind("a"), graph.StringKind("b")}
	mappedKinds := []int16{1, 2}
	mockKindMapper.EXPECT().
		MapKinds(gomock.Any(), rawKinds).
		Return(mappedKinds, nil)

	mockDB.ExpectExec(updateEdgesLastSeenSQL).
		WithArgs(pgxmock.AnyArg(), []string{"12", "56"}, []string{"34", "78"}, mappedKinds).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	rowsAffected, err := db.flushEdgeChanges(ctx, []EdgeChange{
		{SourceNodeID: "12", TargetNodeID: "34", Kind: graph.StringKind("a"), Properties: graph.NewProperties()},
		{SourceNodeID: "56", TargetNodeID: "78", Kind: graph.StringKind("b"), Properties: graph.NewProperties()},
	})

	require.NoError(t, err)
	require.Equal(t, int64(2), rowsAffected)
	require.NoError(t, mockDB.ExpectationsWereMet())
}
