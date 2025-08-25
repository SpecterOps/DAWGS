package changelog

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

func TestFlushNodeChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)

	db, err := newLogDB(ctx, mockDB)
	require.NoError(t, err)

	mockDB.ExpectExec(updateNodesLastSeenSQL).
		WithArgs(pgxmock.AnyArg(), []string{"123", "456"}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	rowsAffected, err := db.flushNodeChanges(ctx, []*NodeChange{{NodeID: "123"}, {NodeID: "456"}})
	require.NoError(t, err)
	require.Equal(t, int64(2), rowsAffected)
}
