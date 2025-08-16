package changestream

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/specterops/dawgs/drivers/pg/mocks"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDiffProps(t *testing.T) {
	oldProps := graph.NewProperties().SetAll(map[string]any{"a": 1})
	newProps := graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2})

	diff := diffProps(oldProps, newProps)
	require.Equal(t, 1, diff.Len())
	bProp, err := diff.Get("b").Int()
	require.NoError(t, err)
	require.Equal(t, 2, bProp)
	require.Empty(t, diff.DeletedProperties())

	oldProps = graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2})
	newProps = graph.NewProperties().SetAll(map[string]any{"a": 1})

	diff = diffProps(oldProps, newProps)
	require.Equal(t, 0, diff.Len())
	require.Len(t, diff.DeletedProperties(), 1)
	require.Contains(t, diff.Deleted, "b")

}

// this column def elides the hash because replay doesn't consider that field
var nodeChangeStreamColumns = []string{
	"change_type", "node_id", "kind_ids", "modified_properties", "deleted_properties",
}
var fooKind = graph.StringKind("foo")
var barKind = graph.StringKind("bar")

func TestReplayNodeChange(t *testing.T) {
	t.Run("replay single node", func(t *testing.T) {
		dbMock, changelog, kindMapperMock := newChangelogWithMockDB(t)
		defer dbMock.Close()

		expectedKindIDs := []int16{1, 2}
		kindMapperMock.
			EXPECT().
			MapKindIDs(gomock.Any(), gomock.Eq(expectedKindIDs)).
			Return(graph.Kinds{fooKind, barKind}, nil)

		rows := pgxmock.NewRows(nodeChangeStreamColumns).AddRow(
			ChangeTypeModified,
			"node123",
			expectedKindIDs,
			map[string]any{"foo": "bar"},
			[]string{"baz"},
		)

		sinceID := int64(10)
		dbMock.ExpectQuery(SELECT_NODE_CHANGE_RANGE_SQL).
			WithArgs(sinceID).
			WillReturnRows(rows)

		var got []NodeChange
		err := changelog.replayNodeChanges(context.Background(), sinceID,
			func(c NodeChange) {
				got = append(got, c)
			},
		)
		require.NoError(t, err)
		require.Len(t, got, 1)

		change := got[0]
		require.Equal(t, "node123", change.NodeID)
		require.Contains(t, change.Kinds, fooKind)
		require.Contains(t, change.Kinds, barKind)
		require.Equal(t, "bar", change.Properties.Map["foo"])
		require.Contains(t, change.Properties.Deleted, "baz")

	})
	t.Run("empty result", func(t *testing.T) {
		mockDB, changelog, _ := newChangelogWithMockDB(t)
		defer mockDB.Close()

		mockDB.ExpectQuery(SELECT_NODE_CHANGE_RANGE_SQL).
			WithArgs(int64(99)).
			WillReturnRows(pgxmock.NewRows(nodeChangeStreamColumns))

		var called bool
		err := changelog.replayNodeChanges(context.Background(), 99,
			func(c NodeChange) {
				called = true
			})

		require.NoError(t, err)
		require.False(t, called, "visitor should not be called on empty result set")
	})
	t.Run("scan error ", func(t *testing.T) {
		dbMock, changelog, _ := newChangelogWithMockDB(t)
		defer dbMock.Close()

		rows := pgxmock.NewRows(nodeChangeStreamColumns).AddRow(
			ChangeTypeModified,
			12345, // number instead of a string. simulates a scan failure
			[]int{1, 2},
			map[string]any{"foo": "bar"},
			[]string{"baz"},
		)

		sinceID := int64(1)
		dbMock.ExpectQuery(SELECT_NODE_CHANGE_RANGE_SQL).
			WithArgs(sinceID).
			WillReturnRows(rows)

		err := changelog.replayNodeChanges(context.Background(), sinceID, func(c NodeChange) {})
		require.Error(t, err, "expecting replay() to error out")
	})
	t.Run("kind mapper error ", func(t *testing.T) {})

}

func newChangelogWithMockDB(t *testing.T) (pgxmock.PgxPoolIface, *Changelog, *mocks.MockKindMapper) {
	// create the pgxmock with the option of exact string matching. this is chill since we define all of
	// our sql stmts as constants in sql.go
	mockDB, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	mockKindMapper := mocks.NewMockKindMapper(ctrl)

	changelog := &Changelog{
		DB: db{
			Conn:       mockDB,
			kindMapper: mockKindMapper,
		},
	}

	return mockDB, changelog, mockKindMapper
}
