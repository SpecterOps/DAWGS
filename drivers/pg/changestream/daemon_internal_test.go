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
	oldProps := map[string]any{"a": 1}
	newProps := map[string]any{"a": 1, "b": 2}

	modified, deleted := diffProps(oldProps, newProps)
	require.Len(t, modified, 1)
	require.Len(t, deleted, 0)
}

func TestMergeNodeChanges(t *testing.T) {
	a1 := NodeChange{
		NodeID:             "abc",
		changeType:         ChangeTypeAdded,
		Properties:         graph.NewProperties().Set("a", 1),
		ModifiedProperties: graph.NewProperties().Set("a", 1).MapOrEmpty(),
		Deleted:            nil,
		Kinds:              graph.Kinds{graph.StringKind("kindA")},
	}

	a2b3 := NodeChange{
		NodeID:             "abc",
		changeType:         ChangeTypeModified,
		Properties:         graph.NewProperties().SetAll(map[string]any{"a": 2, "b": 3}),
		ModifiedProperties: graph.NewProperties().SetAll(map[string]any{"a": 2, "b": 3}).MapOrEmpty(),
		Deleted:            nil,
		Kinds:              graph.Kinds{graph.StringKind("kindA")},
	}

	c1 := NodeChange{
		NodeID:             "abc",
		changeType:         ChangeTypeModified,
		Properties:         graph.NewProperties().Set("c", 1),
		ModifiedProperties: graph.NewProperties().Set("c", 1).MapOrEmpty(),
		Deleted:            []string{"a", "b"},
		Kinds:              graph.Kinds{graph.StringKind("kindA")},
	}

	kindB := NodeChange{
		NodeID:             "abc",
		changeType:         ChangeTypeModified,
		Properties:         nil,
		ModifiedProperties: nil,
		Deleted:            nil,
		Kinds:              graph.Kinds{graph.StringKind("kindB")},
	}

	changes := []*NodeChange{&a1, &a2b3, &c1, &kindB}
	merged, err := mergeNodeChanges(changes)
	require.NoError(t, err)

	require.Len(t, merged, 1)
	m := merged[0]

	require.Equal(t, m.Type(), ChangeTypeAdded)
	require.Len(t, m.Kinds, 2)
	require.Contains(t, m.Properties.MapOrEmpty(), "c")
	require.Contains(t, m.Deleted, "a")
	require.Contains(t, m.Deleted, "b")

}

func TestMergeNodeChanges2(t *testing.T) {
	a1b2c3 := NodeChange{
		NodeID:             "abc",
		changeType:         ChangeTypeAdded,
		Properties:         graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2, "c": 3}),
		ModifiedProperties: graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2, "c": 3}).MapOrEmpty(),
		Deleted:            nil,
		Kinds:              graph.Kinds{graph.StringKind("kindA")},
	}

	a2 := NodeChange{
		NodeID:             "abc",
		changeType:         ChangeTypeModified,
		Properties:         graph.NewProperties().SetAll(map[string]any{"a": 2}),
		ModifiedProperties: graph.NewProperties().SetAll(map[string]any{"a": 2}).MapOrEmpty(),
		Deleted:            []string{"b", "c"},
		Kinds:              graph.Kinds{graph.StringKind("kindA")},
	}

	d4 := NodeChange{
		NodeID:             "abc",
		changeType:         ChangeTypeModified,
		Properties:         graph.NewProperties().Set("d", 4),
		ModifiedProperties: graph.NewProperties().Set("d", 4).MapOrEmpty(),
		Deleted:            []string{"a"},
		Kinds:              graph.Kinds{graph.StringKind("kindA")},
	}

	changes := []*NodeChange{&a1b2c3, &a2, &d4}
	merged, err := mergeNodeChanges(changes)
	require.NoError(t, err)

	require.Len(t, merged, 1)
	m := merged[0]

	require.Equal(t, m.Type(), ChangeTypeAdded)
	require.Len(t, m.Kinds, 1)
	require.Len(t, m.Deleted, 0) // cancelled out
	require.Len(t, m.ModifiedProperties, 1)
	require.Contains(t, m.Properties.MapOrEmpty(), "d")

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
		err := changelog.ReplayNodeChanges(context.Background(), sinceID,
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
		err := changelog.ReplayNodeChanges(context.Background(), 99,
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

		err := changelog.ReplayNodeChanges(context.Background(), sinceID, func(c NodeChange) {})
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
