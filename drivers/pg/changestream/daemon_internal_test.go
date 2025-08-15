package changestream

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestChangeCache(t *testing.T) {
	t.Run("proposed change not in cache. return false.", func(t *testing.T) {
		c := newChangeCache()

		node := &NodeChange{
			NodeID:     "123",
			Kinds:      nil,
			Properties: &graph.Properties{Map: map[string]any{"foo": "bar"}},
		}

		handled, err := c.checkCache(node)
		require.NoError(t, err)
		require.False(t, handled)

	})

	t.Run("proposed change is in cache. properties have no diff.", func(t *testing.T) {
		var (
			c      = newChangeCache()
			change = &NodeChange{
				NodeID:     "123",
				Kinds:      nil,
				Properties: &graph.Properties{Map: map[string]any{"a": 1}},
			}
			key = change.IdentityKey()
		)

		// simulate a full cache
		c.put(key, change)

		handled, err := c.checkCache(change)
		require.NoError(t, err)
		require.True(t, handled)
	})

	t.Run("proposed change is in cache. properties have a diff.", func(t *testing.T) {
		var (
			c         = newChangeCache()
			oldChange = &NodeChange{
				NodeID:     "123",
				Kinds:      nil,
				Properties: &graph.Properties{Map: map[string]any{"a": 1}},
			}
			key = oldChange.IdentityKey()
		)

		// simulate a full cache
		c.put(key, oldChange)

		newChange := &NodeChange{
			NodeID:     "123",
			Kinds:      nil,
			Properties: &graph.Properties{Map: map[string]any{"changed": 1}},
		}

		handled, err := c.checkCache(newChange)
		require.NoError(t, err)
		require.True(t, handled)
		require.Contains(t, newChange.ModifiedProperties, "changed")
		require.Contains(t, newChange.Deleted, "a")
	})

}

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
