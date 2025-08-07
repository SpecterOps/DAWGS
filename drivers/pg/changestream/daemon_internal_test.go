package changestream

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestChangeCache(t *testing.T) {
	t.Run("proposed change not in cache. changed = true, exists = false ", func(t *testing.T) {
		c := newChangeCache()

		node := &graph.NodeChange{
			ChangeType: graph.ChangeTypeModified,

			NodeID:     "abc",
			Kinds:      nil,
			Properties: &graph.Properties{Map: map[string]any{"foo": "bar"}},
		}

		result, err := c.evaluateNodeChange(node)
		require.NoError(t, err)
		require.True(t, result.Changed)
		require.False(t, result.Exists)

		cached, ok := c.get(node.IdentityKey())
		require.True(t, ok)
		require.Equal(t, result.PropertiesHash, cached.PropertiesHash)
	})

	t.Run("proposed change is in cache. properties have no diff.", func(t *testing.T) {
		c := newChangeCache()

		props := &graph.Properties{Map: map[string]any{"x": "y"}}
		hash, _ := props.Hash(nil)

		key := "foo"
		c.put(key, graph.ChangeStatus{
			PropertiesHash: hash,
		})

		node := &graph.NodeChange{
			NodeID:     key,
			Properties: props,
		}

		result, err := c.evaluateNodeChange(node)
		require.NoError(t, err)
		require.False(t, result.Changed)
		require.True(t, result.Exists)
	})

	t.Run("proposed change is in cache. properties have a diff.", func(t *testing.T) {
		c := newChangeCache()
		key := "bar"

		c.put(key, graph.ChangeStatus{
			PropertiesHash: []byte("previous-hash"),
		})

		node := &graph.NodeChange{
			NodeID: key,
			Properties: &graph.Properties{
				Map: map[string]any{
					"changed": 123,
				},
			},
		}

		result, err := c.evaluateNodeChange(node)
		require.NoError(t, err)
		require.True(t, result.Changed)
		require.True(t, result.Exists)
	})

}
