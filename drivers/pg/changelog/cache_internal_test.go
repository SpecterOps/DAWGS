package changelog

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
		require.Contains(t, newChange.Properties.Modified, "changed")
		require.Contains(t, newChange.Properties.Deleted, "a")
	})

}
