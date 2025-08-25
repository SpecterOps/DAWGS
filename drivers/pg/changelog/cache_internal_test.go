package changelog

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestChangeCache(t *testing.T) {
	t.Run("node doesn't exist in cache. return true.", func(t *testing.T) {
		c := newChangeCache()

		node := &NodeChange{
			NodeID:     "123",
			Kinds:      nil,
			Properties: &graph.Properties{Map: map[string]any{"foo": "bar"}},
		}

		shouldSubmit, err := c.checkCache(node)
		require.NoError(t, err)
		require.True(t, shouldSubmit)

	})

	t.Run("node is cached with same properties. return false", func(t *testing.T) {
		var (
			c      = newChangeCache()
			change = &NodeChange{
				NodeID:     "123",
				Kinds:      nil,
				Properties: &graph.Properties{Map: map[string]any{"a": 1}},
			}
			idHash      = change.IdentityKey()
			dataHash, _ = change.Hash()
		)

		// simulate a full cache
		c.put(idHash, dataHash)

		shouldSubmit, err := c.checkCache(change)
		require.NoError(t, err)
		require.False(t, shouldSubmit)
	})

	t.Run("node is cached. properties have a diff. return true", func(t *testing.T) {
		var (
			c         = newChangeCache()
			oldChange = &NodeChange{
				NodeID:     "123",
				Kinds:      nil,
				Properties: &graph.Properties{Map: map[string]any{"a": 1}},
			}
			idHash      = oldChange.IdentityKey()
			dataHash, _ = oldChange.Hash()
		)

		// simulate a populated cache
		c.put(idHash, dataHash)

		newChange := &NodeChange{
			NodeID:     "123",
			Kinds:      nil,
			Properties: &graph.Properties{Map: map[string]any{"changed": 1}},
		}

		shouldSubmit, err := c.checkCache(newChange)
		require.NoError(t, err)
		require.True(t, shouldSubmit)
	})

}
