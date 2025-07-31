package changestream_test

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg/changestream"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestCheckCachedNodeChange(t *testing.T) {
	t.Run("when there is no cached change", func(t *testing.T) {
		t.Run("and changelog is enabled", func(t *testing.T) {
			d := changestream.NewTestDaemon()
			d.State.EnableDaemon()

			node := &changestream.NodeChange{
				ChangeType: changestream.ChangeTypeModified,

				NodeID:     "abc",
				Kinds:      nil,
				Properties: &graph.Properties{Map: map[string]any{"foo": "bar"}},
			}

			result, err := d.CheckCachedNodeChange(node)
			require.NoError(t, err)
			// todo: is this the intended behavior for result.Changed ?
			require.False(t, result.Changed)
			require.False(t, result.Exists)

			cached, ok := d.LastCachedChange(node.IdentityKey())
			require.True(t, ok)
			require.Equal(t, result.PropertiesHash, cached.PropertiesHash)
		})

		t.Run("and changelog is disabled", func(t *testing.T) {
			d := changestream.NewTestDaemon()
			d.State.DisableDaemon()

			node := &changestream.NodeChange{
				ChangeType: changestream.ChangeTypeModified,

				NodeID:     "abc",
				Kinds:      nil,
				Properties: &graph.Properties{Map: map[string]any{"foo": "bar"}},
			}

			result, err := d.CheckCachedNodeChange(node)
			require.NoError(t, err)
			require.True(t, result.Changed)
			require.False(t, result.Exists)
		})
	})

	t.Run("when there is a cached change", func(t *testing.T) {
		t.Run("and the properties hash matches", func(t *testing.T) {
			d := changestream.NewTestDaemon()
			props := &graph.Properties{Map: map[string]any{"x": "y"}}
			hash, _ := props.Hash(nil)

			key := "foo"
			d.PutCachedChange(key, changestream.ChangeStatus{
				PropertiesHash: hash,
			})

			node := &changestream.NodeChange{
				NodeID:     key,
				Properties: props,
			}

			result, err := d.CheckCachedNodeChange(node)
			require.NoError(t, err)
			require.False(t, result.Changed)
			require.True(t, result.Exists)
		})

		t.Run("and the properties hash is different", func(t *testing.T) {
			d := changestream.NewTestDaemon()
			key := "bar"

			d.PutCachedChange(key, changestream.ChangeStatus{
				PropertiesHash: []byte("previous-hash"),
			})

			node := &changestream.NodeChange{
				NodeID: key,
				Properties: &graph.Properties{Map: map[string]any{
					"changed": 123,
				}},
			}

			result, err := d.CheckCachedNodeChange(node)
			require.NoError(t, err)
			require.True(t, result.Changed)
			require.True(t, result.Exists)
		})
	})
}
