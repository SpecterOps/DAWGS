package graph_test

import (
	"slices"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_StripAllPropertiesExcept(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func() *graph.Node
		except     []string
		assertions func(t *testing.T, node *graph.Node)
	}{
		{
			name: "keeps only specified properties that exist",
			setup: func() *graph.Node {
				node := graph.NewNode(graph.ID(1), graph.NewProperties())
				node.Properties.Set("keep1", "value1")
				node.Properties.Set("keep2", "value2")
				node.Properties.Set("remove", "value3")
				return node
			},
			except: []string{"keep1", "keep2"},
			assertions: func(t *testing.T, node *graph.Node) {
				assert.True(t, node.Properties.Exists("keep1"))
				assert.Equal(t, "value1", node.Properties.Get("keep1").Any())
				assert.True(t, node.Properties.Exists("keep2"))
				assert.Equal(t, "value2", node.Properties.Get("keep2").Any())
				assert.False(t, node.Properties.Exists("remove"))
			},
		},
		{
			name: "non-existent except keys are silently skipped",
			setup: func() *graph.Node {
				node := graph.NewNode(graph.ID(1), graph.NewProperties())
				node.Properties.Set("keep", "value")
				return node
			},
			except: []string{"keep", "nonexistent"},
			assertions: func(t *testing.T, node *graph.Node) {
				assert.True(t, node.Properties.Exists("keep"))
				assert.Equal(t, 1, node.Properties.Len())
			},
		},
		{
			name: "no args clears all properties",
			setup: func() *graph.Node {
				node := graph.NewNode(graph.ID(1), graph.NewProperties())
				node.Properties.Set("key", "value")
				return node
			},
			except: nil,
			assertions: func(t *testing.T, node *graph.Node) {
				assert.Equal(t, 0, node.Properties.Len())
			},
		},
		{
			name: "respects deleted properties",
			setup: func() *graph.Node {
				node := graph.NewNode(graph.ID(1), graph.NewProperties())
				node.Properties.Delete("key")
				return node
			},
			except: []string{"key"},
			assertions: func(t *testing.T, node *graph.Node) {
				deleted := node.Properties.DeletedProperties()
				assert.Equal(t, 1, len(deleted))
				assert.True(t, slices.Contains(deleted, "key"))
			},
		},
		{
			name: "respects deleted properties 2",
			setup: func() *graph.Node {
				node := graph.NewNode(graph.ID(1), graph.NewProperties())
				node.Properties.Set("hello", "value1")
				node.Properties.Set("loremipsum", "value2")
				node.Properties.Delete("key1")
				node.Properties.Delete("key2")
				node.Properties.Delete("key3")
				return node
			},
			except: []string{"hello", "world", "key1", "key2"},
			assertions: func(t *testing.T, node *graph.Node) {
				deleted := node.Properties.DeletedProperties()
				assert.Equal(t, 2, len(deleted))
				assert.True(t, slices.Contains(deleted, "key1"))
				assert.True(t, slices.Contains(deleted, "key2"))
				assert.False(t, node.Properties.Exists("loremipsum"))
				assert.True(t, node.Properties.Exists("hello"))
				assert.Equal(t, "value1", node.Properties.Get("hello").Any())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := tc.setup()
			node.StripAllPropertiesExcept(tc.except...)
			tc.assertions(t, node)
		})
	}
}

func Test_NodeSizeOf(t *testing.T) {
	node := graph.Node{ID: graph.ID(1)}
	oldSize := int64(node.SizeOf())

	// ensure that reassignment of the Kinds field affects the size
	node.Kinds = append(node.Kinds, permissionKind, userKind, groupKind)
	newSize := int64(node.SizeOf())
	require.Greater(t, newSize, oldSize)

	// ensure that reassignment of the AddedKinds field affects the size
	oldSize = newSize
	node.AddedKinds = append(node.AddedKinds, permissionKind)
	newSize = int64(node.SizeOf())
	require.Greater(t, newSize, oldSize)

	// ensure that reassignment of the DeletedKinds field affects the size
	oldSize = newSize
	node.DeletedKinds = append(node.DeletedKinds, userKind)
	newSize = int64(node.SizeOf())
	require.Greater(t, newSize, oldSize)

	// ensure that reassignment of the Properties field affects the size
	oldSize = newSize
	node.Properties = graph.NewProperties()
	newSize = int64(node.SizeOf())
	require.Greater(t, newSize, oldSize)
}
