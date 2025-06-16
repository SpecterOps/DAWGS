package graph_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

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
