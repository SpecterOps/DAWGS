package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortNodeSetById(t *testing.T) {
	nodeSet := NewNodeSet()

	nodeA := NewNode(11, nil, StringKind("a"))
	nodeB := NewNode(2, nil, StringKind("b"))
	nodeC := NewNode(43, nil, StringKind("c"))

	nodeSet.Add(nodeA)
	nodeSet.Add(nodeB)
	nodeSet.Add(nodeC)

	nodes := SortNodeSetById(nodeSet)

	require.Equal(t, "2", nodes[0].ID.String())
	require.Equal(t, "11", nodes[1].ID.String())
	require.Equal(t, "43", nodes[2].ID.String())
}
