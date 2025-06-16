package cardinality_test

import (
	"testing"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestDuplexToGraphIDs(t *testing.T) {
	uintIDs := []uint64{1, 2, 3, 4, 5}
	duplex := cardinality.NewBitmap64()
	duplex.Add(uintIDs...)

	ids := graph.DuplexToGraphIDs(duplex)

	for _, uintID := range uintIDs {
		found := false

		for _, id := range ids {
			if id.Uint64() == uintID {
				found = true
				break
			}
		}

		require.True(t, found)
	}
}

func TestNodeSetToDuplex(t *testing.T) {
	nodes := graph.NodeSet{
		1: &graph.Node{
			ID: 1,
		},
		2: &graph.Node{
			ID: 2,
		},
	}

	duplex := graph.NodeSetToDuplex(nodes)

	require.True(t, duplex.Contains(1))
	require.True(t, duplex.Contains(2))
}
