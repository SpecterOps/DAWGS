package traversal

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

func TestNewNodeCollector(t *testing.T) {
	collector := NewNodeCollector()

	assert.NotNil(t, collector)
	assert.NotNil(t, collector.Nodes)
	assert.NotNil(t, collector.lock)
	assert.Empty(t, collector.Nodes)
}

func TestNodeCollectorAdd(t *testing.T) {
	collector := NewNodeCollector()
	firstNode := graph.NewNode(1, nil, kindA)
	secondNode := graph.NewNode(2, nil, kindB)
	replacementNode := graph.NewNode(1, nil, kindB)

	collector.Add(firstNode)
	collector.Add(secondNode)
	collector.Add(replacementNode)

	assert.Equal(t, 2, collector.Nodes.Len())
	assert.Same(t, replacementNode, collector.Nodes.Get(1))
	assert.Same(t, secondNode, collector.Nodes.Get(2))
}

func TestNodeCollectorCollect(t *testing.T) {
	collector := NewNodeCollector()
	segment := graph.NewRootPathSegment(graph.NewNode(10, nil, kindA))

	collector.Collect(segment)

	assert.Equal(t, 1, collector.Nodes.Len())
	assert.Same(t, segment.Node, collector.Nodes.Get(segment.Node.ID))
}

func TestNewPathCollector(t *testing.T) {
	collector := NewPathCollector()

	assert.NotNil(t, collector)
	assert.NotNil(t, collector.lock)
	assert.Empty(t, collector.Paths)
}
