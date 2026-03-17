package container

import (
	"math/rand"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

func TestAddNode(t *testing.T) {
	ts := NewTriplestore()

	node := uint64(10)
	ts.AddNode(node)
	assert.Equal(t, uint64(1), ts.NumNodes())
}

func TestAddEdge(t *testing.T) {
	ts := NewTriplestore()

	edge := Edge{
		ID:    1,
		Kind:  graph.StringKind("12345"),
		Start: 10,
		End:   20,
	}

	ts.AddEdge(edge)
	assert.Equal(t, uint64(1), ts.NumEdges())
}

func TestEachEdge(t *testing.T) {
	ts := NewTriplestore()

	edges := []Edge{
		{ID: 1, Start: 10, End: 20},
		{ID: 2, Start: 20, End: 30},
	}

	for _, e := range edges {
		ts.AddEdge(e)
	}

	var collected []Edge
	ts.EachEdge(func(e Edge) bool {
		collected = append(collected, e)
		return true
	})

	assert.Equal(t, edges, collected)
}

func TestAdjacentEdges(t *testing.T) {
	ts := NewTriplestore()

	edges := []Edge{
		{ID: 1, Start: 10, End: 20},
		{ID: 2, Start: 20, End: 30},
		{ID: 3, Start: 10, End: 30},
	}

	for _, e := range edges {
		ts.AddEdge(e)
	}

	dir := graph.DirectionOutbound
	adjacent := AdjacentEdges(ts, 10, dir)
	assert.Len(t, adjacent, 2)

	dir = graph.DirectionInbound
	adjacent = AdjacentEdges(ts, 30, dir)
	assert.Len(t, adjacent, 2)
}

func TestSort(t *testing.T) {
	ts := NewTriplestore()

	edges := []Edge{
		{ID: 3, Start: 10, End: 20},
		{ID: 1, Start: 20, End: 30},
		{ID: 2, Start: 10, End: 30},
	}

	for _, e := range edges {
		ts.AddEdge(e)
	}

	ts.Sort()

	expected := []Edge{
		{ID: 1, Start: 20, End: 30},
		{ID: 2, Start: 10, End: 30},
		{ID: 3, Start: 10, End: 20},
	}

	var collected []Edge
	ts.EachEdge(func(e Edge) bool {
		collected = append(collected, e)
		return true
	})

	assert.Equal(t, expected, collected)
}

func TestDegrees(t *testing.T) {
	ts := NewTriplestore()

	edges := []Edge{
		{ID: 1, Start: 10, End: 20},
		{ID: 2, Start: 20, End: 30},
		{ID: 3, Start: 10, End: 30},
	}

	for _, e := range edges {
		ts.AddEdge(e)
	}

	degOut := Degrees(ts, 10, graph.DirectionOutbound)
	assert.Equal(t, uint64(2), degOut)

	degIn := Degrees(ts, 30, graph.DirectionInbound)
	assert.Equal(t, uint64(2), degIn)
}

func TestEachAdjacentNode(t *testing.T) {
	ts := NewTriplestore()

	edges := []Edge{
		{ID: 1, Start: 10, End: 20},
		{ID: 2, Start: 20, End: 30},
		{ID: 3, Start: 10, End: 30},
	}

	for _, e := range edges {
		ts.AddEdge(e)
	}

	var collected []uint64
	ts.EachAdjacentNode(10, graph.DirectionOutbound, func(n uint64) bool {
		collected = append(collected, n)
		return true
	})

	assert.ElementsMatch(t, []uint64{20, 30}, collected)
}

func TestNumNodes(t *testing.T) {
	ts := NewTriplestore()

	ts.AddNode(10)
	ts.AddNode(20)
	ts.AddNode(30)

	assert.Equal(t, uint64(3), ts.NumNodes())
}

func TestAddMultipleEdges(t *testing.T) {
	ts := NewTriplestore()

	edges := make([]Edge, 100)
	for i := range edges {
		edges[i] = Edge{
			ID:    uint64(i),
			Start: uint64(rand.Intn(100)),
			End:   uint64(rand.Intn(100)),
		}
	}

	for _, e := range edges {
		ts.AddEdge(e)
	}

	assert.Equal(t, uint64(len(edges)), ts.NumEdges())
}

func TestEachAdjacentEdge(t *testing.T) {
	ts := NewTriplestore()

	edges := []Edge{
		{ID: 1, Start: 10, End: 20},
		{ID: 2, Start: 20, End: 30},
		{ID: 3, Start: 10, End: 30},
	}

	for _, e := range edges {
		ts.AddEdge(e)
	}

	var collected []Edge
	ts.EachAdjacentEdge(10, graph.DirectionOutbound, func(e Edge) bool {
		collected = append(collected, e)
		return true
	})

	expected := []Edge{
		{ID: 1, Start: 10, End: 20},
		{ID: 3, Start: 10, End: 30},
	}

	assert.Equal(t, expected, collected)
}
