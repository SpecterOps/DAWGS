package graph_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/specterops/dawgs/util/test"
	"github.com/stretchr/testify/require"
)

var (
	groupKind      = graph.StringKind("group")
	domainKind     = graph.StringKind("domain")
	userKind       = graph.StringKind("user")
	computerKind   = graph.StringKind("computer")
	permissionKind = graph.StringKind("permission")
	membershipKind = graph.StringKind("member")
)

func TestPathSegment_Root(t *testing.T) {
	var (
		domainNode   = test.Node(domainKind)
		groupNode    = test.Node(groupKind)
		userNode     = test.Node(userKind)
		computerNode = test.Node(computerKind)

		expectedStartNodes         = []*graph.Node{domainNode, groupNode, userNode}
		expectedStartNodesReversed = []*graph.Node{userNode, groupNode, domainNode}

		domainSegment   = graph.NewRootPathSegment(domainNode)
		groupSegment    = domainSegment.Descend(groupNode, test.Edge(groupNode, domainNode, permissionKind))
		userSegment     = groupSegment.Descend(userNode, test.Edge(userNode, groupNode, membershipKind))
		computerSegment = userSegment.Descend(computerNode, test.Edge(computerNode, userNode, permissionKind))
		inst            = computerSegment.Path()
	)

	require.Equal(t, domainNode, inst.Root())
	require.Equal(t, computerNode, inst.Terminal())

	walkIdx := 0
	inst.Walk(func(start, end *graph.Node, relationship *graph.Relationship) bool {
		require.Equal(t, expectedStartNodes[walkIdx].ID, start.ID)
		walkIdx++

		return true
	})

	walkIdx = 0
	inst.WalkReverse(func(start, end *graph.Node, relationship *graph.Relationship) bool {
		require.Equal(t, expectedStartNodesReversed[walkIdx].ID, start.ID)
		walkIdx++

		return true
	})

	seen := false
	inst.Walk(func(start, end *graph.Node, relationship *graph.Relationship) bool {
		if seen {
			t.Fatal("Expected to be called only once.")
		} else {
			seen = true
		}

		return false
	})

	seen = false
	inst.WalkReverse(func(start, end *graph.Node, relationship *graph.Relationship) bool {
		if seen {
			t.Fatal("Expected to be called only once.")
		} else {
			seen = true
		}

		return false
	})
}

func TestPathSegment_SizeOf(t *testing.T) {
	var (
		domainNode   = test.Node(domainKind)
		groupNode    = test.Node(groupKind)
		userNode     = test.Node(userKind)
		computerNode = test.Node(computerKind)

		domainSegment = graph.NewRootPathSegment(domainNode)
		originalSize  = int64(domainSegment.SizeOf())
		treeSize      = originalSize
	)

	// Group segment
	groupSegment := domainSegment.Descend(groupNode, test.Edge(groupNode, domainNode, permissionKind))

	// Add the size of the path edge but also ensure that the capacity increase for storing branches is also tracked
	groupSegmentSize := int64(groupSegment.SizeOf())

	treeSize += groupSegmentSize
	treeSize += int64(size.Of(domainSegment.Branches))

	require.Equal(t, treeSize, int64(domainSegment.SizeOf()))

	// User segment
	userSegment := groupSegment.Descend(userNode, test.Edge(userNode, groupNode, membershipKind))

	// Add the size of the path edge but also ensure that the capacity increase for storing branches is also tracked
	userSegmentSize := int64(userSegment.SizeOf())

	treeSize += userSegmentSize
	treeSize += int64(size.Of(groupSegment.Branches))

	require.Equal(t, treeSize, int64(domainSegment.SizeOf()))

	// Computer segment
	computerSegment := userSegment.Descend(computerNode, test.Edge(computerNode, userNode, permissionKind))

	// Add the size of the path edge but also ensure that the capacity increase for storing branches is also tracked
	computerSegmentSize := int64(computerSegment.SizeOf())

	treeSize += computerSegmentSize
	treeSize += int64(size.Of(userSegment.Branches))

	require.Equal(t, treeSize, int64(domainSegment.SizeOf()))

	// Test detaching from the path tree
	computerSegment.Detach()
	treeSize -= computerSegmentSize

	require.Equal(t, treeSize, int64(domainSegment.SizeOf()))

	userSegment.Detach()

	treeSize -= userSegmentSize
	treeSize -= int64(size.Of(userSegment.Branches))

	require.Equal(t, treeSize, int64(domainSegment.SizeOf()))

	groupSegment.Detach()

	treeSize -= groupSegmentSize
	treeSize -= int64(size.Of(groupSegment.Branches))

	require.Equal(t, treeSize, int64(domainSegment.SizeOf()))

	// The original size should have one slice allocation
	originalSize += int64(size.Of(domainSegment.Branches))
	require.Equal(t, treeSize, originalSize)
}
