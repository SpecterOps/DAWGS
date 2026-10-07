package overview

import (
	"bytes"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestAccumulatorSummaryAggregatesClustersAndRelationships(t *testing.T) {
	accumulator := NewAccumulator()
	accumulator.AddNode(1, graph.StringsToKinds([]string{"User", "Base"}))
	accumulator.AddNode(2, graph.StringsToKinds([]string{"Base", "User", "User"}))
	accumulator.AddNode(3, graph.StringsToKinds([]string{"Group"}))

	require.NoError(t, accumulator.AddRelationship(1, 3, graph.StringKind("MemberOf")))
	require.NoError(t, accumulator.AddRelationship(2, 3, graph.StringKind("MemberOf")))
	require.NoError(t, accumulator.AddRelationship(1, 3, graph.StringKind("AdminTo")))
	require.NoError(t, accumulator.AddRelationship(3, 1, graph.StringKind("Contains")))

	summary := accumulator.Summary()
	require.Equal(t, int64(3), summary.NodeCount)
	require.Equal(t, int64(4), summary.EdgeCount)
	require.Equal(t, []Cluster{
		{ID: "cluster-1", Label: "Base + User", Count: 2, Size: 88},
		{ID: "cluster-2", Label: "Group", Count: 1, Size: 38},
	}, summary.Nodes)
	require.Equal(t, []Connection{
		{
			ID: "edge-1", Source: "cluster-1", Target: "cluster-2", Count: 3, Width: 12,
			Kinds: []KindCount{{Kind: "AdminTo", Count: 1}, {Kind: "MemberOf", Count: 2}},
		},
		{
			ID: "edge-2", Source: "cluster-2", Target: "cluster-1", Count: 1, Width: 2,
			Kinds: []KindCount{{Kind: "Contains", Count: 1}},
		},
	}, summary.Edges)
}

func TestAccumulatorRejectsRelationshipWithMissingEndpoint(t *testing.T) {
	accumulator := NewAccumulator()
	accumulator.AddNode(1, graph.StringsToKinds([]string{"User"}))

	require.EqualError(t, accumulator.AddRelationship(1, 2, graph.StringKind("MemberOf")), "relationship end node 2 was not included in the overview")
}

func TestWriteHTMLEscapesOverviewData(t *testing.T) {
	var output bytes.Buffer
	summary := Summary{
		Nodes: []Cluster{{ID: "cluster-1", Label: "</script><script>alert(1)</script>", Count: 1, Size: 38}},
	}

	require.NoError(t, WriteHTML(&output, summary))
	require.Contains(t, output.String(), "https://unpkg.com/cytoscape@3.33.1/dist/cytoscape.min.js")
	require.Contains(t, output.String(), `"label":"\u003c/script\u003e\u003cscript\u003ealert(1)\u003c/script\u003e"`)
	require.NotContains(t, output.String(), "</script><script>alert(1)</script>")
}
