package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestDualBoundTraversalUsesExactPairJoinWithoutOptimizerMarker verifies fixed
// traversal correctness does not depend on ExpandInto analysis being exhaustive.
func TestDualBoundTraversalUsesExactPairJoinWithoutOptimizerMarker(t *testing.T) {
	previousFrame := &Frame{Binding: &BoundIdentifier{Identifier: "s0"}}
	currentFrame := &Frame{
		Previous: previousFrame,
		Binding:  &BoundIdentifier{Identifier: "s1"},
	}
	left := &BoundIdentifier{Identifier: "n0"}
	right := &BoundIdentifier{Identifier: "n1"}
	edge := &BoundIdentifier{Identifier: "e0"}
	step := &TraversalStep{
		Frame:           currentFrame,
		Direction:       graph.DirectionOutbound,
		LeftNode:        left,
		LeftNodeBound:   true,
		Edge:            edge,
		EdgeConstraints: &Constraint{},
		EdgeJoinCondition: pgsql.NewBinaryExpression(
			boundEndpointIDReference(previousFrame, left),
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{edge.Identifier, pgsql.ColumnStartID},
		),
		RightNode:      right,
		RightNodeBound: true,
		RightNodeJoinCondition: pgsql.NewBinaryExpression(
			boundEndpointIDReference(previousFrame, right),
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{edge.Identifier, pgsql.ColumnEndID},
		),
	}

	translator := &Translator{query: &Query{Parts: []*QueryPart{{}}}}
	query, err := translator.buildTraversalPatternRoot(currentFrame, step)
	require.NoError(t, err)

	selectBody, ok := query.Body.(pgsql.Select)
	require.True(t, ok)
	require.Len(t, selectBody.From, 1)
	require.Len(t, selectBody.From[0].Joins, 1, "dual-bound fallback must not add an uncorrelated terminal-node join")
	edgeTable, ok := selectBody.From[0].Joins[0].Table.(pgsql.TableReference)
	require.True(t, ok)
	require.Equal(t, pgsql.CompoundIdentifier{pgsql.TableEdge}, edgeTable.Name)
}
