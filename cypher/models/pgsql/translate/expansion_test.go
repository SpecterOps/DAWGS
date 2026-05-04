package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/stretchr/testify/require"
)

const (
	shortestPathSeedTestPreviousFrame pgsql.Identifier = "s0"
	shortestPathSeedTestFrame         pgsql.Identifier = "s1"
	shortestPathSeedTestRoot          pgsql.Identifier = "n0"
	shortestPathSeedTestTerminal      pgsql.Identifier = "n1"
	shortestPathSeedTestOther         pgsql.Identifier = "x"
	shortestPathSeedTestEdge          pgsql.Identifier = "e0"
)

func shortestPathSeedTestBoundColumn(nodeIdentifier pgsql.Identifier, column pgsql.Identifier) pgsql.RowColumnReference {
	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{shortestPathSeedTestPreviousFrame, nodeIdentifier},
		Column:     column,
	}
}

func shortestPathSeedTestLocalFunctionPredicate(nodeIdentifier pgsql.Identifier, value string) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		pgsql.FunctionCall{
			Function: pgsql.FunctionToLower,
			Parameters: []pgsql.Expression{
				shortestPathSeedTestBoundColumn(nodeIdentifier, pgsql.ColumnID),
			},
			CastType: pgsql.Text,
		},
		pgsql.OperatorEquals,
		pgsql.NewLiteral(value, pgsql.Text),
	)
}

func shortestPathSeedTestExternalPredicate(nodeIdentifier pgsql.Identifier) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		shortestPathSeedTestBoundColumn(nodeIdentifier, pgsql.ColumnID),
		pgsql.OperatorEquals,
		shortestPathSeedTestBoundColumn(shortestPathSeedTestOther, pgsql.ColumnID),
	)
}

func newShortestPathSeedTestBuilder(leftBound, rightBound bool) (*ExpansionBuilder, *Expansion) {
	previousFrame := &Frame{
		Binding: &BoundIdentifier{Identifier: shortestPathSeedTestPreviousFrame},
	}
	expansionFrame := &Frame{
		Previous: previousFrame,
		Binding:  &BoundIdentifier{Identifier: shortestPathSeedTestFrame},
	}
	expansionModel := &Expansion{
		Frame:                   expansionFrame,
		PrimerQueryParameter:    &BoundIdentifier{Identifier: "pi0"},
		RecursiveQueryParameter: &BoundIdentifier{Identifier: "pi1"},
		EdgeStartIdentifier:     pgsql.ColumnStartID,
		EdgeStartColumn:         pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnStartID},
		EdgeEndIdentifier:       pgsql.ColumnEndID,
		EdgeEndColumn:           pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnEndID},
		PrimerNodeJoinCondition: pgd.Equals(pgd.EntityID(shortestPathSeedTestRoot), pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnStartID}),
		ExpansionNodeJoinCondition: pgd.Equals(
			pgd.EntityID(shortestPathSeedTestTerminal),
			pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnEndID},
		),
		Projection: []pgsql.SelectItem{
			pgsql.CompoundIdentifier{shortestPathSeedTestRoot, pgsql.ColumnID},
			pgsql.CompoundIdentifier{shortestPathSeedTestTerminal, pgsql.ColumnID},
		},
	}

	traversalStep := &TraversalStep{
		Frame:          expansionFrame,
		Expansion:      expansionModel,
		LeftNode:       &BoundIdentifier{Identifier: shortestPathSeedTestRoot},
		LeftNodeBound:  leftBound,
		Edge:           &BoundIdentifier{Identifier: shortestPathSeedTestEdge},
		RightNode:      &BoundIdentifier{Identifier: shortestPathSeedTestTerminal},
		RightNodeBound: rightBound,
	}

	return &ExpansionBuilder{
		queryParameters: map[string]any{},
		traversalStep:   traversalStep,
		model:           expansionModel,
	}, expansionModel
}

func TestBoundRootShortestPathPrimerKeepsOnlySeedLocalConstraints(t *testing.T) {
	builder, expansionModel := newShortestPathSeedTestBuilder(true, false)
	expansionModel.PrimerNodeConstraints = pgsql.NewBinaryExpression(
		shortestPathSeedTestLocalFunctionPredicate(shortestPathSeedTestRoot, "1"),
		pgsql.OperatorAnd,
		shortestPathSeedTestExternalPredicate(shortestPathSeedTestRoot),
	)

	query, err := builder.buildShortestPathsHarnessCall(pgsql.FunctionUnidirectionalSPHarness)
	require.NoError(t, err)

	primerQuery, hasPrimerQuery := builder.queryParameters[expansionModel.PrimerQueryParameter.Identifier.String()].(string)
	require.True(t, hasPrimerQuery)
	require.Contains(t, primerQuery, "lower(n0.id)::text = '1'")
	require.NotContains(t, primerQuery, "s0")
	require.NotContains(t, primerQuery, "(s0.x).id")

	formattedQuery, err := format.Statement(query, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Contains(t, formattedQuery, "n0.id = (s0.x).id")
	require.Contains(t, formattedQuery, "(s0.n0).id = s1.root_id")
}

func TestBoundTerminalShortestPathPrimerKeepsOnlySeedLocalConstraints(t *testing.T) {
	builder, expansionModel := newShortestPathSeedTestBuilder(false, true)
	expansionModel.BackwardPrimerQueryParameter = &BoundIdentifier{Identifier: "pi2"}
	expansionModel.BackwardRecursiveQueryParameter = &BoundIdentifier{Identifier: "pi3"}
	expansionModel.TerminalNodeConstraints = pgsql.NewBinaryExpression(
		shortestPathSeedTestLocalFunctionPredicate(shortestPathSeedTestTerminal, "2"),
		pgsql.OperatorAnd,
		shortestPathSeedTestExternalPredicate(shortestPathSeedTestTerminal),
	)

	query, err := builder.buildBiDirectionalShortestPathsHarnessCall(pgsql.FunctionBidirectionalSPHarness)
	require.NoError(t, err)

	backwardPrimerQuery, hasBackwardPrimerQuery := builder.queryParameters[expansionModel.BackwardPrimerQueryParameter.Identifier.String()].(string)
	require.True(t, hasBackwardPrimerQuery)
	require.Contains(t, backwardPrimerQuery, "lower(n1.id)::text = '2'")
	require.NotContains(t, backwardPrimerQuery, "s0")
	require.NotContains(t, backwardPrimerQuery, "(s0.x).id")

	formattedQuery, err := format.Statement(query, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Contains(t, formattedQuery, "n1.id = (s0.x).id")
	require.Contains(t, formattedQuery, "(s0.n1).id = s1.next_id")
}
