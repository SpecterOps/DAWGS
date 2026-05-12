package translate

import (
	"strings"
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

func TestShortestPathSelfEndpointGuardsUseCaseErrorHelper(t *testing.T) {
	projectionGuard, err := format.Expression(shortestPathSelfEndpointGuard(shortestPathSeedTestFrame), format.NewOutputBuilder())
	require.NoError(t, err)
	require.Equal(t, "case when s1.root_id != s1.next_id then true else shortest_path_self_endpoint_error(s1.root_id, s1.next_id) end", projectionGuard)
	require.NotContains(t, projectionGuard, " / ")

	terminalFilterGuard, err := format.Expression(
		shortestPathSeedSelfEndpointGuard(pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnStartID}, false),
		format.NewOutputBuilder(),
	)
	require.NoError(t, err)
	require.Contains(t, terminalFilterGuard, "case when (select count(*)::int8 from traversal_terminal_filter where traversal_terminal_filter.id = e0.start_id) = 0 then true else shortest_path_self_endpoint_error(e0.start_id, e0.start_id) end")
	require.NotContains(t, terminalFilterGuard, " / ")

	endpointPairFilterGuard, err := format.Expression(
		shortestPathSeedSelfEndpointGuard(pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnStartID}, true),
		format.NewOutputBuilder(),
	)
	require.NoError(t, err)
	require.Contains(t, endpointPairFilterGuard, "case when (select count(*)::int8 from traversal_pair_filter where traversal_pair_filter.root_id = e0.start_id and traversal_pair_filter.terminal_id = e0.start_id) = 0 then true else shortest_path_self_endpoint_error(e0.start_id, e0.start_id) end")
	require.NotContains(t, endpointPairFilterGuard, " / ")
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

func TestZeroDepthExpansionRejectsEdgeDependentTerminalSatisfaction(t *testing.T) {
	builder, expansionModel := newShortestPathSeedTestBuilder(false, false)
	seed := newExpansionNodeSeed(expansionSeedIdentifier(shortestPathSeedTestFrame), shortestPathSeedTestRoot, nil)
	expansionModel.TerminalNodeSatisfactionProjection = pgsql.NewBinaryExpression(
		pgsql.RowColumnReference{
			Identifier: &pgsql.ArrayIndex{
				Expression: pgsql.NewParenthetical(shortestPathSeedTestEdge),
				Indexes: []pgsql.Expression{
					pgd.IntLiteral(1),
				},
				CastType: pgsql.EdgeComposite,
			},
			Column: pgsql.ColumnProperties,
		},
		pgsql.OperatorJSONTextField,
		pgd.TextLiteral("enforced"),
	)

	zeroDepthSelect, err := builder.buildZeroDepthExpansionSelect(&seed)
	require.NoError(t, err)

	formattedQuery, err := format.Statement(pgsql.Query{Body: zeroDepthSelect}, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Contains(t, formattedQuery, "select s1_seed.root_id, s1_seed.root_id, 0, false, false")
	require.NotContains(t, formattedQuery, "e0")
}

func TestZeroDepthExpansionBuildKeepsPrimerBranch(t *testing.T) {
	expansionSelect := func(root, next, depth int64, isCycle pgsql.SelectItem, edgeID int64) pgsql.Select {
		return pgsql.Select{
			Projection: []pgsql.SelectItem{
				pgsql.NewLiteral(root, pgsql.Int8),
				pgsql.NewLiteral(next, pgsql.Int8),
				pgsql.NewLiteral(depth, pgsql.Int),
				pgsql.NewLiteral(true, pgsql.Boolean),
				isCycle,
				pgsql.ArrayLiteral{
					Values: []pgsql.Expression{
						pgsql.NewLiteral(edgeID, pgsql.Int8),
					},
				},
			},
		}
	}

	zeroDepthStatement := pgsql.Select{
		Projection: []pgsql.SelectItem{
			pgsql.NewLiteral(int64(1), pgsql.Int8),
			pgsql.NewLiteral(int64(1), pgsql.Int8),
			pgsql.NewLiteral(int64(0), pgsql.Int),
			pgsql.NewLiteral(true, pgsql.Boolean),
			pgsql.NewLiteral(false, pgsql.Boolean),
			pgsql.ArrayLiteral{CastType: pgsql.Int8Array},
		},
	}

	builder := ExpansionBuilder{
		PrimerStatement: expansionSelect(
			1,
			2,
			1,
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnStartID},
				pgsql.OperatorEquals,
				pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnEndID},
			),
			7,
		),
		RecursiveStatement:  expansionSelect(1, 3, 2, pgsql.NewLiteral(false, pgsql.Boolean), 8),
		ProjectionStatement: pgsql.Select{Projection: []pgsql.SelectItem{pgsql.NewLiteral(1, pgsql.Int)}},
		ZeroDepthStatement:  &zeroDepthStatement,
		UseUnionAll:         true,
	}

	query := builder.Build(shortestPathSeedTestFrame)
	formattedQuery, err := format.Statement(query, format.NewOutputBuilder())
	require.NoError(t, err)

	zeroDepthBranch := "select 1, 1, 0, true, false, array []::int8[]"
	primerBranch := "select 1, 2, 1, true, e0.start_id = e0.end_id, array [7]"
	recursiveBranch := "select 1, 3, 2, true, false, array [8]"

	require.Contains(t, formattedQuery, zeroDepthBranch)
	require.Contains(t, formattedQuery, primerBranch)
	require.Contains(t, formattedQuery, recursiveBranch)
	require.Contains(t, formattedQuery, "where s1.depth > 0")
	require.Less(t, strings.Index(formattedQuery, zeroDepthBranch), strings.Index(formattedQuery, primerBranch))
	require.Less(t, strings.Index(formattedQuery, primerBranch), strings.Index(formattedQuery, recursiveBranch))
}
