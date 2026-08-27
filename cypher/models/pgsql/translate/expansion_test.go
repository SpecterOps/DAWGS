package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// translateCypher parses and translates a Cypher query into formatted PostgreSQL for shape assertions.
func translateCypher(t *testing.T, cypher string) string {
	t.Helper()

	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("NodeKind1"))

	query, err := frontend.ParseCypher(frontend.NewContext(), cypher)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)

	return formatted
}

// TestSelfLoopExpansionInLaterFrameSeedsIndependently covers a variable-length self-loop introduced in a
// later MATCH part. The endpoints share a binding, but the node is not exported by the previous frame, so
// it must be seeded from all nodes rather than from the previous frame, and no previous-frame endpoint
// constraint may be emitted. See isUnboundSelfLoop.
func TestSelfLoopExpansionInLaterFrameSeedsIndependently(t *testing.T) {
	formatted := translateCypher(t, `MATCH (x) MATCH (n)-[*..]->(n) RETURN x, n`)

	// Seed reads from the node table, not from the previous frame s0.
	require.Contains(t, formatted, "select n1.id as root_id from node n1")
	// No invalid previous-frame reference to the self-loop node is emitted anywhere.
	require.NotContains(t, formatted, "(s0.n1)")
	// The self-loop identity constraint still ties the endpoints.
	require.Contains(t, formatted, "s2.root_id = s2.next_id")
	// The carried x binding is still projected.
	require.Contains(t, formatted, "s1.n0 as x")
}

// TestSelfLoopExpansionCarriedNodeStaysBound covers a variable-length self-loop whose node is genuinely
// carried by a previous WITH. The node is exported by the previous frame, so it must stay bound to that
// frame (seeded from it and constrained to it) rather than being seeded independently.
func TestSelfLoopExpansionCarriedNodeStaysBound(t *testing.T) {
	formatted := translateCypher(t, `MATCH (n) WITH n MATCH (n)-[*..]->(n) RETURN n`)

	// Seed reads the carried node from the previous frame.
	require.Contains(t, formatted, "select distinct (s0.n0).id as root_id from s0")
	// The expansion stays constrained to the carried node.
	require.Contains(t, formatted, "(s0.n0).id = s3.root_id")
}

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
	require.Equal(t, "case when s1.root_id != s1.next_id then true else shortest_path_self_endpoint_error(s1.root_id, s1.next_id) end", projectionGuard.Statement)
	require.NotContains(t, projectionGuard.Statement, " / ")

	terminalFilterGuard, err := format.Expression(
		shortestPathSeedSelfEndpointGuard(pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnStartID}, false),
		format.NewOutputBuilder(),
	)
	require.NoError(t, err)
	require.Contains(t, terminalFilterGuard.Statement, "case when (select count(*)::int8 from traversal_terminal_filter where traversal_terminal_filter.id = e0.start_id) = 0 then true else shortest_path_self_endpoint_error(e0.start_id, e0.start_id) end")
	require.NotContains(t, terminalFilterGuard.Statement, " / ")

	endpointPairFilterGuard, err := format.Expression(
		shortestPathSeedSelfEndpointGuard(pgsql.CompoundIdentifier{shortestPathSeedTestEdge, pgsql.ColumnStartID}, true),
		format.NewOutputBuilder(),
	)
	require.NoError(t, err)
	require.Contains(t, endpointPairFilterGuard.Statement, "case when (select count(*)::int8 from traversal_pair_filter where traversal_pair_filter.root_id = e0.start_id and traversal_pair_filter.terminal_id = e0.start_id) = 0 then true else shortest_path_self_endpoint_error(e0.start_id, e0.start_id) end")
	require.NotContains(t, endpointPairFilterGuard.Statement, " / ")
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
	require.Contains(t, formattedQuery.Statement, "n0.id = (s0.x).id")
	require.Contains(t, formattedQuery.Statement, "(s0.n0).id = s1.root_id")
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
	require.Contains(t, formattedQuery.Statement, "n1.id = (s0.x).id")
	require.Contains(t, formattedQuery.Statement, "(s0.n1).id = s1.next_id")
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
	require.Contains(t, formattedQuery.Statement, "select s1_seed.root_id, s1_seed.root_id, 0, false, false")
	require.NotContains(t, formattedQuery.Statement, "e0")
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

	require.Contains(t, formattedQuery.Statement, zeroDepthBranch)
	require.Contains(t, formattedQuery.Statement, primerBranch)
	require.Contains(t, formattedQuery.Statement, recursiveBranch)
	require.Contains(t, formattedQuery.Statement, "where s1.depth > 0")
	require.Less(t, strings.Index(formattedQuery.Statement, zeroDepthBranch), strings.Index(formattedQuery.Statement, primerBranch))
	require.Less(t, strings.Index(formattedQuery.Statement, primerBranch), strings.Index(formattedQuery.Statement, recursiveBranch))
}
