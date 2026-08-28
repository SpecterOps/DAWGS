package translate

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models"
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

func TestRewriteCurrentFrameProjectionReferencesCopiesHandledExpressions(t *testing.T) {
	const (
		frameID pgsql.Identifier = "s0"
		alias   pgsql.Identifier = "n0"
	)

	var (
		ref         = func() pgsql.CompoundIdentifier { return pgsql.CompoundIdentifier{frameID, alias} }
		replacement = func() pgsql.CompoundIdentifier { return pgsql.CompoundIdentifier{"s1", alias} }
		aliases     = map[pgsql.Identifier]pgsql.Expression{alias: replacement()}
	)

	newTypeCast := func(expression pgsql.Expression) *pgsql.TypeCast {
		return &pgsql.TypeCast{Expression: expression, CastType: pgsql.Text}
	}
	newCompositeValue := func(expression pgsql.Expression) *pgsql.CompositeValue {
		return &pgsql.CompositeValue{Values: []pgsql.Expression{expression}, DataType: pgsql.NodeComposite}
	}
	newArrayLiteral := func(expression pgsql.Expression) *pgsql.ArrayLiteral {
		return &pgsql.ArrayLiteral{Values: []pgsql.Expression{expression}, CastType: pgsql.Int8}
	}
	newArrayExpression := func(expression pgsql.Expression) *pgsql.ArrayExpression {
		return &pgsql.ArrayExpression{Expression: expression}
	}
	newArrayIndex := func(expression pgsql.Expression) *pgsql.ArrayIndex {
		return &pgsql.ArrayIndex{Expression: expression, Indexes: []pgsql.Expression{expression}, CastType: pgsql.Int8}
	}
	newArraySlice := func(expression pgsql.Expression) *pgsql.ArraySlice {
		return &pgsql.ArraySlice{Expression: expression, Lower: expression, Upper: expression, CastType: pgsql.Int8Array}
	}
	newAllExpression := func(expression pgsql.Expression) *pgsql.AllExpression {
		allExpression := pgsql.NewAllExpression(expression)
		return &allExpression
	}
	newCase := func(expression pgsql.Expression) *pgsql.Case {
		return &pgsql.Case{
			Operand:    expression,
			Conditions: []pgsql.Expression{expression},
			Then:       []pgsql.Expression{expression},
			Else:       expression,
		}
	}
	newFunctionCall := func(expression pgsql.Expression) *pgsql.FunctionCall {
		return &pgsql.FunctionCall{
			Function:   pgsql.FunctionToLower,
			Parameters: []pgsql.Expression{expression},
			Over: &pgsql.Window{
				PartitionBy: []pgsql.Expression{expression},
				OrderBy:     []pgsql.OrderBy{{Expression: expression, Ascending: false}},
				WindowFrame: &pgsql.WindowFrame{
					Unit:          pgsql.WindowFrameUnitRows,
					StartBoundary: pgsql.WindowFrameBoundary{BoundaryType: pgsql.WindowFrameBoundaryTypePreceding, BoundaryLiteral: &pgsql.Literal{Value: 1, CastType: pgsql.Int4}},
					EndBoundary:   &pgsql.WindowFrameBoundary{BoundaryType: pgsql.WindowFrameBoundaryTypeCurrentRow},
				},
			},
			Distinct: true,
			CastType: pgsql.Text,
		}
	}
	newSelect := func(expression pgsql.Expression) pgsql.Select {
		return pgsql.Select{
			Projection: pgsql.Projection{
				pgsql.AliasedExpression{Expression: expression, Alias: models.OptionalValue(alias)},
			},
			From: []pgsql.FromClause{{
				Source: pgsql.LateralSubquery{
					Query: pgsql.Query{Body: pgsql.Select{Where: expression}},
				},
				Joins: []pgsql.Join{{
					Table: pgsql.LateralSubquery{
						Query: pgsql.Query{Body: pgsql.Select{Where: expression}},
					},
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: expression},
				}},
			}},
			Where:   expression,
			GroupBy: []pgsql.Expression{expression},
			Having:  expression,
		}
	}
	newQuery := func(expression pgsql.Expression) *pgsql.Query {
		return &pgsql.Query{
			Body:    newSelect(expression),
			OrderBy: []*pgsql.OrderBy{{Expression: expression, Ascending: false}},
			Offset:  expression,
			Limit:   expression,
		}
	}
	newSetOperation := func(expression pgsql.Expression) *pgsql.SetOperation {
		return &pgsql.SetOperation{
			LOperand: newSelect(expression),
			ROperand: pgsql.Select{Where: expression},
			Operator: pgsql.OperatorUnion,
			All:      true,
			Distinct: true,
		}
	}
	newProjectionFrom := func(expression pgsql.Expression) *pgsql.ProjectionFrom {
		return &pgsql.ProjectionFrom{
			Projection: pgsql.Projection{pgsql.AliasedExpression{Expression: expression, Alias: models.OptionalValue(alias)}},
			From: []pgsql.FromClause{{
				Joins: []pgsql.Join{{JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: expression}}},
			}},
		}
	}
	ptrToSelect := func(selectBody pgsql.Select) *pgsql.Select {
		return &selectBody
	}

	testCases := []struct {
		name     string
		actual   pgsql.Expression
		original pgsql.Expression
		expected pgsql.Expression
	}{
		{
			name:     "unary expression pointer",
			actual:   &pgsql.UnaryExpression{Operator: pgsql.OperatorNot, Operand: ref()},
			original: &pgsql.UnaryExpression{Operator: pgsql.OperatorNot, Operand: ref()},
			expected: &pgsql.UnaryExpression{Operator: pgsql.OperatorNot, Operand: replacement()},
		},
		{
			name:     "binary expression pointer",
			actual:   pgsql.NewBinaryExpression(ref(), pgsql.OperatorEquals, ref()),
			original: pgsql.NewBinaryExpression(ref(), pgsql.OperatorEquals, ref()),
			expected: pgsql.NewBinaryExpression(replacement(), pgsql.OperatorEquals, replacement()),
		},
		{
			name:     "function call pointer",
			actual:   newFunctionCall(ref()),
			original: newFunctionCall(ref()),
			expected: newFunctionCall(replacement()),
		},
		{
			name:     "type cast pointer",
			actual:   newTypeCast(ref()),
			original: newTypeCast(ref()),
			expected: newTypeCast(replacement()),
		},
		{
			name:     "composite value pointer",
			actual:   newCompositeValue(ref()),
			original: newCompositeValue(ref()),
			expected: newCompositeValue(replacement()),
		},
		{
			name:     "parenthetical pointer",
			actual:   pgsql.NewParenthetical(ref()),
			original: pgsql.NewParenthetical(ref()),
			expected: pgsql.NewParenthetical(replacement()),
		},
		{
			name:     "edge array from path IDs pointer",
			actual:   &pgsql.EdgeArrayFromPathIDs{PathIDs: ref()},
			original: &pgsql.EdgeArrayFromPathIDs{PathIDs: ref()},
			expected: &pgsql.EdgeArrayFromPathIDs{PathIDs: replacement()},
		},
		{
			name:     "array literal pointer",
			actual:   newArrayLiteral(ref()),
			original: newArrayLiteral(ref()),
			expected: newArrayLiteral(replacement()),
		},
		{
			name:     "array expression pointer",
			actual:   newArrayExpression(ref()),
			original: newArrayExpression(ref()),
			expected: newArrayExpression(replacement()),
		},
		{
			name:     "array index pointer",
			actual:   newArrayIndex(ref()),
			original: newArrayIndex(ref()),
			expected: newArrayIndex(replacement()),
		},
		{
			name:     "array slice pointer",
			actual:   newArraySlice(ref()),
			original: newArraySlice(ref()),
			expected: newArraySlice(replacement()),
		},
		{
			name:     "all expression pointer",
			actual:   newAllExpression(ref()),
			original: newAllExpression(ref()),
			expected: newAllExpression(replacement()),
		},
		{
			name:     "any expression pointer",
			actual:   pgsql.NewAnyExpression(ref(), pgsql.Int8Array),
			original: pgsql.NewAnyExpression(ref(), pgsql.Int8Array),
			expected: pgsql.NewAnyExpression(replacement(), pgsql.Int8Array),
		},
		{
			name:     "case pointer",
			actual:   newCase(ref()),
			original: newCase(ref()),
			expected: newCase(replacement()),
		},
		{
			name: "exists expression pointer",
			actual: &pgsql.ExistsExpression{
				Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Where: ref()}}},
				Negated:  true,
			},
			original: &pgsql.ExistsExpression{
				Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Where: ref()}}},
				Negated:  true,
			},
			expected: &pgsql.ExistsExpression{
				Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Where: replacement()}}},
				Negated:  true,
			},
		},
		{
			name:     "subquery pointer",
			actual:   &pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Where: ref()}}},
			original: &pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Where: ref()}}},
			expected: &pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Where: replacement()}}},
		},
		{
			name:     "query pointer",
			actual:   newQuery(ref()),
			original: newQuery(ref()),
			expected: newQuery(replacement()),
		},
		{
			name:     "query pointer with select pointer body",
			actual:   &pgsql.Query{Body: ptrToSelect(pgsql.Select{Where: ref()})},
			original: &pgsql.Query{Body: ptrToSelect(pgsql.Select{Where: ref()})},
			expected: &pgsql.Query{Body: ptrToSelect(pgsql.Select{Where: replacement()})},
		},
		{
			name:     "query pointer with set operation pointer body",
			actual:   &pgsql.Query{Body: newSetOperation(ref())},
			original: &pgsql.Query{Body: newSetOperation(ref())},
			expected: &pgsql.Query{Body: newSetOperation(replacement())},
		},
		{
			name:     "select pointer",
			actual:   ptrToSelect(newSelect(ref())),
			original: ptrToSelect(newSelect(ref())),
			expected: ptrToSelect(newSelect(replacement())),
		},
		{
			name:     "set operation pointer",
			actual:   newSetOperation(ref()),
			original: newSetOperation(ref()),
			expected: newSetOperation(replacement()),
		},
		{
			name:     "projection value",
			actual:   pgsql.Projection{ref()},
			original: pgsql.Projection{ref()},
			expected: pgsql.Projection{replacement()},
		},
		{
			name:     "projection from pointer",
			actual:   newProjectionFrom(ref()),
			original: newProjectionFrom(ref()),
			expected: newProjectionFrom(replacement()),
		},
		{
			name:     "aliased expression pointer",
			actual:   &pgsql.AliasedExpression{Expression: ref(), Alias: models.OptionalValue(alias)},
			original: &pgsql.AliasedExpression{Expression: ref(), Alias: models.OptionalValue(alias)},
			expected: &pgsql.AliasedExpression{Expression: replacement(), Alias: models.OptionalValue(alias)},
		},
		{
			name:     "variadic pointer",
			actual:   &pgsql.Variadic{Expression: ref()},
			original: &pgsql.Variadic{Expression: ref()},
			expected: &pgsql.Variadic{Expression: replacement()},
		},
		{
			name:     "lateral subquery pointer",
			actual:   &pgsql.LateralSubquery{Query: pgsql.Query{Body: pgsql.Select{Where: ref()}}, Binding: models.OptionalValue(alias)},
			original: &pgsql.LateralSubquery{Query: pgsql.Query{Body: pgsql.Select{Where: ref()}}, Binding: models.OptionalValue(alias)},
			expected: &pgsql.LateralSubquery{Query: pgsql.Query{Body: pgsql.Select{Where: replacement()}}, Binding: models.OptionalValue(alias)},
		},
		{
			name:     "values pointer",
			actual:   &pgsql.Values{Values: []pgsql.Expression{ref()}},
			original: &pgsql.Values{Values: []pgsql.Expression{ref()}},
			expected: &pgsql.Values{Values: []pgsql.Expression{replacement()}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rewritten := rewriteCurrentFrameProjectionReferences(testCase.actual, frameID, aliases)

			require.Equal(t, testCase.original, testCase.actual)
			require.Equal(t, testCase.expected, rewritten)
			requireDistinctPointers(t, testCase.actual, rewritten)
		})
	}
}
func requireDistinctPointers(t *testing.T, original pgsql.Expression, rewritten pgsql.Expression) {
	t.Helper()

	originalValue := reflect.ValueOf(original)
	if originalValue.Kind() != reflect.Ptr {
		return
	}

	rewrittenValue := reflect.ValueOf(rewritten)
	require.Equal(t, reflect.Ptr, rewrittenValue.Kind())
	require.NotEqual(t, originalValue.Pointer(), rewrittenValue.Pointer())
}
