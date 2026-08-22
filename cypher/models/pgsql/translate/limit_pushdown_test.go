package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

const (
	// limitPushdownTestSourceFrame identifies the source frame referenced by limit-pushdown fixtures.
	limitPushdownTestSourceFrame pgsql.Identifier = "s0"

	// limitPushdownTestHarnessFrame identifies the shortest-path harness frame in limit-pushdown fixtures.
	limitPushdownTestHarnessFrame pgsql.Identifier = "s1"

	// limitPushdownTestPreviousFrame identifies the frame that supplies bound endpoints in fixtures.
	limitPushdownTestPreviousFrame pgsql.Identifier = "s2"

	// limitPushdownTestRootAlias identifies the root-node binding in limit-pushdown fixtures.
	limitPushdownTestRootAlias pgsql.Identifier = "n0"

	// limitPushdownTestTerminalAlias identifies the terminal-node binding in limit-pushdown fixtures.
	limitPushdownTestTerminalAlias pgsql.Identifier = "n1"
)

// limitPushdownTestEndpointRef references an endpoint ID projected by the fixture source frame.
func limitPushdownTestEndpointRef(alias pgsql.Identifier) pgsql.RowColumnReference {
	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{limitPushdownTestSourceFrame, alias},
		Column:     pgsql.ColumnID,
	}
}

// limitPushdownTestEndpointInequality builds the Cypher inequality used to exclude identical endpoints.
func limitPushdownTestEndpointInequality(leftAlias, rightAlias pgsql.Identifier) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		limitPushdownTestEndpointRef(leftAlias),
		pgsql.OperatorCypherNotEquals,
		limitPushdownTestEndpointRef(rightAlias),
	)
}

// limitPushdownTestBoundEndpointConstraint equates a previous-frame endpoint ID with a harness expansion column.
func limitPushdownTestBoundEndpointConstraint(endpointAlias, expansionColumn pgsql.Identifier) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		pgsql.RowColumnReference{
			Identifier: pgsql.CompoundIdentifier{limitPushdownTestPreviousFrame, endpointAlias},
			Column:     pgsql.ColumnID,
		},
		pgsql.OperatorEquals,
		pgsql.CompoundIdentifier{limitPushdownTestHarnessFrame, expansionColumn},
	)
}

// limitPushdownTestSourceWhere combines the fixture's root, terminal, and endpoint-pair constraints.
func limitPushdownTestSourceWhere(t *testing.T, part *QueryPart, where pgsql.Expression) {
	t.Helper()

	sourceCTE := findCTE(part.Model, limitPushdownTestSourceFrame)
	require.NotNil(t, sourceCTE)

	selectBody, isSelect := sourceCTE.Query.Body.(pgsql.Select)
	require.True(t, isSelect)

	selectBody.Where = where
	sourceCTE.Query.Body = selectBody
}

// limitPushdownTestJoin joins one bound endpoint from the previous frame to the shortest-path harness.
func limitPushdownTestJoin(nodeAlias, expansionColumn pgsql.Identifier) pgsql.Join {
	return pgsql.Join{
		Table: pgsql.TableReference{
			Name:    pgsql.TableNode.AsCompoundIdentifier(),
			Binding: models.OptionalValue(nodeAlias),
		},
		JoinOperator: pgsql.JoinOperator{
			JoinType: pgsql.JoinTypeInner,
			Constraint: pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{nodeAlias, pgsql.ColumnID},
				pgsql.OperatorEquals,
				pgsql.CompoundIdentifier{limitPushdownTestHarnessFrame, expansionColumn},
			),
		},
	}
}

// limitPushdownTestPart constructs a query part containing a bounded shortest-path harness and final projection.
func limitPushdownTestPart(harnessFunction pgsql.Identifier) *QueryPart {
	part := NewQueryPart(1, 0)
	part.Limit = pgsql.NewLiteral(10, pgsql.Int)
	part.AllowLimitPushdown(limitPushdownTestSourceFrame)
	part.Model.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: limitPushdownTestSourceFrame},
		Query: pgsql.Query{
			CommonTableExpressions: &pgsql.With{Expressions: []pgsql.CommonTableExpression{{
				Alias: pgsql.TableAlias{Name: limitPushdownTestHarnessFrame},
				Query: pgsql.Query{
					Body: pgsql.Select{From: []pgsql.FromClause{{
						Source: pgsql.FunctionCall{Function: harnessFunction},
					}}},
				},
			}}},
			Body: pgsql.Select{From: []pgsql.FromClause{{
				Source: pgsql.TableReference{Name: pgsql.CompoundIdentifier{limitPushdownTestHarnessFrame}},
				Joins: []pgsql.Join{
					limitPushdownTestJoin(limitPushdownTestRootAlias, expansionRootID),
					limitPushdownTestJoin(limitPushdownTestTerminalAlias, expansionNextID),
				},
			}}},
		},
	})

	return part
}

// limitPushdownTestTail returns the terminal query part used to determine whether a limit may be pushed down.
func limitPushdownTestTail(where pgsql.Expression) pgsql.Select {
	return pgsql.Select{
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: pgsql.CompoundIdentifier{limitPushdownTestSourceFrame}},
		}},
		Where: where,
	}
}

func TestLimitPushdownTailSourceAllowsUnidirectionalShortestPathEndpointInequality(t *testing.T) {
	var (
		part       = limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
		tailSelect = limitPushdownTestTail(limitPushdownTestEndpointInequality(
			limitPushdownTestRootAlias,
			limitPushdownTestTerminalAlias,
		))
	)

	sourceFrame, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

func TestLimitPushdownTailSourceAllowsReversedUnidirectionalShortestPathEndpointInequality(t *testing.T) {
	var (
		part       = limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
		tailSelect = limitPushdownTestTail(limitPushdownTestEndpointInequality(
			limitPushdownTestTerminalAlias,
			limitPushdownTestRootAlias,
		))
	)

	sourceFrame, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

func TestLimitPushdownTailSourceBlocksMixedShortestPathWherePredicate(t *testing.T) {
	var (
		part       = limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
		tailSelect = limitPushdownTestTail(pgsql.NewBinaryExpression(
			limitPushdownTestEndpointInequality(limitPushdownTestRootAlias, limitPushdownTestTerminalAlias),
			pgsql.OperatorAnd,
			pgsql.NewBinaryExpression(
				limitPushdownTestEndpointRef(limitPushdownTestTerminalAlias),
				pgsql.OperatorGreaterThan,
				pgsql.NewLiteral(0, pgsql.Int),
			),
		))
	)

	_, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.False(t, canPushDown)
}

func TestLimitPushdownTailSourceBlocksFilteredShortestPathSource(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
	limitPushdownTestSourceWhere(t, part, pgsql.NewLiteral(true, pgsql.Boolean))

	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestRootAlias,
		limitPushdownTestTerminalAlias,
	))

	_, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.False(t, canPushDown)
}

func TestLimitPushdownTailSourceAllowsBoundEndpointShortestPathSource(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
	limitPushdownTestSourceWhere(t, part, pgsql.NewBinaryExpression(
		limitPushdownTestBoundEndpointConstraint(limitPushdownTestRootAlias, expansionRootID),
		pgsql.OperatorAnd,
		limitPushdownTestBoundEndpointConstraint(limitPushdownTestTerminalAlias, expansionNextID),
	))

	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestRootAlias,
		limitPushdownTestTerminalAlias,
	))

	sourceFrame, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

func TestLimitPushdownTailSourceBlocksUnrelatedSourceEndpointConstraint(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
	limitPushdownTestSourceWhere(t, part, limitPushdownTestBoundEndpointConstraint(
		pgsql.Identifier("n2"),
		expansionRootID,
	))

	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestRootAlias,
		limitPushdownTestTerminalAlias,
	))

	_, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.False(t, canPushDown)
}

func TestLimitPushdownTailSourceAllowsBoundEndpointShortestPathSourceWithoutTailWhere(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
	limitPushdownTestSourceWhere(t, part, limitPushdownTestBoundEndpointConstraint(
		limitPushdownTestRootAlias,
		expansionRootID,
	))

	sourceFrame, canPushDown := limitPushdownTailSource(part, limitPushdownTestTail(nil))
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

func TestLimitPushdownTailSourceAllowsBidirectionalShortestPathEndpointInequality(t *testing.T) {
	var (
		part       = limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
		tailSelect = limitPushdownTestTail(limitPushdownTestEndpointInequality(
			limitPushdownTestRootAlias,
			limitPushdownTestTerminalAlias,
		))
	)

	sourceFrame, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

// TestPushDownShortestPathLimitAppendsHarnessLimitWithEndpointInequality verifies endpoint filtering does not displace the harness limit.
func TestPushDownShortestPathLimitAppendsHarnessLimitWithEndpointInequality(t *testing.T) {
	var (
		part       = limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
		tailSelect = limitPushdownTestTail(limitPushdownTestEndpointInequality(
			limitPushdownTestRootAlias,
			limitPushdownTestTerminalAlias,
		))
	)

	pushDownShortestPathLimit(part, tailSelect)

	sourceCTE := findCTE(part.Model, limitPushdownTestSourceFrame)
	require.NotNil(t, sourceCTE)
	require.NotNil(t, sourceCTE.Query.CommonTableExpressions)
	require.Len(t, sourceCTE.Query.CommonTableExpressions.Expressions, 1)

	harnessCTE := sourceCTE.Query.CommonTableExpressions.Expressions[0]
	require.Equal(t, part.Limit, harnessCTE.Query.Limit)
	selectBody, isSelect := harnessCTE.Query.Body.(pgsql.Select)
	require.True(t, isSelect)
	require.Len(t, selectBody.From, 1)

	functionCall, isFunctionCall := selectBody.From[0].Source.(pgsql.FunctionCall)
	require.True(t, isFunctionCall)
	require.Len(t, functionCall.Parameters, 1)
	require.Equal(t, pgsql.NewTypeCast(part.Limit, pgsql.Int8), functionCall.Parameters[0])
}

func TestPushDownBidirectionalShortestPathLimitAppendsHarnessLimitWithEndpointInequality(t *testing.T) {
	var (
		part       = limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
		tailSelect = limitPushdownTestTail(limitPushdownTestEndpointInequality(
			limitPushdownTestRootAlias,
			limitPushdownTestTerminalAlias,
		))
	)

	pushDownShortestPathLimit(part, tailSelect)

	sourceCTE := findCTE(part.Model, limitPushdownTestSourceFrame)
	require.NotNil(t, sourceCTE)
	require.NotNil(t, sourceCTE.Query.CommonTableExpressions)
	require.Len(t, sourceCTE.Query.CommonTableExpressions.Expressions, 1)

	harnessCTE := sourceCTE.Query.CommonTableExpressions.Expressions[0]
	selectBody, isSelect := harnessCTE.Query.Body.(pgsql.Select)
	require.True(t, isSelect)
	require.Len(t, selectBody.From, 1)

	functionCall, isFunctionCall := selectBody.From[0].Source.(pgsql.FunctionCall)
	require.True(t, isFunctionCall)
	require.Len(t, functionCall.Parameters, 1)
	require.Equal(t, pgsql.NewTypeCast(part.Limit, pgsql.Int8), functionCall.Parameters[0])
}

func TestPushDownBidirectionalShortestPathLimitAllowsBoundEndpointSourceWhere(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
	limitPushdownTestSourceWhere(t, part, pgsql.NewBinaryExpression(
		limitPushdownTestBoundEndpointConstraint(limitPushdownTestRootAlias, expansionRootID),
		pgsql.OperatorAnd,
		limitPushdownTestBoundEndpointConstraint(limitPushdownTestTerminalAlias, expansionNextID),
	))

	pushDownShortestPathLimit(part, limitPushdownTestTail(nil))

	sourceCTE := findCTE(part.Model, limitPushdownTestSourceFrame)
	require.NotNil(t, sourceCTE)
	require.NotNil(t, sourceCTE.Query.CommonTableExpressions)
	require.Len(t, sourceCTE.Query.CommonTableExpressions.Expressions, 1)

	harnessCTE := sourceCTE.Query.CommonTableExpressions.Expressions[0]
	selectBody, isSelect := harnessCTE.Query.Body.(pgsql.Select)
	require.True(t, isSelect)
	require.Len(t, selectBody.From, 1)

	functionCall, isFunctionCall := selectBody.From[0].Source.(pgsql.FunctionCall)
	require.True(t, isFunctionCall)
	require.Len(t, functionCall.Parameters, 1)
	require.Equal(t, pgsql.NewTypeCast(part.Limit, pgsql.Int8), functionCall.Parameters[0])
}
