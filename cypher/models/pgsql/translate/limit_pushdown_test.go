package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

const (
	limitPushdownTestSourceFrame   pgsql.Identifier = "s0"
	limitPushdownTestHarnessFrame  pgsql.Identifier = "s1"
	limitPushdownTestPreviousFrame pgsql.Identifier = "s2"
	limitPushdownTestRootAlias     pgsql.Identifier = "n0"
	limitPushdownTestTerminalAlias pgsql.Identifier = "n1"
)

func limitPushdownTestEndpointRef(alias pgsql.Identifier) pgsql.RowColumnReference {
	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{limitPushdownTestSourceFrame, alias},
		Column:     pgsql.ColumnID,
	}
}

func limitPushdownTestEndpointInequality(leftAlias, rightAlias pgsql.Identifier) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		limitPushdownTestEndpointRef(leftAlias),
		pgsql.OperatorCypherNotEquals,
		limitPushdownTestEndpointRef(rightAlias),
	)
}

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

func limitPushdownTestSourceWhere(t *testing.T, part *QueryPart, where pgsql.Expression) {
	t.Helper()

	sourceCTE := findCTE(part.Model, limitPushdownTestSourceFrame)
	require.NotNil(t, sourceCTE)

	selectBody, isSelect := sourceCTE.Query.Body.(pgsql.Select)
	require.True(t, isSelect)

	selectBody.Where = where
	sourceCTE.Query.Body = selectBody
}

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

func limitPushdownTestTail(where pgsql.Expression) pgsql.Select {
	return pgsql.Select{
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: pgsql.CompoundIdentifier{limitPushdownTestSourceFrame}},
		}},
		Where: where,
	}
}

func TestLimitPushdownTailSourceAllowsUnidirectionalShortestPathEndpointInequality(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestRootAlias,
		limitPushdownTestTerminalAlias,
	))

	sourceFrame, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

func TestLimitPushdownTailSourceAllowsReversedUnidirectionalShortestPathEndpointInequality(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestTerminalAlias,
		limitPushdownTestRootAlias,
	))

	sourceFrame, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

func TestLimitPushdownTailSourceBlocksMixedShortestPathWherePredicate(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
	tailSelect := limitPushdownTestTail(pgsql.NewBinaryExpression(
		limitPushdownTestEndpointInequality(limitPushdownTestRootAlias, limitPushdownTestTerminalAlias),
		pgsql.OperatorAnd,
		pgsql.NewBinaryExpression(
			limitPushdownTestEndpointRef(limitPushdownTestTerminalAlias),
			pgsql.OperatorGreaterThan,
			pgsql.NewLiteral(0, pgsql.Int),
		),
	))

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
	part := limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestRootAlias,
		limitPushdownTestTerminalAlias,
	))

	sourceFrame, canPushDown := limitPushdownTailSource(part, tailSelect)
	require.True(t, canPushDown)
	require.Equal(t, limitPushdownTestSourceFrame, sourceFrame)
}

func TestPushDownShortestPathLimitAppendsHarnessLimitWithEndpointInequality(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionUnidirectionalSPHarness)
	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestRootAlias,
		limitPushdownTestTerminalAlias,
	))

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

func TestPushDownBidirectionalShortestPathLimitAppendsHarnessLimitWithEndpointInequality(t *testing.T) {
	part := limitPushdownTestPart(pgsql.FunctionBidirectionalSPHarness)
	tailSelect := limitPushdownTestTail(limitPushdownTestEndpointInequality(
		limitPushdownTestRootAlias,
		limitPushdownTestTerminalAlias,
	))

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
