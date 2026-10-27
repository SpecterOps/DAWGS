package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestBooleanExpressionsRemainValuesUntilPredicateExtraction(t *testing.T) {
	for _, operator := range []pgsql.Operator{pgsql.OperatorAnd, pgsql.OperatorOr} {
		t.Run(string(operator), func(t *testing.T) {
			translator := NewExpressionTreeTranslator(nil)
			translator.VisitOperator(operator)
			translator.PushOperand(pgsql.NewLiteral(true, pgsql.Boolean))
			translator.PushOperand(pgsql.NewLiteral(false, pgsql.Boolean))
			require.NoError(t, translator.CompleteBinaryExpression(NewScope(), operator))
			require.Empty(t, translator.UserConstraints.Constraints)
			require.Empty(t, translator.TranslationConstraints.Constraints)
			value := translator.PeekOperand().(*pgsql.BinaryExpression)
			require.Equal(t, operator, value.Operator)
			require.Equal(t, true, value.LOperand.(pgsql.Literal).Value)
			require.Equal(t, false, value.ROperand.(pgsql.Literal).Value)

			require.NoError(t, translator.PopRemainingExpressionsAsUserConstraints())
			require.True(t, translator.treeBuilder.IsEmpty())
			require.Len(t, translator.UserConstraints.Constraints, 1)
		})
	}
}

func TestBooleanValuesWithEmptyBuilderPredicates(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			builder := query.NewBuilderWithCriteria(
				query.Where(query.And(
					query.And(),
					query.GreaterThan(query.NodeProperty("k"), 0),
					query.And(query.And(), query.LessThan(query.NodeProperty("k"), 2)),
				)),
				query.Returning(query.Node()),
			)
			parsed, err := builder.Build(false)
			require.NoError(t, err)
			result, err := TranslateWithOptions(context.Background(), parsed, pgutil.NewInMemoryKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
			require.NoError(t, err)
			formatted, err := Translated(result)
			require.NoError(t, err)
			require.Contains(t, formatted.Statement, "and")
			require.Contains(t, formatted.Statement, "> 0")
			require.Contains(t, formatted.Statement, "< 0")
		})
	}
}

// Explicit extraction remains available to users of the expression tree API.
func TestExplicitBooleanConstraintExtraction(t *testing.T) {
	translator := NewExpressionTreeTranslator(nil)
	require.ErrorContains(t, translator.ConstrainConjoinedOperandPair(), "at least one operand")
	require.ErrorContains(t, translator.ConstrainDisjointOperandPair(), "at least one operand")
	translator.PushOperand(pgsql.NewLiteral(true, pgsql.Boolean))
	translator.PushOperand(pgsql.NewLiteral(false, pgsql.Boolean))
	require.NoError(t, translator.ConstrainDisjointOperandPair())
	require.Empty(t, translator.UserConstraints.Constraints)
	require.Equal(t, pgsql.OperatorOr, translator.PeekOperand().(*pgsql.BinaryExpression).Operator)
	require.NoError(t, translator.ConstrainDisjointOperandPair())
	require.Len(t, translator.UserConstraints.Constraints, 1)
	require.True(t, translator.treeBuilder.IsEmpty())

	translator = NewExpressionTreeTranslator(nil)
	translator.PushOperand(pgsql.NewLiteral(true, pgsql.Boolean))
	require.NoError(t, translator.ConstrainConjoinedOperandPair())
	require.Len(t, translator.UserConstraints.Constraints, 1)
	require.True(t, translator.treeBuilder.IsEmpty())
}

func TestPredicateExtractionRetainsBooleanGrouping(t *testing.T) {
	a := pgsql.CompoundIdentifier{"a", "active"}
	b := pgsql.CompoundIdentifier{"b", "active"}
	for _, expression := range []pgsql.Expression{
		pgsql.NewBinaryExpression(a, pgsql.OperatorOr, b),
		pgsql.NewParenthetical(pgsql.NewBinaryExpression(a, pgsql.OperatorAnd, b)),
	} {
		translator := NewExpressionTreeTranslator(nil)
		translator.PushOperand(expression)
		require.NoError(t, translator.PopRemainingExpressionsAsUserConstraints())
		require.Len(t, translator.UserConstraints.Constraints, 1)
		require.True(t, translator.UserConstraints.Constraints[0].Dependencies.Matches(pgsql.AsIdentifierSet("a", "b")))
		require.Same(t, expression, translator.UserConstraints.Constraints[0].Expression)
	}
}
