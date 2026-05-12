package translate_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

func mustAsLiteral(value any) pgsql.Literal {
	if literal, err := pgsql.AsLiteral(value); err != nil {
		panic(fmt.Sprintf("%v", err))
	} else {
		return literal
	}
}

func TestInferExpressionType(t *testing.T) {
	type testCase struct {
		ExpectedType pgsql.DataType
		Expression   pgsql.Expression
		Exclusive    bool
	}

	testCases := []testCase{{
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewBinaryExpression(
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				mustAsLiteral("field_a"),
			),
			pgsql.OperatorAnd,
			pgsql.NewBinaryExpression(
				mustAsLiteral("123"),
				pgsql.OperatorIn,
				pgsql.ArrayLiteral{
					Values:   []pgsql.Expression{mustAsLiteral("a"), mustAsLiteral("b")},
					CastType: pgsql.TextArray,
				},
			),
		),
	}, {
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewBinaryExpression(
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				mustAsLiteral("field_a"),
			),
			pgsql.OperatorAnd,
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				mustAsLiteral("field_b"),
			),
		),
	}, {
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewBinaryExpression(
			mustAsLiteral("123"),
			pgsql.OperatorIn,
			pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{mustAsLiteral("a"), mustAsLiteral("b")},
				CastType: pgsql.TextArray,
			},
		),
	}, {
		ExpectedType: pgsql.Text,
		Expression: pgsql.NewBinaryExpression(
			mustAsLiteral("123"),
			pgsql.OperatorConcatenate,
			mustAsLiteral("456"),
		),
	}, {
		ExpectedType: pgsql.Text,
		Expression: pgsql.NewBinaryExpression(
			mustAsLiteral("n"),
			pgsql.OperatorConcatenate,
			mustAsLiteral(123),
		),
	}, {
		ExpectedType: pgsql.Int8,
		Expression: pgsql.NewBinaryExpression(
			mustAsLiteral(123),
			pgsql.OperatorAdd,
			pgsql.NewBinaryExpression(
				mustAsLiteral(123),
				pgsql.OperatorMultiply,
				mustAsLiteral(1),
			),
		),
	}, {
		ExpectedType: pgsql.Int8,
		Expression: pgsql.NewBinaryExpression(
			mustAsLiteral(123),
			pgsql.OperatorAdd,
			pgsql.NewBinaryExpression(
				mustAsLiteral(int16(123)),
				pgsql.OperatorMultiply,
				mustAsLiteral(int16(1)),
			),
		),
	}, {
		Exclusive:    true,
		ExpectedType: pgsql.Int4,
		Expression: pgsql.NewBinaryExpression(
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				mustAsLiteral("field"),
			),
			pgsql.OperatorAdd,
			pgsql.NewBinaryExpression(
				mustAsLiteral(int16(123)),
				pgsql.OperatorMultiply,
				mustAsLiteral(int32(1)),
			),
		),
	}}

	var (
		exclusive    []testCase
		hasExclusive bool
	)

	for _, nextCase := range testCases {
		if hasExclusive {
			if nextCase.Exclusive {
				exclusive = append(exclusive, nextCase)
			}
		} else if nextCase.Exclusive {
			hasExclusive = true

			exclusive = exclusive[:0]
			exclusive = append(exclusive, nextCase)
		} else {
			exclusive = append(exclusive, nextCase)
		}
	}

	for _, nextCase := range exclusive {
		if testName, err := format.Expression(nextCase.Expression, format.NewOutputBuilder()); err != nil {
			t.Fatalf("unable to format test case expression: %v", err)
		} else {
			t.Run(testName, func(t *testing.T) {
				inferredType, err := translate.InferExpressionType(nextCase.Expression)

				require.Nil(t, err)
				require.Equal(t, nextCase.ExpectedType, inferredType)
			})
		}
	}
}

func TestInferUnaryExpressionType(t *testing.T) {
	testCases := []struct {
		Name         string
		ExpectedType pgsql.DataType
		Expression   pgsql.Expression
	}{{
		Name:         "not",
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewUnaryExpression(
			pgsql.OperatorNot,
			pgsql.NewBinaryExpression(
				mustAsLiteral("a"),
				pgsql.OperatorEquals,
				mustAsLiteral("a"),
			),
		),
	}, {
		Name:         "negative numeric",
		ExpectedType: pgsql.Int4,
		Expression: pgsql.NewUnaryExpression(
			pgsql.OperatorSubtract,
			mustAsLiteral(int32(1)),
		),
	}, {
		Name:         "positive numeric value",
		ExpectedType: pgsql.Int2,
		Expression: pgsql.UnaryExpression{
			Operator: pgsql.OperatorAdd,
			Operand:  mustAsLiteral(int16(1)),
		},
	}, {
		Name:         "unknown numeric operand",
		ExpectedType: pgsql.UnknownDataType,
		Expression: pgsql.NewUnaryExpression(
			pgsql.OperatorSubtract,
			pgsql.Identifier("x"),
		),
	}, {
		Name:         "non-numeric operand",
		ExpectedType: pgsql.UnknownDataType,
		Expression: pgsql.NewUnaryExpression(
			pgsql.OperatorSubtract,
			mustAsLiteral("text"),
		),
	}, {
		Name:         "is null",
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewUnaryExpression(
			pgsql.OperatorIs,
			pgsql.Identifier("x"),
		),
	}, {
		Name:         "is not null",
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewUnaryExpression(
			pgsql.OperatorIsNot,
			pgsql.Identifier("x"),
		),
	}, {
		Name:         "nil pointer",
		ExpectedType: pgsql.UnknownDataType,
		Expression:   (*pgsql.UnaryExpression)(nil),
	}}

	for _, nextCase := range testCases {
		t.Run(nextCase.Name, func(t *testing.T) {
			inferredType, err := translate.InferExpressionType(nextCase.Expression)

			require.NoError(t, err)
			require.Equal(t, nextCase.ExpectedType, inferredType)
		})
	}
}

func TestInferWrappedExpressionType(t *testing.T) {
	testCases := []struct {
		Name         string
		ExpectedType pgsql.DataType
		Expression   pgsql.Expression
	}{{
		Name:         "composite value",
		ExpectedType: pgsql.PathComposite,
		Expression: pgsql.CompositeValue{
			DataType: pgsql.PathComposite,
		},
	}, {
		Name:         "empty composite value",
		ExpectedType: pgsql.UnknownDataType,
		Expression:   pgsql.CompositeValue{},
	}, {
		Name:         "aliased expression",
		ExpectedType: pgsql.Text,
		Expression: pgsql.AliasedExpression{
			Expression: mustAsLiteral("text"),
			Alias:      pgsql.AsOptionalIdentifier("alias"),
		},
	}, {
		Name:         "aliased expression pointer",
		ExpectedType: pgsql.Int8,
		Expression: &pgsql.AliasedExpression{
			Expression: mustAsLiteral(int64(1)),
			Alias:      pgsql.AsOptionalIdentifier("alias"),
		},
	}, {
		Name:         "exists expression",
		ExpectedType: pgsql.Boolean,
		Expression:   pgsql.ExistsExpression{},
	}, {
		Name:         "negated exists expression",
		ExpectedType: pgsql.Boolean,
		Expression:   pgsql.ExistsExpression{Negated: true},
	}, {
		Name:         "variadic expression",
		ExpectedType: pgsql.Int8Array,
		Expression: pgsql.Variadic{
			Expression: pgsql.ArrayLiteral{
				CastType: pgsql.Int8Array,
			},
		},
	}, {
		Name:         "all expression",
		ExpectedType: pgsql.Int8,
		Expression: pgsql.NewAllExpression(pgsql.ArrayLiteral{
			CastType: pgsql.Int8Array,
		}),
	}, {
		Name:         "unknown all expression",
		ExpectedType: pgsql.UnknownDataType,
		Expression:   pgsql.NewAllExpression(pgsql.Identifier("path")),
	}, {
		Name:         "all expression over scalar",
		ExpectedType: pgsql.UnknownDataType,
		Expression:   pgsql.NewAllExpression(mustAsLiteral(int64(1))),
	}}

	for _, nextCase := range testCases {
		t.Run(nextCase.Name, func(t *testing.T) {
			inferredType, err := translate.InferExpressionType(nextCase.Expression)

			require.NoError(t, err)
			require.Equal(t, nextCase.ExpectedType, inferredType)
		})
	}
}

func TestPropertyLookupEqualityScalarRewrites(t *testing.T) {
	propertyLookup := func(property string) *pgsql.BinaryExpression {
		return pgsql.NewPropertyLookup(
			pgsql.CompoundIdentifier{"n", pgsql.ColumnProperties},
			mustAsLiteral(property),
		)
	}

	renderEquality := func(t *testing.T, lOperand, rOperand pgsql.Expression) string {
		t.Helper()

		treeTranslator := translate.NewExpressionTreeTranslator(nil)
		treeTranslator.PushOperand(lOperand)
		treeTranslator.PushOperand(rOperand)
		require.NoError(t, treeTranslator.CompleteBinaryExpression(translate.NewScope(), pgsql.OperatorEquals))

		formatted, err := format.Expression(treeTranslator.PeekOperand(), format.NewOutputBuilder())
		require.NoError(t, err)

		return formatted
	}

	testCases := []struct {
		Name     string
		LOperand pgsql.Expression
		ROperand pgsql.Expression
		Expected string
	}{{
		Name:     "text literal keeps text property lookup",
		LOperand: propertyLookup("isassignabletorole"),
		ROperand: mustAsLiteral("true"),
		Expected: "(n.properties ->> 'isassignabletorole') = 'true'",
	}, {
		Name:     "text literal keeps text property lookup when reversed",
		LOperand: mustAsLiteral("true"),
		ROperand: propertyLookup("isassignabletorole"),
		Expected: "'true' = (n.properties ->> 'isassignabletorole')",
	}, {
		Name:     "boolean literal keeps jsonb scalar equality",
		LOperand: propertyLookup("isassignabletorole"),
		ROperand: mustAsLiteral(true),
		Expected: "((n.properties -> 'isassignabletorole'))::jsonb = to_jsonb((true)::bool)::jsonb",
	}, {
		Name:     "numeric literal keeps jsonb scalar equality",
		LOperand: propertyLookup("count"),
		ROperand: mustAsLiteral(1),
		Expected: "((n.properties -> 'count'))::jsonb = to_jsonb((1)::int8)::jsonb",
	}, {
		Name:     "property to property equality keeps jsonb operands",
		LOperand: propertyLookup("left"),
		ROperand: propertyLookup("right"),
		Expected: "(n.properties -> 'left') = (n.properties -> 'right')",
	}}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			require.Equal(t, testCase.Expected, renderEquality(t, testCase.LOperand, testCase.ROperand))
		})
	}
}

func TestExpressionTreeTranslator(t *testing.T) {
	// Tree translator is a stack oriented expression tree builder
	var (
		treeTranslator = translate.NewExpressionTreeTranslator(nil)
		scope          = translate.NewScope()
	)

	// Case: Translating the constraint: a.name = 'a' and a.num_a > 1 and b.name = 'b' and a.other = b.other

	// Perform a prefix visit of the parent expression and its operator. This is used for tracking
	// conjunctions and disjunctions.
	treeTranslator.VisitOperator(pgsql.OperatorEquals)

	// Postfix visit and push the compound identifier first: a.name
	treeTranslator.PushOperand(pgsql.CompoundIdentifier{"a", "name"})

	// Postfix visit and push the literal next: "a"
	treeTranslator.PushOperand(mustAsLiteral("a"))

	// Perform a postfix visit of the parent expression and its operator.
	require.Nil(t, treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorEquals))

	// Expect one newly created binary expression to be the only thing left on the tree
	// translator's operand stack
	require.IsType(t, &pgsql.BinaryExpression{}, treeTranslator.PeekOperand())

	// Continue with: and a.num_a > 1
	// Preform a prefix visit of the 'and' operator:
	treeTranslator.VisitOperator(pgsql.OperatorAnd)

	// Preform a prefix visit of the '>' operator:
	treeTranslator.VisitOperator(pgsql.OperatorGreaterThan)

	// Postfix visit and push the compound identifier first: a.num_a
	treeTranslator.PushOperand(pgsql.CompoundIdentifier{"a", "num_a"})

	// Postfix visit and push the literal next: 1
	treeTranslator.PushOperand(mustAsLiteral(1))

	// Perform a postfix visit of the parent expression and its operator.
	require.Nil(t, treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorGreaterThan))

	// Perform a postfix visit of the conjoining parent expression and its operator.
	require.Nil(t, treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorAnd))

	// Continue with: and b.name = "b"
	// Preform a prefix visit of the 'and' operator:
	treeTranslator.VisitOperator(pgsql.OperatorAnd)

	// Preform a prefix visit of the '=' operator:
	treeTranslator.VisitOperator(pgsql.OperatorEquals)

	// Postfix visit and push the compound identifier first: b.name
	treeTranslator.PushOperand(pgsql.CompoundIdentifier{"b", "name"})

	// Postfix visit and push the literal next: "b"
	treeTranslator.PushOperand(mustAsLiteral("b"))

	// Perform a postfix visit of the parent expression and its operator.
	require.Nil(t, treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorEquals))

	// Perform a postfix visit of the conjoining parent expression and its operator.
	require.Nil(t, treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorAnd))

	// Continue with: and a.other = b.other
	// enter Op(and), enter Op(=)
	treeTranslator.VisitOperator(pgsql.OperatorAnd)
	treeTranslator.VisitOperator(pgsql.OperatorEquals)

	// push LOperand, push ROperand
	treeTranslator.PushOperand(pgsql.CompoundIdentifier{"a", "other"})
	treeTranslator.PushOperand(pgsql.CompoundIdentifier{"b", "other"})

	// exit  exit Op(=), Op(and)
	treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorEquals)
	treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorAnd)

	// Assign remaining operands as constraints
	treeTranslator.PopRemainingExpressionsAsUserConstraints()

	// Pull out the 'a' constraint
	aIdentifier := pgsql.AsIdentifierSet("a")
	expectedTranslation := "(a.name = 'a' and a.num_a > 1)"
	validateConstraints(t, treeTranslator, aIdentifier, expectedTranslation)

	// Pull out the 'b' constraint next
	bIdentifier := pgsql.AsIdentifierSet("b")
	expectedTranslation = "(b.name = 'b')"
	validateConstraints(t, treeTranslator, bIdentifier, expectedTranslation)

	// Pull out the constraint that depends on both 'a' and 'b' identifiers
	idents := pgsql.AsIdentifierSet("a", "b")
	expectedTranslation = "(a.other = b.other)"
	validateConstraints(t, treeTranslator, idents, expectedTranslation)
}

func validateConstraints(t *testing.T, constraintTracker *translate.ExpressionTreeTranslator, idents *pgsql.IdentifierSet, expectedTranslation string) {
	constraint, err := constraintTracker.ConsumeConstraintsFromVisibleSet(idents)

	require.NotNil(t, constraint)
	require.True(t, constraint.Dependencies.Matches(idents))
	require.Nil(t, err)

	formattedConstraint, err := format.Expression(constraint.Expression, format.NewOutputBuilder())

	require.Nil(t, err)
	require.Equal(t, expectedTranslation, formattedConstraint)
}
