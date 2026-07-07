package translate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

func mustDefineNew(t *testing.T, scope *translate.Scope, dataType pgsql.DataType) *translate.BoundIdentifier {
	binding, err := scope.DefineNew(dataType)
	require.Nil(t, err)

	return binding
}

func mustPushFrame(t *testing.T, scope *translate.Scope) *translate.Frame {
	frame, err := scope.PushFrame()
	require.Nil(t, err)

	return frame
}

func TestRewriteFrameBindings(t *testing.T) {
	type testCase struct {
		Case     pgsql.Expression
		Expected pgsql.Expression
	}

	var (
		scope = translate.NewScope()
		frame = mustPushFrame(t, scope)
		a     = mustDefineNew(t, scope, pgsql.Int)
	)

	frame.Reveal(a.Identifier)
	frame.Export(a.Identifier)

	a.MaterializedBy(frame)
	rewrittenA := pgsql.CompoundIdentifier{frame.Binding.Identifier, a.Identifier}

	// Cases
	testCases := []testCase{{
		Case: &pgsql.Parenthetical{
			Expression: a.Identifier,
		},
		Expected: &pgsql.Parenthetical{
			Expression: rewrittenA,
		},
	}, {
		Case:     pgsql.NewBinaryExpression(a.Identifier, pgsql.OperatorEquals, a.Identifier),
		Expected: pgsql.NewBinaryExpression(rewrittenA, pgsql.OperatorEquals, rewrittenA),
	}, {
		Case: &pgsql.AliasedExpression{
			Expression: a.Identifier,
			Alias:      pgsql.AsOptionalIdentifier("name"),
		},
		Expected: &pgsql.AliasedExpression{
			Expression: rewrittenA,
			Alias:      pgsql.AsOptionalIdentifier("name"),
		},
	}, {
		Case: &pgsql.EdgeArrayFromPathIDs{
			PathIDs: a.Identifier,
		},
		Expected: &pgsql.EdgeArrayFromPathIDs{
			PathIDs: rewrittenA,
		},
	}, {
		Case: pgsql.NewBinaryExpression(
			a.Identifier,
			pgsql.OperatorEquals,
			pgsql.NewAnyExpression(pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{a.Identifier},
				CastType: pgsql.IntArray,
			}, pgsql.IntArray),
		),
		Expected: pgsql.NewBinaryExpression(
			rewrittenA,
			pgsql.OperatorEquals,
			pgsql.NewAnyExpression(pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{rewrittenA},
				CastType: pgsql.IntArray,
			}, pgsql.IntArray),
		),
	}, {
		Case: pgsql.NewBinaryExpression(
			pgsql.ArraySlice{
				Expression: a.Identifier,
				Lower:      a.Identifier,
				Upper:      a.Identifier,
			},
			pgsql.OperatorEquals,
			a.Identifier,
		),
		Expected: pgsql.NewBinaryExpression(
			pgsql.ArraySlice{
				Expression: rewrittenA,
				Lower:      rewrittenA,
				Upper:      rewrittenA,
			},
			pgsql.OperatorEquals,
			rewrittenA,
		),
	}, {
		Case: &pgsql.Case{
			Operand: a.Identifier,
			Conditions: []pgsql.Expression{
				a.Identifier,
			},
			Then: []pgsql.Expression{
				a.Identifier,
			},
			Else: a.Identifier,
		},
		Expected: &pgsql.Case{
			Operand: rewrittenA,
			Conditions: []pgsql.Expression{
				rewrittenA,
			},
			Then: []pgsql.Expression{
				rewrittenA,
			},
			Else: rewrittenA,
		},
	}}

	for _, nextTestCase := range testCases {
		t.Run("", func(t *testing.T) {
			assert.NoError(t, translate.RewriteFrameBindings(scope, nextTestCase.Case))

			var (
				formattedCase, formattedCaseErr         = format.SyntaxNode(nextTestCase.Case)
				formattedExpected, formattedExpectedErr = format.SyntaxNode(nextTestCase.Expected)
			)

			assert.NoError(t, formattedCaseErr)
			assert.NoError(t, formattedExpectedErr)

			assert.Equal(t, formattedExpected, formattedCase)
		})
	}
}

func TestRewriteFrameBindings_ArraySliceDirectParents(t *testing.T) {
	var (
		scope = translate.NewScope()
		frame = mustPushFrame(t, scope)
		a     = mustDefineNew(t, scope, pgsql.Int)
	)

	frame.Reveal(a.Identifier)
	frame.Export(a.Identifier)

	a.MaterializedBy(frame)
	rewrittenA := pgsql.CompoundIdentifier{frame.Binding.Identifier, a.Identifier}

	newSlice := func(expression pgsql.Expression) pgsql.ArraySlice {
		return pgsql.ArraySlice{
			Expression: expression,
			Lower:      expression,
			Upper:      expression,
		}
	}

	testCases := []struct {
		name     string
		actual   pgsql.Expression
		expected pgsql.Expression
	}{{
		name: "projection value",
		actual: pgsql.Projection{
			newSlice(a.Identifier),
		},
		expected: pgsql.Projection{
			newSlice(rewrittenA),
		},
	}, {
		name: "projection pointer",
		actual: pgsql.Projection{
			func() *pgsql.ArraySlice {
				value := newSlice(a.Identifier)
				return &value
			}(),
		},
		expected: pgsql.Projection{
			func() *pgsql.ArraySlice {
				value := newSlice(rewrittenA)
				return &value
			}(),
		},
	}, {
		name: "order by value",
		actual: &pgsql.OrderBy{
			Expression: newSlice(a.Identifier),
			Ascending:  true,
		},
		expected: &pgsql.OrderBy{
			Expression: newSlice(rewrittenA),
			Ascending:  true,
		},
	}, {
		name: "order by pointer",
		actual: &pgsql.OrderBy{
			Expression: func() *pgsql.ArraySlice {
				value := newSlice(a.Identifier)
				return &value
			}(),
			Ascending: true,
		},
		expected: &pgsql.OrderBy{
			Expression: func() *pgsql.ArraySlice {
				value := newSlice(rewrittenA)
				return &value
			}(),
			Ascending: true,
		},
	}}

	for _, nextTestCase := range testCases {
		t.Run(nextTestCase.name, func(t *testing.T) {
			require.NoError(t, translate.RewriteFrameBindings(scope, nextTestCase.actual))
			assert.Equal(t, nextTestCase.expected, nextTestCase.actual)
		})
	}
}
