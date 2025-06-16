package translate_test

import (
	"github.com/stretchr/testify/assert"
	"testing"

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

	// Cases
	testCases := []testCase{{
		Case: &pgsql.Parenthetical{
			Expression: a.Identifier,
		},
		Expected: &pgsql.Parenthetical{
			Expression: pgsql.CompoundIdentifier{frame.Binding.Identifier, a.Identifier},
		},
	}, {
		Case:     pgsql.NewBinaryExpression(a.Identifier, pgsql.OperatorEquals, a.Identifier),
		Expected: pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{frame.Binding.Identifier, a.Identifier}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{frame.Binding.Identifier, a.Identifier}),
	}, {
		Case: &pgsql.AliasedExpression{
			Expression: a.Identifier,
			Alias:      pgsql.AsOptionalIdentifier("name"),
		},
		Expected: &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{frame.Binding.Identifier, a.Identifier},
			Alias:      pgsql.AsOptionalIdentifier("name"),
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
