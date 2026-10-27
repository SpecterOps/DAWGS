package cypher_test

import (
	"math"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

// TestFormatFloatLiteralKeepsFloatingPointSpelling verifies that integral
// float values retain a floating-point literal spelling instead of degrading
// to integer text, and that non-integral and non-finite values keep their
// shortest round-trip rendering.
func TestFormatFloatLiteralKeepsFloatingPointSpelling(t *testing.T) {
	testCases := []struct {
		name     string
		input    float64
		expected string
	}{
		{name: "integral positive", input: 3, expected: "3.0"},
		{name: "integral negative", input: -2, expected: "-2.0"},
		{name: "integral zero", input: 0, expected: "0.0"},
		{name: "negative zero", input: math.Copysign(0, -1), expected: "-0.0"},
		{name: "fractional", input: 1.5, expected: "1.5"},
		{name: "negative fractional", input: -0.25, expected: "-0.25"},
		{name: "small magnitude", input: 1e-7, expected: "0.0000001"},
		{name: "large magnitude", input: 1e15, expected: "1000000000000000.0"},
		{name: "beyond integer precision", input: 1e16, expected: "10000000000000000.0"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rendered := cypher.FormatFloatLiteral(testCase.input)
			require.Equal(t, testCase.expected, rendered)

			// The rendered text must parse back to the same float value.
			parsed, err := parseReturnedFloatLiteral(t, rendered)
			require.NoError(t, err)
			require.Equal(t, testCase.input, parsed)
		})
	}
}

// TestFormatFloatLiteralLeavesNonFiniteRenderingsAlone verifies that values
// without a decimal literal spelling keep their strconv rendering rather
// than producing malformed text.
func TestFormatFloatLiteralLeavesNonFiniteRenderingsAlone(t *testing.T) {
	require.Equal(t, "NaN", cypher.FormatFloatLiteral(math.NaN()))
	require.Equal(t, "+Inf", cypher.FormatFloatLiteral(math.Inf(1)))
	require.Equal(t, "-Inf", cypher.FormatFloatLiteral(math.Inf(-1)))
}

// parseReturnedFloatLiteral projects the rendered literal through the Cypher
// frontend and asserts it parses as exactly one floating-point literal.
func parseReturnedFloatLiteral(t *testing.T, rendered string) (float64, error) {
	t.Helper()

	query, err := frontend.ParseCypher(frontend.NewContext(), "return "+rendered+" as value")
	if err != nil {
		return 0, err
	}

	singlePart := query.SingleQuery.SinglePartQuery
	require.NotNil(t, singlePart)
	require.Len(t, singlePart.Return.Projection.Items, 1)

	projectionItem, isProjectionItem := singlePart.Return.Projection.Items[0].(*cypher.ProjectionItem)
	require.True(t, isProjectionItem, "rendered float literal projected as %T", singlePart.Return.Projection.Items[0])

	// Negative spellings parse as a unary minus applied to wrappers around a
	// positive literal; strip those wrappers down to the literal itself while
	// tracking the applied sign.
	var (
		expression = projectionItem.Expression
		negated    = false
	)

	for {
		switch typedExpression := expression.(type) {
		case *cypher.UnaryAddOrSubtractExpression:
			negated = !negated
			expression = typedExpression.Right
		case *cypher.Parenthetical:
			expression = typedExpression.Expression
		case *cypher.ArithmeticExpression:
			require.Empty(t, typedExpression.Partials)
			expression = typedExpression.Left
		default:
			literal, isLiteral := expression.(*cypher.Literal)
			require.True(t, isLiteral, "rendered float literal parsed as %T", projectionItem.Expression)
			value, isFloat := literal.Value.(float64)
			require.True(t, isFloat, "rendered float literal parsed as literal type %T", literal.Value)
			if negated {
				value = -value
			}
			return value, nil
		}
	}
}
