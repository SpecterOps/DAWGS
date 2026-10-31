package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestPropertyArithmeticTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, query := range []string{
				`MATCH (n) RETURN n.x + n.y AS value`,
				`MATCH (n) RETURN n.x - n.y AS value`,
				`MATCH (n) RETURN n.x * n.y AS value`,
				`MATCH (n) RETURN n.x / n.y AS value`,
				`MATCH (n) RETURN n.x % n.y AS value`,
				`MATCH (n) RETURN n.x ^ n.y AS value`,
				`MATCH (n) RETURN (n.x + n.y) * (n.y - n.x) AS value`,
				`MATCH (n) RETURN (n.x + n.y) / 2.0 AS value`,
				`MATCH (n) RETURN (n.x + n.y) + null AS value`,
				`MATCH (n) WITH n.x AS x, n.y AS y RETURN x + y AS value`,
				`MATCH (n) WHERE n.x + n.y > 9 RETURN n`,
				`MATCH (n) SET n.total = n.x + n.y RETURN n`,
			} {
				t.Run(query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, "cypher_arithmetic(")
				})
			}
			for _, query := range []string{
				`RETURN 7 + 2 AS value`,
				`RETURN 7 / 2 AS value`,
				`RETURN 7.0 / 2.0 AS value`,
				`RETURN 'a' + 'b' AS value`,
				`MATCH (n) RETURN n.x + 2 AS value`,
				`MATCH (n) RETURN n.x + 'b' AS value`,
			} {
				t.Run(query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(result)
					require.NoError(t, err)
					require.NotContains(t, formatted.Statement, "cypher_arithmetic(")
				})
			}
		})
	}
}

func TestArithmeticOperandErrors(t *testing.T) {
	invalid := pgsql.CompoundIdentifier{"invalid", "three", "parts"}
	for _, expression := range []*pgsql.BinaryExpression{
		pgsql.NewBinaryExpression(invalid, pgsql.OperatorAdd, pgsql.NewLiteral(1, pgsql.Int8)),
		pgsql.NewBinaryExpression(pgsql.NewLiteral(1, pgsql.Int8), pgsql.OperatorAdd, invalid),
	} {
		_, err := requiresDynamicArithmetic(expression)
		require.ErrorContains(t, err, "only 2 components")
	}
	_, err := arithmeticJSONOperand(invalid)
	require.ErrorContains(t, err, "only 2 components")

	dynamic := pgsql.FunctionCall{Function: pgsql.FunctionCypherArithmetic, CastType: pgsql.JSONB}
	for _, expression := range []*pgsql.BinaryExpression{
		pgsql.NewBinaryExpression(invalid, pgsql.OperatorEquals, dynamic),
		pgsql.NewBinaryExpression(dynamic, pgsql.OperatorEquals, invalid),
	} {
		_, err := rewriteArithmeticComparison(expression)
		require.ErrorContains(t, err, "only 2 components")
	}
}
