package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestNumericSumTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, query := range []string{
				"MATCH (n) RETURN sum(n.x)",
				"MATCH (n) RETURN n.group, sum(DISTINCT n.x)",
				"MATCH (n) WITH n.x AS x RETURN sum(x)",
				"MATCH (n) WITH sum(n.x) AS total RETURN total / 2",
				"RETURN sum(null)",
				"RETURN sum(1.0)",
				"RETURN sum($value)",
			} {
				t.Run(query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), map[string]any{"value": int64(9007199254740993)}, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, "cypher_sum(")
					require.NotContains(t, formatted.Statement, "float8")
					if strings.Contains(query, "DISTINCT") {
						require.Contains(t, formatted.Statement, "cypher_sum(distinct ")
					}
				})
			}
		})
	}
}

func TestNumericSumAggregateRecognition(t *testing.T) {
	call := pgsql.FunctionCall{Function: pgsql.FunctionCypherSum, Parameters: []pgsql.Expression{pgsql.Identifier("value")}, CastType: pgsql.JSONB}
	require.True(t, selectContainsAggregate(pgsql.Select{Projection: []pgsql.SelectItem{call}}))
	symbols, err := GetAggregatedFunctionParameterSymbols(call)
	require.NoError(t, err)
	require.True(t, symbols.Contains(pgsql.Identifier("value")))
}
