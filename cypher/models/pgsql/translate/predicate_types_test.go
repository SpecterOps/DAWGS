package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestPredicateTypeTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			`MATCH (n) WHERE n.x < 200 RETURN n`,
			`MATCH (n) RETURN 200 > n.x AS value`,
			`MATCH (n) RETURN n.x <= n.y AS value`,
			`MATCH (n) RETURN (n.x + n.y) >= 2 AS value`,
			`MATCH (n) WITH n.x AS x RETURN x < 200 AS value`,
			`MATCH (n) WHERE n.x STARTS WITH '1' SET n.seen = true RETURN n`,
			`MATCH (n) RETURN NOT n.x ENDS WITH $pattern AS value`,
			`MATCH (n) RETURN n.x CONTAINS n.y AS value`,
			`MATCH (n) RETURN n.x =~ '1.*' AS value`,
			`MATCH (n) WITH n.x AS x RETURN x CONTAINS '1' AS value`,
		} {
			t.Run(string(mode)+"/"+query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), map[string]any{"pattern": "3"}, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(result)
				require.NoError(t, err)
				if strings.Contains(query, "STARTS WITH") || strings.Contains(query, "ENDS WITH") || strings.Contains(query, "CONTAINS") || strings.Contains(query, "=~") {
					require.Contains(t, formatted.Statement, "jsonb_typeof(")
					require.Contains(t, formatted.Statement, "and null")
					require.NotContains(t, formatted.Statement, "coalesce(")
				} else {
					require.Contains(t, formatted.Statement, "cypher_compare(")
					require.NotContains(t, formatted.Statement, "->>")
				}
			})
		}
	}
}

func TestUnwindStringPredicatesUseElementTypes(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct{ query, fragment string }{
			{`UNWIND [123] AS x RETURN x STARTS WITH '1' AS value`, "(null)::text like"},
			{`UNWIND ['123'] AS x RETURN x =~ '1.*' AS value`, "(i0)::text ~"},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(result)
				require.NoError(t, err)
				require.Contains(t, formatted.Statement, tc.fragment)
				require.NotContains(t, formatted.Statement, "jsonb_typeof(", "native arrays supply a static element type")
			})
		}
	}
}

func TestPredicateOperandErrors(t *testing.T) {
	invalid := pgsql.CompoundIdentifier{"invalid", "three", "parts"}
	_, _, err := stringPredicateOperand(invalid)
	require.ErrorContains(t, err, "only 2 components")
	for _, operands := range [][2]pgsql.Expression{{invalid, pgsql.NewLiteral("x", pgsql.Text)}, {pgsql.NewLiteral("x", pgsql.Text), invalid}, {pgsql.NewLiteral("x", pgsql.Text), pgsql.NewLiteral(1, pgsql.Text)}} {
		_, err := typedStringPredicate(pgsql.NewBinaryExpression(operands[0], pgsql.OperatorCypherContains, operands[1]))
		require.Error(t, err)
	}
	for _, operands := range [][2]pgsql.Expression{{invalid, pgsql.NewLiteral(1, pgsql.Int8)}, {pgsql.NewLiteral(1, pgsql.Int8), invalid}} {
		_, err := rewriteTypedOrdering(pgsql.NewBinaryExpression(operands[0], pgsql.OperatorLessThan, operands[1]))
		require.ErrorContains(t, err, "only 2 components")
	}
}

func TestPredicateNativeAndRegexPaths(t *testing.T) {
	for _, query := range []string{
		`RETURN 1 < 2 AS value`,
		`RETURN 'a' < 'b' AS value`,
		`MATCH (n) WHERE n.created < datetime($threshold) RETURN n`,
	} {
		t.Run(query, func(t *testing.T) {
			parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
			require.NoError(t, err)
			result, err := Translate(context.Background(), parsed, optimizerSafetyKindMapper(), map[string]any{"threshold": "2026-01-01T00:00:00Z"}, DefaultGraphID)
			require.NoError(t, err)
			formatted, err := Translated(result)
			require.NoError(t, err)
			require.NotContains(t, formatted.Statement, "cypher_compare(")
		})
	}
	parsed, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n) RETURN n.x =~ 'a_b%' AS value`)
	require.NoError(t, err)
	result, err := Translate(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(result)
	require.NoError(t, err)
	require.Contains(t, formatted.Parameters, "__strlit1")
	require.Equal(t, "a_b%", formatted.Parameters["__strlit1"], "type guards must preserve the existing regex pattern spelling")
}
