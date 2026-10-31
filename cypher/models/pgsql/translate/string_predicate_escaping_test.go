package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestStringPredicateLiteralEscaping(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, operator := range []struct {
			name    string
			pattern string
		}{
			{"STARTS WITH", `a\_\%\\%`},
			{"ENDS WITH", `%a\_\%\\`},
			{"CONTAINS", `%a\_\%\\%`},
		} {
			for _, operand := range []struct {
				prefix string
				text   string
			}{
				{`MATCH (n)`, `n.name`},
				{`MATCH (n)`, `toLower(n.name)`},
				{`MATCH (n)`, `toUpper(n.name)`},
				{`MATCH (n)`, `toLower(toUpper(n.name))`},
				{`MATCH (n)`, `coalesce(n.name, '')`},
				{`MATCH (n)`, `toString(n.name)`},
				{`WITH 'a_%\\tail' AS value`, `value`},
				{`MATCH (n) WITH toLower(n.name) AS value`, `value`},
				{"", `'a_%\\tail'`},
			} {
				query := fmt.Sprintf(`%s RETURN %s %s 'a_%%\\' AS value`, operand.prefix, operand.text, operator.name)
				t.Run(string(mode)+"/"+query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					var patternParameter string
					for name, value := range formatted.Parameters {
						if value == operator.pattern {
							patternParameter = "@" + name + "::text"
						}
					}
					require.NotEmpty(t, patternParameter, "literal pattern %q missing from %#v", operator.pattern, formatted.Parameters)
					require.Contains(t, formatted.Statement, " like "+patternParameter)
					require.NotContains(t, formatted.Statement, "cypher_like_escape(", "literal patterns need only compile-time escaping")
					if operand.text == "toLower(n.name)" {
						require.Contains(t, formatted.Statement, " ilike "+patternParameter)
						require.Equal(t, 2, strings.Count(formatted.Statement, patternParameter), "prefilter and exact predicate must share the escaped pattern")
					}
				})
			}
		}
	}
}

func TestFunctionalStringPredicateEscapingControls(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, operator := range []string{"STARTS WITH", "ENDS WITH", "CONTAINS", "=~"} {
			query := fmt.Sprintf(`MATCH (n) RETURN toLower(n.name) %s $needle AS value`, operator)
			t.Run(string(mode)+"/"+operator, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), map[string]any{"needle": `a_%\b`}, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				require.Equal(t, `a_%\b`, translation.Parameters["pi0"], "parameters must reach runtime escaping unchanged")
				if operator == "=~" {
					require.Contains(t, formatted.Statement, " ~ ")
					require.NotContains(t, formatted.Statement, "cypher_like_escape(")
				} else {
					require.Contains(t, formatted.Statement, "cypher_like_escape(")
					require.Contains(t, formatted.Statement, " ilike ")
				}
			})
		}
		query := `MATCH (n) RETURN toLower(n.name) =~ 'a_%' AS value`
		parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
		require.NoError(t, err)
		translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		require.Equal(t, map[string]any{"__strlit0": "a_%"}, formatted.Parameters, "regex literals must retain their spelling")
	}
}
