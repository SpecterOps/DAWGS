package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

// Pattern-less scalar-list quantifier projections panicked with a nil current pattern
// dereference before containment; each shape must now return a translation error in
// both optimizer modes.
func TestQuantifiersWithoutPatternReturnError(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, quantifier := range []string{"all", "any", "none", "single"} {
				for _, shape := range []struct{ name, query string }{
					{"standalone return", `RETURN ` + quantifier + `(x IN [1] WHERE x = 1) AS value`},
					{"standalone return with property comparison", `RETURN ` + quantifier + `(x IN ['a', 'b'] WHERE x STARTS WITH 'a') AS value`},
					{"with projection continuation", `MATCH (n:MemberOf) WITH ` + quantifier + `(x IN [1] WHERE x = 1) AS ok RETURN ok`},
					{"unwind collection", `UNWIND [1, 2] AS xs RETURN ` + quantifier + `(x IN xs WHERE x > 0) AS value`},
				} {
					t.Run(shape.name+" "+quantifier, func(t *testing.T) {
						parsed, err := frontend.ParseCypher(frontend.NewContext(), shape.query)
						require.NoError(t, err)
						_, err = TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
						require.Error(t, err, "%s must return a translation error instead of panicking", shape.query)
					})
				}
			}

			// The canonical DAWGS-003 trigger fails during projection aggregation with a
			// stable message shared with pattern-present projection quantifiers.
			parsed, err := frontend.ParseCypher(frontend.NewContext(), `RETURN all(x IN [1] WHERE x = 1) AS value`)
			require.NoError(t, err)
			_, err = TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
			require.EqualError(t, err, "aggregate grouping expression has unsupported type pgsql.Select")
		})
	}
}

// Quantifiers in WHERE contexts keep compiling, including scalar-list collections that
// never required a projection.
func TestQuantifierWhereContextsStillTranslate(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct{ name, query string }{
				{"scalar list match where", `MATCH (n:MemberOf) WHERE all(x IN [1, 2] WHERE x < 3) RETURN n`},
				{"scalar list none where", `MATCH (n:MemberOf) WHERE none(x IN [1, 2] WHERE x = 5) RETURN count(n)`},
				{"property array where", `MATCH (n:MemberOf) WHERE any(x IN n.tags WHERE x = 'a') RETURN n`},
				{"labels collection where", `MATCH (n:MemberOf) WHERE any(l IN labels(n) WHERE l = 'MemberOf') RETURN n`},
				{"with output where", `MATCH (n:MemberOf) WITH n WHERE all(x IN [1] WHERE x = 1) RETURN n`},
				{"optional match where", `MATCH (n:MemberOf) OPTIONAL MATCH (n)-[:MemberOf]->(m) WHERE any(x IN [1] WHERE x = 1) RETURN n`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, "count(*)", formatted.Statement)
					require.Contains(t, formatted.Statement, "unnest(", formatted.Statement)
				})
			}
		})
	}
}

// Projection quantifiers over an existing pattern already failed as normal translation
// errors before containment; their messages must not change.
func TestQuantifierProjectionErrorsUnchangedWithPattern(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct{ query, message string }{
				{`MATCH (n:MemberOf) RETURN all(x IN [1] WHERE x = 1) AS value`, "aggregate grouping expression has unsupported type pgsql.Select"},
				{`MATCH (n:MemberOf) RETURN all(x IN n.tags WHERE x = 'a') AS value`, "unknown quantifier loperand expression"},
			} {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				_, err = TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.ErrorContains(t, err, tc.message)
			}
		})
	}
}

// A quantifier visited with neither pattern nor projection state is rejected with the
// preparation boundary error rather than dereferencing missing state.
func TestQuantifierWithoutPatternOrProjectionRejected(t *testing.T) {
	translator := NewTranslator(context.Background(), nil, nil, DefaultGraphID)
	translator.query.AddPart(NewQueryPart(0, 0))

	filterExpression := cypher.NewFilterExpression()
	filterExpression.Specifier = cypher.NewIDInCollection()
	filterExpression.Specifier.Variable = cypher.NewVariableWithSymbol("x")

	err := translator.prepareFilterExpression(filterExpression)
	require.ErrorContains(t, err, "quantifiers are not supported without a pattern or projection expression")
}
