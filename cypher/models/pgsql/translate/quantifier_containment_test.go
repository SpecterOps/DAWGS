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
				for _, shape := range []struct {
					// name identifies this test case in subtest and benchmark output.
					name string
					// query is the Cypher statement exercised by this case.
					query string
				}{
					{name: "standalone return", query: `RETURN ` + quantifier + `(x IN [1] WHERE x = 1) AS value`},
					{name: "standalone return with property comparison", query: `RETURN ` + quantifier + `(x IN ['a', 'b'] WHERE x STARTS WITH 'a') AS value`},
					{name: "with projection continuation", query: `MATCH (n:MemberOf) WITH ` + quantifier + `(x IN [1] WHERE x = 1) AS ok RETURN ok`},
					{name: "unwind collection", query: `UNWIND [1, 2] AS xs RETURN ` + quantifier + `(x IN xs WHERE x > 0) AS value`},
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
			for _, tc := range []struct {
				// name identifies this test case in subtest and benchmark output.
				name string
				// query is the Cypher statement exercised by this case.
				query string
			}{
				{name: "scalar list match where", query: `MATCH (n:MemberOf) WHERE all(x IN [1, 2] WHERE x < 3) RETURN n`},
				{name: "scalar list none where", query: `MATCH (n:MemberOf) WHERE none(x IN [1, 2] WHERE x = 5) RETURN count(n)`},
				{name: "property array where", query: `MATCH (n:MemberOf) WHERE any(x IN n.tags WHERE x = 'a') RETURN n`},
				{name: "labels collection where", query: `MATCH (n:MemberOf) WHERE any(l IN labels(n) WHERE l = 'MemberOf') RETURN n`},
				{name: "with output where", query: `MATCH (n:MemberOf) WITH n WHERE all(x IN [1] WHERE x = 1) RETURN n`},
				{name: "optional match where", query: `MATCH (n:MemberOf) OPTIONAL MATCH (n)-[:MemberOf]->(m) WHERE any(x IN [1] WHERE x = 1) RETURN n`},
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
			for _, tc := range []struct {
				// query is the Cypher statement exercised by this case.
				query string
				// message is the diagnostic substring expected for the rejected query.
				message string
			}{
				{query: `MATCH (n:MemberOf) RETURN all(x IN [1] WHERE x = 1) AS value`, message: "aggregate grouping expression has unsupported type pgsql.Select"},
				{query: `MATCH (n:MemberOf) RETURN all(x IN n.tags WHERE x = 'a') AS value`, message: "unknown quantifier loperand expression"},
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
