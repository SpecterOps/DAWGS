package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

// TestTypedCollectTranslation checks SQL collection types for entity, scalar and dynamic input expressions.
func TestTypedCollectTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				// query is the Cypher statement exercised by this case.
				query string
				// fragment is the SQL substring required in the translation.
				fragment string
			}{
				{
					query:    "MATCH (n) RETURN collect(n.x)",
					fragment: "array_agg(nullif(",
				},
				{
					query:    "MATCH ()-[r]->() RETURN collect(DISTINCT r.x)",
					fragment: "array_agg(distinct nullif(",
				},
				{
					query:    "MATCH (n) WITH n.x AS x RETURN collect(x)",
					fragment: "array_agg(nullif(",
				},
				{
					query:    "MATCH (n) WITH collect(n.x) AS xs UNWIND xs AS x RETURN x + 1",
					fragment: "cypher_arithmetic(",
				},
				{
					query:    "MATCH (n) WITH collect(n.x) AS xs UNWIND xs AS x RETURN x AND true",
					fragment: ")::bool",
				},
				{
					query:    "MATCH (n) WITH collect(n.x) AS xs UNWIND xs AS x RETURN toUpper(x)",
					fragment: "cypher_text(",
				},
				{
					query:    "MATCH (n) WITH collect(n.x) AS xs RETURN collect(xs)",
					fragment: "array_agg(to_jsonb(",
				},
				{
					query:    "WITH [1, 2] AS xs RETURN collect(xs)",
					fragment: "array_agg(to_jsonb(",
				},
				{
					query:    "RETURN collect(true)",
					fragment: "array_agg(to_jsonb(",
				},
				{
					query:    "RETURN collect(null)",
					fragment: "array_agg((null)::jsonb)",
				},
				{
					query:    "RETURN collect([])",
					fragment: "array_agg(to_jsonb(",
				},
				{
					query:    "MATCH (n) RETURN 2 IN collect(n.x)",
					fragment: " = any (",
				},
				{
					query:    "MATCH (n) WITH collect(n.x) AS xs RETURN 2 IN xs",
					fragment: " = any (",
				},
				{
					query:    "MATCH (n) WITH collect(n.x) AS xs MATCH (m) WHERE NOT m.x IN xs RETURN m",
					fragment: " = any (",
				},
				{
					query:    "MATCH (n) WITH collect(n.x) AS xs WITH xs + ['1'] AS ys UNWIND ys AS y RETURN y",
					fragment: "array [to_jsonb(",
				},
				{
					query:    "MATCH (n) WITH [1, 2] AS ys, collect(n.x) AS xs RETURN ys + xs",
					fragment: "unnest(",
				},
			} {
				t.Run(tc.query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{
						OptimizerMode: mode,
					})
					require.NoError(t, err)
					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, tc.fragment)
					require.Contains(t, formatted.Statement, "jsonb[]")
				})
			}
		})
	}
}

// TestPrepareCollectExpressionTypes checks the element types assigned before collection expressions are rendered.
func TestPrepareCollectExpressionTypes(t *testing.T) {
	scope := NewScope()
	for _, tc := range []struct {
		// scalar is the element type supplied to collection type inference.
		scalar pgsql.DataType
		// array is the expected collection type for the scalar input.
		array pgsql.DataType
	}{
		{
			scalar: pgsql.Int8,
			array:  pgsql.Int8Array,
		}, {
			scalar: pgsql.Text,
			array:  pgsql.TextArray,
		},
		{
			scalar: pgsql.Boolean,
			array:  pgsql.JSONBArray,
		}, {
			scalar: pgsql.JSONB,
			array:  pgsql.JSONBArray,
		},
		{
			scalar: pgsql.Int8Array,
			array:  pgsql.JSONBArray,
		}, {
			scalar: pgsql.NodeComposite,
			array:  pgsql.NodeCompositeArray,
		},
		{
			scalar: pgsql.EdgeComposite,
			array:  pgsql.EdgeCompositeArray,
		}, {
			scalar: pgsql.UnknownDataType,
			array:  pgsql.AnyArray,
		},
	} {
		t.Run(string(tc.scalar), func(t *testing.T) {
			binding, err := scope.DefineNew(tc.scalar)
			require.NoError(t, err)
			_, arrayType, err := prepareCollectExpression(scope, binding.Identifier, "collect")
			require.NoError(t, err)
			require.Equal(t, tc.array, arrayType)
		})
	}
}
