package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestTypedCollectTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct{ query, fragment string }{
				{"MATCH (n) RETURN collect(n.x)", "array_agg(nullif("},
				{"MATCH ()-[r]->() RETURN collect(DISTINCT r.x)", "array_agg(distinct nullif("},
				{"MATCH (n) WITH n.x AS x RETURN collect(x)", "array_agg(nullif("},
				{"MATCH (n) WITH collect(n.x) AS xs UNWIND xs AS x RETURN x + 1", "cypher_arithmetic("},
				{"MATCH (n) WITH collect(n.x) AS xs UNWIND xs AS x RETURN x AND true", ")::bool"},
				{"MATCH (n) WITH collect(n.x) AS xs UNWIND xs AS x RETURN toUpper(x)", "cypher_text("},
				{"MATCH (n) WITH collect(n.x) AS xs RETURN collect(xs)", "array_agg(to_jsonb("},
				{"WITH [1, 2] AS xs RETURN collect(xs)", "array_agg(to_jsonb("},
				{"RETURN collect(true)", "array_agg(to_jsonb("},
				{"RETURN collect(null)", "array_agg((null)::jsonb)"},
				{"RETURN collect([])", "array_agg(to_jsonb("},
				{"MATCH (n) RETURN 2 IN collect(n.x)", " = any ("},
				{"MATCH (n) WITH collect(n.x) AS xs RETURN 2 IN xs", " = any ("},
				{"MATCH (n) WITH collect(n.x) AS xs MATCH (m) WHERE NOT m.x IN xs RETURN m", " = any ("},
				{"MATCH (n) WITH collect(n.x) AS xs WITH xs + ['1'] AS ys UNWIND ys AS y RETURN y", "array [to_jsonb("},
				{"MATCH (n) WITH [1, 2] AS ys, collect(n.x) AS xs RETURN ys + xs", "unnest("},
			} {
				t.Run(tc.query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
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

func TestPrepareCollectExpressionTypes(t *testing.T) {
	scope := NewScope()
	for _, tc := range []struct{ scalar, array pgsql.DataType }{
		{pgsql.Int8, pgsql.Int8Array}, {pgsql.Text, pgsql.TextArray},
		{pgsql.Boolean, pgsql.JSONBArray}, {pgsql.JSONB, pgsql.JSONBArray},
		{pgsql.Int8Array, pgsql.JSONBArray}, {pgsql.NodeComposite, pgsql.NodeCompositeArray},
		{pgsql.EdgeComposite, pgsql.EdgeCompositeArray}, {pgsql.UnknownDataType, pgsql.AnyArray},
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
