package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

func TestTailOrderAliasExpressions(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct{ query, order string }{
			{`MATCH (n:User) RETURN n.k AS x ORDER BY x + 0`, `properties`},
			{`UNWIND [1,2,3] AS x RETURN 10-x AS i0 ORDER BY x`, `i0.i0`},
			{`UNWIND [1,2,3] AS x RETURN 10-x AS i0 ORDER BY ((x))`, `i0.i0`},
			{`UNWIND [1,2,3] AS x RETURN 10-x AS i0 ORDER BY i0 + 0`, `10 - i0`},
			{`UNWIND [1,2,3] AS x RETURN 10-x AS i0 ORDER BY -i0`, `(10 - i0)`},
			{`UNWIND [1,2,3] AS x RETURN 10-x AS i0 ORDER BY toString(i0)`, `(10 - i0)`},
			{`MATCH (n:User) RETURN sum(n.k) AS x ORDER BY x + 0`, `sum(`},
			{`MATCH (n:User) RETURN n.k AS s0 ORDER BY coalesce(s0, 0)`, `properties`},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				translated, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				query := translated.Statement.(pgsql.Query)
				require.Len(t, query.OrderBy, 1)
				formatted, err := format.Expression(query.OrderBy[0].Expression, format.NewOutputBuilder())
				require.NoError(t, err)
				require.Contains(t, formatted.Statement, tc.order)
			})
		}
	}
}

func TestDistinctTailOrderAliasesPreserveProjectionAndPage(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, text := range []string{
			`UNWIND [1,1,2] AS x RETURN DISTINCT 10-x AS i2 ORDER BY i2 + 0 DESC SKIP 1 LIMIT 1`,
			`UNWIND [1,1,2] AS x WITH x RETURN DISTINCT x AS value ORDER BY value + 0 SKIP 1 LIMIT 1`,
			`MATCH (n:User) RETURN DISTINCT n.k AS x ORDER BY x + 0 SKIP 1 LIMIT 1`,
		} {
			t.Run(string(mode)+"/"+text, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), text)
				require.NoError(t, err)
				translated, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				query := translated.Statement.(pgsql.Query)
				require.NotNil(t, query.Offset)
				require.NotNil(t, query.Limit)
				outer := query.Body.(pgsql.Select)
				require.False(t, outer.Distinct)
				require.Len(t, outer.Projection, 1, "hidden sort keys must not become result columns")
				source := outer.From[0].Source.(pgsql.AliasedExpression)
				inner := source.Expression.(*pgsql.Parenthetical).Expression.(pgsql.Query)
				require.Nil(t, inner.Offset)
				require.Nil(t, inner.Limit)
				projection := inner.Body.(pgsql.Select)
				require.True(t, projection.Distinct)
				require.Len(t, projection.Projection, 2)
				value := projection.Projection[0].(*pgsql.AliasedExpression)
				key := projection.Projection[1].(*pgsql.AliasedExpression)
				require.NotEqual(t, value.Alias.Value, key.Alias.Value, "internal keys must avoid user aliases")
				require.Equal(t, pgsql.CompoundIdentifier{source.Alias.Value, key.Alias.Value}, query.OrderBy[0].Expression)
			})
		}
	}
}
