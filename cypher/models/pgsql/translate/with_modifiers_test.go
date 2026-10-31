package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyWithModifiers(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			query string
			order int
			skip  bool
			limit bool
		}{
			{query: `MATCH (n:User) WITH n ORDER BY n.x DESC RETURN n`, order: 1},
			{query: `MATCH (n:User) WITH n SKIP 1 RETURN n`, skip: true},
			{query: `MATCH (n:User) WITH n LIMIT 2 RETURN n`, limit: true},
			{query: `MATCH (n:User) WITH n ORDER BY n.x DESC SKIP 1 LIMIT 2 RETURN n.x`, order: 1, skip: true, limit: true},
			{query: `MATCH (n:User) WITH n AS item ORDER BY item.x DESC LIMIT 2 RETURN item`, order: 1, limit: true},
			{query: `MATCH (n:User) WITH n.x AS value ORDER BY value DESC LIMIT 2 RETURN value`, order: 1, limit: true},
			{query: `MATCH (n:User) WITH n.x AS value ORDER BY value + 1 DESC LIMIT 2 RETURN value`, order: 1, limit: true},
			{query: `MATCH (n:User) WITH DISTINCT n.x AS value ORDER BY value DESC LIMIT 2 RETURN value`, order: 1, limit: true},
			{query: `MATCH (n:User) WITH n.x AS value, count(n) AS total ORDER BY total DESC, value LIMIT 2 RETURN value, total`, order: 2, limit: true},
			{query: `UNWIND [3, 1, 2] AS x WITH x ORDER BY x SKIP 1 LIMIT 2 RETURN x`, order: 1, skip: true, limit: true},
			{query: `MATCH (n:User) WITH n ORDER BY n.x DESC LIMIT 2 SET n.seen = true RETURN n`, order: 1, limit: true},
			{query: `MATCH (n:User) WITH n ORDER BY n.x DESC LIMIT 2 CREATE (m:User {x: n.x}) RETURN m`, order: 1, limit: true},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				require.Empty(t, translation.Stages)
				query := translation.Statement.(pgsql.Query)
				boundary := query.CommonTableExpressions.Expressions[0]
				require.Len(t, boundary.Query.OrderBy, tc.order)
				require.Equal(t, tc.skip, boundary.Query.Offset != nil)
				require.Equal(t, tc.limit, boundary.Query.Limit != nil)
				require.Empty(t, query.OrderBy, "WITH ordering must stay on its boundary")
				require.Nil(t, query.Offset)
				require.Nil(t, query.Limit)
				for _, item := range boundary.Query.OrderBy {
					formatted, err := format.Expression(item.Expression, format.NewOutputBuilder())
					require.NoError(t, err)
					require.NotContains(t, formatted.Statement, boundary.Alias.Name.String()+".", "ordering must use the boundary's input expressions")
				}
			})
		}
	}
}

func TestReadOnlyWithWhereFollowsPagination(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, projection := range []string{"n", "n.x AS value"} {
			predicate := "n.x < 3"
			if projection != "n" {
				predicate = "value < 3"
			}
			queryText := `MATCH (n:User) WHERE n.x > 0 WITH ` + projection + ` ORDER BY n.x DESC LIMIT 1 WHERE ` + predicate + ` RETURN count(*)`
			t.Run(string(mode)+"/"+projection, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), queryText)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				boundary := translation.Statement.(pgsql.Query).CommonTableExpressions.Expressions[0].Query
				require.Nil(t, boundary.Limit, "WITH WHERE filters the paginated projection")
				require.NotNil(t, boundary.Body.(pgsql.Select).Where)
				ctes := boundary.CommonTableExpressions.Expressions
				paginated := ctes[len(ctes)-1].Query
				require.NotNil(t, paginated.Limit)
				require.Len(t, paginated.OrderBy, 1)
				formatted, err := format.Expression(boundary, format.NewOutputBuilder())
				require.NoError(t, err)
				require.Contains(t, formatted.Statement, " > ", "MATCH's input predicate must remain before pagination")
			})
		}
	}
}

func TestReadOnlyWithIndependentPages(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		parsed, err := frontend.ParseCypher(frontend.NewContext(), `UNWIND [5, 1, 4, 2, 3] AS x WITH x ORDER BY x DESC LIMIT 4 WITH x ORDER BY x SKIP 1 LIMIT 2 RETURN x ORDER BY x DESC LIMIT 1`)
		require.NoError(t, err)
		translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
		require.NoError(t, err)
		query := translation.Statement.(pgsql.Query)
		require.Len(t, query.CommonTableExpressions.Expressions, 2)
		for index, boundary := range query.CommonTableExpressions.Expressions {
			require.Len(t, boundary.Query.OrderBy, 1)
			require.Equal(t, index == 1, boundary.Query.OrderBy[0].Ascending)
			require.Equal(t, index == 1, boundary.Query.Offset != nil)
			require.Equal(t, []any{int64(4), int64(2)}[index], boundary.Query.Limit.(pgsql.Literal).Value)
		}
		require.Len(t, query.OrderBy, 1)
		require.False(t, query.OrderBy[0].Ascending)
		require.Equal(t, int64(1), query.Limit.(pgsql.Literal).Value)
		require.Nil(t, query.Offset)
	}
}

func TestReadOnlyWithLiteralPagePreservesRows(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, input := range []string{`MATCH (n:User)`, `MATCH (n:User) WHERE n.x < 0`} {
			parsed, err := frontend.ParseCypher(frontend.NewContext(), input+` WITH 1 AS value LIMIT 2 RETURN count(value)`)
			require.NoError(t, err)
			translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
			require.NoError(t, err)
			boundary := translation.Statement.(pgsql.Query).CommonTableExpressions.Expressions[0].Query
			require.Len(t, boundary.Body.(pgsql.Select).From, 1, "constant WITH projections must paginate their input rows")
		}
	}
}
