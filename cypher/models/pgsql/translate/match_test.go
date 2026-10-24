package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestOptionalMatchIdentityLifetime(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			name          string
			query         string
			optionalCount int
			columns       []string
		}{
			{"leading unwind", "UNWIND [1,1,1] AS x OPTIONAL MATCH (n:Group) RETURN *", 1, []string{"n", "x"}},
			{"pending unwind", "MATCH (n:Group) UNWIND [1,1,1] AS x OPTIONAL MATCH (n)-[r:MemberOf]->(m) RETURN *", 1, []string{"m", "n", "r", "x"}},
			{"chained optional", "MATCH (n:Group) OPTIONAL MATCH (n)-[r:MemberOf]->(m) OPTIONAL MATCH (m)-[s:MemberOf]->(p) RETURN *", 2, []string{"m", "n", "p", "r", "s"}},
			{"with wildcard", "MATCH (n:Group) OPTIONAL MATCH (n)-[r:MemberOf]->(m) WITH * RETURN *", 1, []string{"m", "n", "r"}},
			{"plain match", "MATCH (n:Group) RETURN *", 0, []string{"n"}},
		} {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				require.Equal(t, tc.optionalCount, strings.Count(formatted.Statement, "row_number() over ()"))
				require.Equal(t, tc.optionalCount, strings.Count(formatted.Statement, "as materialized"))
				require.NotContains(t, formatted.Statement, "is not distinct from", "optional correlation must not compare exported values")

				selectBody := translation.Statement.(pgsql.Query).Body.(pgsql.Select)
				var columns []string
				for _, item := range selectBody.Projection {
					projection := item.(*pgsql.AliasedExpression)
					columns = append(columns, projection.Alias.Value.String())
				}
				require.ElementsMatch(t, tc.columns, columns, "internal identities must not escape wildcard projection")
			})
		}
	}
}
