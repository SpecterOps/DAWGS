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
			{"leading node", "OPTIONAL MATCH (n:Group) RETURN *", 1, []string{"n"}},
			{"leading traversal", "OPTIONAL MATCH (n:Group)-[r:MemberOf]->(m) RETURN *", 1, []string{"m", "n", "r"}},
			{"leading path", "OPTIONAL MATCH p = (n:Group)-[r:MemberOf]->(m) RETURN *", 1, []string{"m", "n", "p", "r"}},
			{"leading chained optional", "OPTIONAL MATCH (n:Group) OPTIONAL MATCH (n)-[r:MemberOf]->(m) RETURN *", 2, []string{"m", "n", "r"}},
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

func TestLeadingOptionalMatchSeed(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, queryText := range []string{
			`OPTIONAL MATCH (n) RETURN n`,
			`OPTIONAL MATCH (n:Group) WHERE n.name = 'missing' RETURN n, n.name`,
			`OPTIONAL MATCH (n:Group)-[r:MemberOf]->(m) WHERE r.name = 'missing' RETURN n, r, m`,
			`OPTIONAL MATCH (n:Group)<-[r:MemberOf]-(m) RETURN n, r, m`,
			`OPTIONAL MATCH (n:Group)-[r:MemberOf]-(m) RETURN n, r, m`,
			`OPTIONAL MATCH (n:Group)-[:MemberOf*1..2]->(m) RETURN n, m`,
			`OPTIONAL MATCH (n:Group)-[:MemberOf*0..0]->(m) RETURN n, m`,
			`OPTIONAL MATCH (n:Group), (m:User) WHERE m.name = 'missing' RETURN n, m`,
			`OPTIONAL MATCH p = (n:Group)-[:MemberOf]->(m) RETURN p`,
			`OPTIONAL MATCH (n:Group) WHERE (n)-[:MemberOf]->() RETURN n`,
			`OPTIONAL MATCH (n:Group) RETURN count(*), count(n), collect(n)`,
		} {
			t.Run(string(mode)+"/"+queryText, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), queryText)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				query := translation.Statement.(pgsql.Query)
				seed := query.CommonTableExpressions.Expressions[0]
				require.NotNil(t, seed.Materialized, "the implicit input row must be shared with the optional join")
				require.True(t, seed.Materialized.Materialized)
				selectBody := seed.Query.Body.(pgsql.Select)
				require.Empty(t, selectBody.From, "the implicit input must exist independently of graph contents")
				require.Nil(t, selectBody.Where, "optional predicates must not filter the seed")
				require.Len(t, selectBody.Projection, 1)
				require.Contains(t, formatted.Statement, "left outer join")
				require.Nil(t, query.Body.(pgsql.Select).Where, "optional predicates must not filter the null-extended row")
			})
		}
	}
}

func TestMultipartOptionalMatchInput(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			query    string
			part     int
			fromSize int
		}{
			{`OPTIONAL MATCH (n:Group) WITH n AS item RETURN item, item.name`, 0, 0},
			{`UNWIND [1,1] AS x OPTIONAL MATCH (n:Group) WITH x, n AS item RETURN x, item.name`, 0, 1},
			{`WITH 1 AS x OPTIONAL MATCH (n:Group) WITH x, n AS item RETURN x, item.name`, 1, 1},
			{`WITH 1 AS x UNWIND [2,2] AS y OPTIONAL MATCH (n:Group) WITH x, y, n AS item RETURN x, y, item.name`, 1, 2},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				query := translation.Statement.(pgsql.Query)
				boundary := query.CommonTableExpressions.Expressions[tc.part]
				seed := boundary.Query.CommonTableExpressions.Expressions[0]
				require.NotNil(t, seed.Materialized)
				from := seed.Query.Body.(pgsql.Select).From
				require.Len(t, from, tc.fromSize)
				if tc.part > 0 {
					source := from[0].Source.(pgsql.TableReference)
					require.Equal(t, query.CommonTableExpressions.Expressions[tc.part-1].Alias.Name, source.Name.Root(), "read the preceding boundary")
				}
			})
		}
	}
}
