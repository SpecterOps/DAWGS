package cypher_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestKindNameCodec(t *testing.T) {
	for _, tc := range []struct{ raw, token string }{
		{"Base", "Base"}, {"MiXeD", "MiXeD"}, {"MATCH", "MATCH"},
		{"has space", "`has space`"}, {"   ", "`   `"}, {"a-b.!", "`a-b.!`"},
		{"1leading", "`1leading`"}, {"日本語", "日本語"}, {"e\u0301", "e\u0301"},
		{"has`tick", "`has``tick`"}, {"a``b", "`a````b`"}, {"`Base`", "```Base```"},
		{"x`) MATCH (n) //", "`x``) MATCH (n) //`"}, {"", "``"},
	} {
		t.Run(tc.token, func(t *testing.T) {
			require.Equal(t, tc.token, cypher.EscapeKindName(tc.raw))
			require.Equal(t, tc.raw, cypher.UnescapeKindName(tc.token))
			require.Equal(t, tc.raw, cypher.UnescapeKindName(cypher.EscapeKindName(tc.raw)))
			query, err := frontend.ParseCypher(frontend.NewContext(), "MATCH (n:"+tc.token+")-[r:"+tc.token+"]->() WHERE n:"+tc.token+" RETURN n")
			require.NoError(t, err)
			var names []string
			require.NoError(t, walk.CypherStructural(query, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
				if kinds, ok := node.(graph.Kinds); ok {
					for _, kind := range kinds {
						names = append(names, kind.String())
					}
				}
			})))
			require.Equal(t, []string{tc.raw, tc.raw, tc.raw}, names)
			for _, strip := range []bool{false, true} {
				canonical, err := format.RegularQuery(query, strip)
				require.NoError(t, err)
				reparsed, err := frontend.ParseCypher(frontend.NewContext(), canonical)
				require.NoError(t, err)
				require.Equal(t, query, reparsed)
			}
		})
	}
	require.Equal(t, "Base", cypher.UnescapeKindName("`Base`"))
	require.Equal(t, "MiXeD", cypher.UnescapeKindName("MiXeD"))
}
