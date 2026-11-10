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
	for _, tc := range []struct {
		// raw is the decoded kind name without Cypher quoting.
		raw string
		// token is the Cypher spelling, including any backtick quoting.
		token string
	}{
		{raw: "Base", token: "Base"}, {raw: "MiXeD", token: "MiXeD"}, {raw: "MATCH", token: "MATCH"},
		{raw: "has space", token: "`has space`"}, {raw: "   ", token: "`   `"}, {raw: "a-b.!", token: "`a-b.!`"},
		{raw: "1leading", token: "`1leading`"}, {raw: "日本語", token: "日本語"}, {raw: "e\u0301", token: "e\u0301"},
		{raw: "has`tick", token: "`has``tick`"}, {raw: "a``b", token: "`a````b`"}, {raw: "`Base`", token: "```Base```"},
		{raw: "x`) MATCH (n) //", token: "`x``) MATCH (n) //`"}, {raw: "", token: "``"},
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
