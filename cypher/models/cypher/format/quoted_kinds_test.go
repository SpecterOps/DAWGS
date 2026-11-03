package format_test

import (
	"bytes"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestEmitRawKinds(t *testing.T) {
	for _, strip := range []bool{false, true} {
		for _, tc := range []struct{ raw, token string }{
			{"Base", "Base"}, {"has space", "`has space`"},
			{"has`tick", "`has``tick`"}, {"`Base`", "```Base```"},
		} {
			kind := graph.StringKind(tc.raw)
			pattern := cypher.NewPatternPredicate()
			pattern.AddElement(&cypher.NodePattern{Kinds: graph.Kinds{kind}})
			pattern.AddElement(&cypher.RelationshipPattern{Kinds: graph.Kinds{kind}, Direction: graph.DirectionOutbound})
			pattern.AddElement(&cypher.NodePattern{})
			for _, tc := range []struct {
				expr cypher.Expression
				want string
			}{
				{kind, ":" + tc.token}, {graph.Kinds{kind}, ":" + tc.token},
				{pattern, "(:" + tc.token + ")-[:" + tc.token + "]->()"},
				{cypher.NewKindMatcher(cypher.NewVariableWithSymbol("n"), graph.Kinds{kind}, true), "n:" + tc.token},
			} {
				var out bytes.Buffer
				require.NoError(t, format.NewCypherEmitter(strip).WriteExpression(&out, tc.expr))
				require.Equal(t, tc.want, out.String())
			}
		}
		for _, source := range []string{
			"MATCH (n:`Base`:`has space`)<-[r:`has``tick`|```Base```*1..2]-(m) WHERE NOT n:`a-b` RETURN n, r",
			"MATCH (n:`Base`) SET n:`has space` REMOVE n:`has``tick` RETURN n",
			"CREATE (n:```Base```)-[:`has space`]->(m:`1leading`) RETURN n",
		} {
			parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
			require.NoError(t, err)
			canonical, err := format.RegularQuery(parsed, strip)
			require.NoError(t, err)
			reparsed, err := frontend.ParseCypher(frontend.NewContext(), canonical)
			require.NoError(t, err)
			require.Equal(t, parsed, reparsed)
			again, err := format.RegularQuery(reparsed, strip)
			require.NoError(t, err)
			require.Equal(t, canonical, again)
		}
	}
}
