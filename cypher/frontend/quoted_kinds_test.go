package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestParsedKindNames(t *testing.T) {
	for _, name := range []struct{ token, raw string }{
		{"Base", "Base"}, {"`Base`", "Base"}, {"`has space`", "has space"},
		{"`has``tick`", "has`tick"}, {"```Base```", "`Base`"}, {"``", ""},
	} {
		for _, source := range []string{
			"MATCH (n:%s) RETURN n", "MATCH ()-[r:%s]->() RETURN r",
			"MATCH (n) WHERE NOT n:%s RETURN n", "MATCH (n) SET n:%s RETURN n",
			"MATCH (n) REMOVE n:%s RETURN n", "CREATE (n:%s) RETURN n",
			"CREATE ()-[r:%s]->() RETURN r", "MERGE (n:%s) RETURN n",
		} {
			source := fmt.Sprintf(source, name.token)
			t.Run(source, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
				require.NoError(t, err)
				var kinds graph.Kinds
				require.NoError(t, walk.CypherStructural(parsed, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
					switch node := node.(type) {
					case graph.Kinds:
						kinds = append(kinds, node...)
					}
				})))
				require.Equal(t, graph.Kinds{graph.StringKind(name.raw)}, kinds)
			})
		}
	}
}
