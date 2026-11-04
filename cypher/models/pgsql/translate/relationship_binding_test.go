package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestReusedRelationshipScanPreservesIdentity(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, query := range []string{
				`MATCH (a)-[r:MemberOf]->(b) MATCH (a)-[r]->(c) RETURN count(*) AS c`,
				`MATCH (a)-[r:MemberOf]->(b) MATCH (a)-[r]->(b) RETURN r`,
				`MATCH (a)-[r:MemberOf]->(b) WITH r MATCH (c)-[r]->(d) RETURN c, d`,
				`MATCH (a)-[r:MemberOf]->(b) MATCH (c)<-[r]-(a) RETURN c`,
				`MATCH (a)-[r:MemberOf]->(b) MATCH (a)-[r]-(c) RETURN c`,
				`MATCH (a)-[r:MemberOf]->(b) MATCH p = (a)-[r]->(c) RETURN p`,
				`MATCH (a)-[r:MemberOf]->(b) WITH a, r AS carried MATCH (a)-[carried]->(c) RETURN c`,
				`MATCH (a)-[r:MemberOf]->(b) MATCH (c)<-[:AdminTo]-(d)<-[r]-(e) RETURN e`,
				`MATCH (a)-[r:MemberOf]->(b) WHERE (a)-[r]->(b) RETURN count(*) AS c`,
				`MATCH (a)-[r:MemberOf]->(b) OPTIONAL MATCH (a)-[r]->(c) SET c.visited = true RETURN c`,
				`MATCH (a)-[r:MemberOf]->(b) MATCH (a)-[r]->(c) CREATE (audit:Group {target:c.name})`,
			} {
				t.Run(query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					// The new scan must constrain the physical relationship ID to
					// the original composite, including across WITH and OPTIONAL.
					require.Regexp(t, `e[1-9][0-9]*\.id = \(s[0-9]+\.e[0-9]+\)\.id`, formatted.Statement)
				})
			}
		})
	}
}
