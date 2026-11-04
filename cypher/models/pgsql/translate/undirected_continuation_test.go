package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestUndirectedContinuationPairsEndpoints(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, query := range []string{
				`MATCH (a)-[:MemberOf]->(b)-[:AdminTo]-(c) RETURN c`,
				`MATCH (a)<-[:MemberOf]-(b)-[:AdminTo]-(c) RETURN count(*) AS c`,
				`MATCH (a)-[:MemberOf]-(b)-[:AdminTo]-(c) RETURN c`,
				`MATCH p = (a)-[:MemberOf]->(b)-[:AdminTo]-(c) RETURN p`,
				`MATCH (a)-[:MemberOf*0..2]->(b)-[:AdminTo]-(c) RETURN c`,
				`MATCH (a)-[:MemberOf]->(b)-[:AdminTo]-(c)-[:AdminTo]->(d) SET c.visited = true RETURN d`,
				`MATCH (a)-[:MemberOf]->(b)-[r:AdminTo]-(c) SET c.visited = true, r.visited = true`,
				`MATCH (a)-[:MemberOf]->(b)-[:AdminTo]-(c) CREATE (:Group {target:c.name})`,
			} {
				t.Run(query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					// The near node can be an ID or a composite in the input
					// frame. Each orientation must pair it with the far node.
					require.Regexp(t, `\(\(?s\d+\.n\d+\)?(?:\.id)? = e1\.start_id and n2\.id = e1\.end_id\) or \(n2\.id = e1\.start_id and \(?s\d+\.n\d+\)?(?:\.id)? = e1\.end_id\)`, formatted.Statement)
				})
			}
		})
	}
}
