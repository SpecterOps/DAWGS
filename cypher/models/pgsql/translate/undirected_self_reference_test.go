package translate

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestUndirectedSelfReferenceRoot(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query, node string
			}{
				{"root", `MATCH (x)-[r:MemberOf]-(x) RETURN x`, "n0"},
				{"right constraints", `MATCH (x)-[r:MemberOf]-(x:Group {name:'loop'}) RETURN x`, "n0"},
				{"path", `MATCH p = (x)-[r:MemberOf]-(x) RETURN p`, "n0"},
				{"input duplicates", `UNWIND [1, 1] AS row WITH row MATCH (x)-[r:MemberOf]-(x) RETURN row, x`, "n0"},
				{"unrelated input", `MATCH (seed:Group) MATCH (x)-[r:MemberOf]-(x) RETURN seed, x`, "n1"},
				{"optional", `MATCH (seed:Group) OPTIONAL MATCH (x)-[r:MemberOf]-(x) RETURN seed, x`, "n1"},
				{"updates", `MATCH (x)-[r:MemberOf]-(x) SET x.visited = true, r.visited = true RETURN x`, "n0"},
				{"create", `MATCH (x)-[r:MemberOf]-(x) CREATE (:Group {target:x.name})`, "n0"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, fmt.Sprintf("%s.id = e0.start_id and %s.id = e0.end_id", tc.node, tc.node))
					require.NotContains(t, formatted.Statement, fmt.Sprintf("%s.id = e0.start_id or %s.id = e0.end_id", tc.node, tc.node))
				})
			}
		})
	}
}
