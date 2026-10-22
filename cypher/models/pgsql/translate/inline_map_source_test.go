package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestInlineMapFunctionsCarryTheirSource(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			query, sourceReference string
		}{
			{`MATCH (s)-[:R]->(m)-[:R]->(e {x: id(s)}) RETURN e`, `\(s\d+\.n0\)\.id`},
			{`MATCH (s)-[:R]->(m)-[:R]->(e {x: labels(s)}) RETURN e`, `\(s\d+\.n0\)\.kind_ids`},
			{`MATCH (s)-[:R]->(m)-[:R {x: id(s)}]->(e) RETURN e`, `\(s\d+\.n0\)\.id`},
			{`MATCH (s)-[:R]->(m)-[:R {x: labels(s)}]->(e) RETURN e`, `\(s\d+\.n0\)\.kind_ids`},
			{`MATCH (s)-[r:R]->(m)-[:R {x: id(r)}]->(e) RETURN e`, `\(s\d+\.e0\)\.id`},
			{`MATCH (s)-[r:R]->(m)-[:R]->(e {x: type(r)}) RETURN e`, `\(s\d+\.e0\)\.kind_id`},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				kinds := pgutil.NewInMemoryKindMapper()
				kinds.Put(graph.StringKind("R"))
				translation, err := TranslateWithOptions(context.Background(), query, kinds, nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				require.Regexp(t, tc.sourceReference, formatted.Statement)
			})
		}
	}
}
