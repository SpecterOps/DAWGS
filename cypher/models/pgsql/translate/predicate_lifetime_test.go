package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPatternPredicateRetainsOriginalMatchFrame(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, predicate := range []string{`(s)-[:T]->()`, `NOT (s)-[:T]->()`, `(s)-[:T]-()`, `(s)-[:T]->()-[:U]->()`} {
			t.Run(string(mode)+"/"+predicate, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				for _, kind := range []string{"A", "T", "R", "U"} {
					mapper.Put(graph.StringKind(kind))
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (s:A) WHERE `+predicate+` MATCH (s)-[:R]->(t) RETURN s,t`)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				// The predicate must retain T's constraint after the next MATCH is rendered.
				require.Contains(t, formatted.Statement, "kind_id = any (array [2]::int2[])", formatted.Statement)
				require.Equal(t, 1, strings.Count(formatted.Statement, "array [2]::int2[]"), formatted.Statement)
				require.Contains(t, formatted.Statement, "kind_id = any (array [3]::int2[])")
			})
		}
	}
}
