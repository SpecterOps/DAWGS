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

// A terminal MATCH's correlated predicate must select mutation input before any
// write CTE runs, rather than surviving as an out-of-scope final RETURN filter.
func TestPatternPredicateFiltersBeforeMutation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, mutation := range []string{`SET b.selected = true`, `SET c.selected = true`, `REMOVE b.name`, `CREATE (:DependencyCreated {name: b.name})`} {
			t.Run(string(mode)+"/"+mutation, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				for _, k := range []string{"DependencySource", "DependencyR", "DependencyCreated"} {
					mapper.Put(graph.StringKind(k))
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (b:DependencySource) MATCH (c {name: 'target'}) WHERE (b)-[:DependencyR]->(c) `+mutation+` RETURN b.name, c.name`)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				statement := formatted.Statement
				predicate := strings.Index(statement, "select count(*) > 0")
				require.GreaterOrEqual(t, predicate, 0, statement)
				write := strings.Index(statement, "update node")
				if strings.HasPrefix(mutation, "CREATE") {
					write = strings.Index(statement, "insert into node")
				}
				require.Greater(t, write, predicate, statement)
				// No deferred filter should remain after the final input source.
				finalFrom := strings.LastIndex(statement, " from ")
				require.NotContains(t, statement[finalFrom:], " where ", statement)
			})
		}
	}
}
