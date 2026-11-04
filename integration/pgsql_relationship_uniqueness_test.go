//go:build manual_integration

package integration

import (
	"fmt"
	"maps"
	"sort"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLRelationshipUniquenessCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "relationship_uniqueness_shapes")
}

// The two-way star admits revisiting nodes through distinct relationships. Its
// closed-form counts check multiplicity without enumerating translated paths.
// EXPLAIN timings are diagnostic; host-dependent latency is not a test threshold.
func TestPostgreSQLRelationshipUniquenessFanout(t *testing.T) {
	session := Open(t, Options{
		RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		CleanupMode:    CleanupGraph,
		ExtraNodeKinds: graph.Kinds{graph.StringKind("UniquenessFanoutNode")},
		ExtraEdgeKinds: graph.Kinds{graph.StringKind("UniquenessFanoutEdge")},
	})
	driver := session.DB.(*pg.Driver)
	defaultGraph, ok := driver.DefaultGraph()
	require.True(t, ok)
	for _, fanout := range []int{8, 32} {
		fixture := &opengraph.Graph{Nodes: []opengraph.Node{{ID: "root", Kinds: []string{"UniquenessFanoutNode"}}}}
		for index := range fanout {
			id := fmt.Sprintf("leaf-%d", index)
			fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: id, Kinds: []string{"UniquenessFanoutNode"}})
			fixture.Edges = append(fixture.Edges,
				opengraph.Edge{StartID: "root", EndID: id, Kind: "UniquenessFanoutEdge"},
				opengraph.Edge{StartID: id, EndID: "root", Kind: "UniquenessFanoutEdge"})
		}
		for _, tc := range []struct {
			name, pattern string
			expected      int64
		}{
			{"comma_fixed", `(a)-[:UniquenessFanoutEdge]->(), (a)-[:UniquenessFanoutEdge]->()`, int64(fanout * (fanout - 1))},
			{"consecutive_expansions", `(a)-[:UniquenessFanoutEdge*1..2]->()-[:UniquenessFanoutEdge*1..2]->()`, int64(3*fanout*fanout - 2*fanout)},
			{"comma_expansions", `(a)-[:UniquenessFanoutEdge*1..2]->(), (a)-[:UniquenessFanoutEdge*1..2]->()`, int64(4 * fanout * (fanout - 1))},
		} {
			for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
				t.Run(fmt.Sprintf("%d/%s/%s", fanout, tc.name, mode), func(t *testing.T) {
					require.NoError(t, session.WithRollbackFixture(t, fixture, false, func(tx graph.Transaction, ids opengraph.IDMap) error {
						parsed, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (a:UniquenessFanoutNode) WHERE id(a) = $root MATCH `+tc.pattern+` RETURN count(*) AS c`)
						require.NoError(t, err)
						translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), map[string]any{"root": ids["root"].Int64()}, defaultGraph.ID, translate.Options{OptimizerMode: mode})
						require.NoError(t, err)
						sql, err := translate.Translated(translation)
						require.NoError(t, err)
						maps.Copy(translation.Parameters, sql.Parameters)
						var planning, execution []float64
						for sample := range 6 {
							result := tx.Raw("EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) "+sql.Statement, translation.Parameters)
							require.True(t, result.Next(), result.Error())
							plan, err := normalizeExplainPlan(result.Values()[0])
							require.NoError(t, err)
							result.Close()
							require.NoError(t, result.Error())
							if sample > 0 {
								summary := plan.([]any)[0].(map[string]any)
								planning = append(planning, summary["Planning Time"].(float64))
								execution = append(execution, summary["Execution Time"].(float64))
							}
						}
						sort.Float64s(planning)
						sort.Float64s(execution)
						t.Logf("median of 5 warm EXPLAIN runs: planning %.3f ms, execution %.3f ms", planning[2], execution[2])
						result := tx.Raw(sql.Statement, translation.Parameters)
						defer result.Close()
						require.True(t, result.Next(), result.Error())
						require.Equal(t, tc.expected, result.Values()[0])
						require.False(t, result.Next())
						return result.Error()
					}))
				})
			}
		}
	}
}
