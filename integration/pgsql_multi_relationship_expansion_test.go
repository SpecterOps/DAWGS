//go:build manual_integration

package integration

import (
	"fmt"
	"maps"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLMultiRelationshipExpansionCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "multi_relationship_expansion_shapes")
}

// Check admitted recursive rows, independently of wall-clock speed and chosen
// indexes. Unrelated components and wrong types must never enter the frontier.
func TestPostgreSQLMultiRelationshipExpansionPlan(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		CleanupMode: CloseOnly, ExtraNodeKinds: graph.Kinds{graph.StringKind("MultiExpansionNode")},
		ExtraEdgeKinds: graph.StringsToKinds([]string{"Okta_MemberOf", "Okta_HasRoleAssignment", "MultiExpansionOther"})})
	driver := session.DB.(*pg.Driver)
	defaultGraph, ok := driver.DefaultGraph()
	require.True(t, ok)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		for _, depth := range []int{2, 3} {
			for _, disconnected := range []int{0, 256} {
				t.Run(fmt.Sprintf("%s/depth=%d/disconnected=%d", mode, depth, disconnected), func(t *testing.T) {
					fixture := multiExpansionFixture(multiExpansionDomains[0], 8, disconnected)
					require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
						query := multiExpansionQuery([]string{fmt.Sprintf("Okta_MemberOf|Okta_HasRoleAssignment*1..%d", depth)}, false, "count(*)")
						parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
						require.NoError(t, err)
						translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), map[string]any{"root": ids["root"].Int64()}, defaultGraph.ID, translate.Options{OptimizerMode: mode})
						require.NoError(t, err)
						sql, err := translate.Translated(translation)
						require.NoError(t, err)
						maps.Copy(translation.Parameters, sql.Parameters)
						result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) "+sql.Statement, translation.Parameters)
						require.True(t, result.Next(), "%v", result.Error())
						plan, err := normalizeExplainPlan(result.Values()[0])
						require.NoError(t, err)
						require.False(t, result.Next())
						result.Close()
						require.NoError(t, result.Error())
						summary := plan.([]any)[0].(map[string]any)
						var recursiveRows []int
						var visit func(any)
						visit = func(value any) {
							switch node := value.(type) {
							case map[string]any:
								if node["Node Type"] == "Recursive Union" {
									require.EqualValues(t, 1, node["Actual Loops"])
									rows, ok := asInt64(node["Actual Rows"])
									require.True(t, ok, "unexpected row count %T", node["Actual Rows"])
									recursiveRows = append(recursiveRows, int(rows))
								}
								for _, child := range node {
									visit(child)
								}
							case []any:
								for _, child := range node {
									visit(child)
								}
							}
						}
						visit(summary["Plan"])
						require.Equal(t, []int{multiExpansionCount(16, depth)}, recursiveRows)
						require.Equal(t, int64(multiExpansionCount(16, depth)), multiExpansionScalar(t, tx.Raw(sql.Statement, translation.Parameters)))
						t.Logf("frontier rows=%v; planning=%v ms execution=%v ms", recursiveRows, summary["Planning Time"], summary["Execution Time"])
						return nil
					}))
				})
			}
		}
	}
}
