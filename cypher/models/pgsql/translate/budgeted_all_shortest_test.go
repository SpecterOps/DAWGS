package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestUsesBudgetedAllShortestPathsRequiresAllShortestPattern verifies dispatch ignores ordinary
// queries and shortestPath-only patterns.
func TestUsesBudgetedAllShortestPathsRequiresAllShortestPattern(t *testing.T) {
	require.False(t, UsesBudgetedAllShortestPaths(nil))

	for _, source := range []string{
		"MATCH (n) RETURN n",
		"RETURN 'allShortestPaths' AS name",
		"MATCH p=shortestPath((s)-[*1..]->(e)) WHERE id(s)=1 AND id(e)=2 RETURN p",
	} {
		t.Run(source, func(t *testing.T) {
			parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
			require.NoError(t, err)
			require.False(t, UsesBudgetedAllShortestPaths(parsed))
		})
	}
}

// TestBudgetedAllShortestResolvesPropertyAndIDEndpoints verifies equivalent endpoint forms and
// maximum depths select the same budgeted architecture.
func TestBudgetedAllShortestResolvesPropertyAndIDEndpoints(t *testing.T) {
	for _, endpoints := range []string{
		"s.name = $source AND e.objectid = $target",
		"id(s) = $start_id AND id(e) = $end_id",
		"id(s) = $start_id AND id(e) = $end_id AND s.name = $source AND e.objectid = $target",
	} {
		for _, maximum := range []string{"", "64", "128"} {
			t.Run(endpoints+"/"+maximum, func(t *testing.T) {
				query := fmt.Sprintf("MATCH p=allShortestPaths((s)-[*1..%s]->(e)) WHERE %s RETURN p", maximum, endpoints)
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				require.True(t, UsesBudgetedAllShortestPaths(parsed))

				translation := optimizerSafetyTranslationWithParameters(t, query, map[string]any{
					"source": "source@example.test", "target": "example-target", "start_id": int64(1), "end_id": int64(2),
				})
				formatted, err := Translated(translation)
				require.NoError(t, err)

				sql := formatted.Statement
				require.Contains(t, sql, "all_shortest_paths_dag_v2")
				require.Contains(t, sql, "asp_root_endpoints(root_id, admitted) as materialized")
				require.Contains(t, sql, "asp_terminal_endpoints(terminal_id, admitted) as materialized")
				require.Contains(t, sql, "dawgs_admit_traversal_work_v2(1, 64,")
				require.NotContains(t, sql, "asp_harness")
				require.NotContains(t, sql, "limit 33")
				require.NotContains(t, sql, "singleton_endpoints")
				if maximum == "" {
					require.Contains(t, sql, "asp_resolved_endpoints.terminal_id, 1, 0, false,")

					executor := translation.Optimization.LoweringPlan.ShortestPathExecutor[0]
					require.Zero(t, executor.MaximumDepth)
					require.Equal(t, optimize.ShortestPathMaximumDepthUnbounded, executor.MaximumDepthSource)
				} else {
					require.Contains(t, sql, "asp_resolved_endpoints.terminal_id, 1, "+maximum+", true,")
					require.Equal(t, optimize.ShortestPathMaximumDepthExplicit,
						translation.Optimization.LoweringPlan.ShortestPathExecutor[0].MaximumDepthSource)
				}
				decision := translation.Optimization.LoweringPlan.EndpointResolution[0]
				require.Equal(t, optimize.EndpointResolutionPlanBudgeted, decision.SelectedPlan)
				require.Zero(t, decision.Root.Limit)
				require.Zero(t, decision.Terminal.Limit)
				require.Empty(t, translation.Optimization.LoweringPlan.ShortestPathExecutor[0].FallbackExecutor)
				requireOptimizationLowering(t, translation.Optimization, optimize.LoweringEndpointResolution)
			})
		}
	}
}

// TestBudgetedAllShortestRetainsSemanticBoundaries verifies predicates and clauses outside the
// compact contract remain on existing routes.
func TestBudgetedAllShortestRetainsSemanticBoundaries(t *testing.T) {
	for _, query := range []string{
		"OPTIONAL MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name=$source AND e.name=$target RETURN p",
		"MATCH p=allShortestPaths((s)-[*0..]->(e)) WHERE s.name=$source AND e.name=$target RETURN p",
		"MATCH p=allShortestPaths((s)-[*2..]->(e)) WHERE s.name=$source AND e.name=$target RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]-(e)) WHERE s.name=$source AND e.name=$target RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name=$source AND e.name=$target AND s.flag=e.flag RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name=$source AND e.name=$target AND length(p)>1 RETURN p",
		"MATCH p=allShortestPaths((s)-[r*1..]->(e)) WHERE s.name=$source AND e.name=$target RETURN p,r",
		"WITH 'source' AS name MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name=name AND e.name=$target RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name=$source AND e.name=$target SET s.visited=true RETURN p",
	} {
		t.Run(query, func(t *testing.T) {
			parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
			require.NoError(t, err)
			require.False(t, UsesBudgetedAllShortestPaths(parsed))
		})
	}
}

// TestBudgetedAllShortestSnapshotCompatibilityControl verifies callers can retain legacy lowering
// when a stable snapshot is unavailable.
func TestBudgetedAllShortestSnapshotCompatibilityControl(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), "MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name=$source AND e.name=$target RETURN p")
	require.NoError(t, err)

	translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), map[string]any{"source": "source", "target": "target"}, DefaultGraphID, Options{
		DisableBudgetedAllShortestPaths: true,
	})
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted.Statement, "unidirectional_asp_harness_v2")
	require.NotContains(t, formatted.Statement, "all_shortest_paths_dag_v2")

	for _, lowering := range translation.Optimization.Lowerings {
		require.NotEqual(t, optimize.LoweringEndpointResolution, lowering.Name)
	}
}

// TestBudgetedAllShortestKeepsEndpointPredicatesBeforeAdmission verifies graph membership, labels,
// and property types remain in endpoint resolution.
func TestBudgetedAllShortestKeepsEndpointPredicatesBeforeAdmission(t *testing.T) {
	translation := optimizerSafetyTranslationWithParameters(t,
		"MATCH p=allShortestPaths((s:User {name:$source})<-[:MemberOf*1..]-(e:Group)) WHERE e.objectid=$target AND s.enabled=true RETURN p",
		map[string]any{"source": "source", "target": "target"})
	formatted, err := Translated(translation)
	require.NoError(t, err)

	sql := strings.Join(strings.Fields(formatted.Statement), " ")
	require.Contains(t, sql, "all_shortest_paths_dag_v2")
	require.Contains(t, sql, "jsonb_typeof")
	require.Contains(t, sql, "E'name'")
	require.Contains(t, sql, "E'objectid'")
	require.Contains(t, sql, "E'enabled'")
	require.Contains(t, sql, "kind_ids")
	require.Contains(t, sql, "asp_resolved_endpoints.terminal_id, 1, 0, false, array [10]::int2[], true")
}

// TestBudgetedAllShortestRestoresReversedLogicalPath verifies physical reversal preserves the
// source query's node and edge order.
func TestBudgetedAllShortestRestoresReversedLogicalPath(t *testing.T) {
	translation := optimizerSafetyTranslation(t,
		"MATCH p=allShortestPaths((s)<-[*1..]-(e)) WHERE s.name IN ['source-a','source-b'] AND e.objectid='target' RETURN p")
	formatted, err := Translated(translation)
	require.NoError(t, err)

	sql := strings.Join(strings.Fields(formatted.Statement), " ")
	// Endpoint selectivity reverses this pattern before physical search. The
	// fully hydrated path must still start at the public s binding (n0).
	require.Contains(t, sql, "array [(n0.id, n0.kind_ids, n0.properties)::nodecomposite]")
	require.Contains(t, sql, "order by m0_path_index desc")
	require.Contains(t, sql, "m0_terminal.id = m0_edge.start_id")
}

// TestBudgetedAllShortestPreservesProductionPolicyIncumbentSQL verifies explicit policy and
// rollback contexts retain their qualified SQL and bindings.
func TestBudgetedAllShortestPreservesProductionPolicyIncumbentSQL(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(),
		"MATCH p=allShortestPaths((s)-[:MemberOf*1..64]->(e)) WHERE id(s)=$source AND id(e)=$target RETURN p")
	require.NoError(t, err)

	parameters := map[string]any{"source": int64(1), "target": int64(2)}
	legacy, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), parameters, DefaultGraphID,
		Options{
			DisableBudgetedAllShortestPaths: true,
		})
	require.NoError(t, err)

	legacySQL, err := Translated(legacy)
	require.NoError(t, err)

	for _, rollback := range []bool{false, true} {
		production, err := TranslateWithProductionOptions(context.Background(), query, optimizerSafetyKindMapper(), parameters, DefaultGraphID,
			ProductionOptions{
				SelectorVersion:     "frozen-policy-incumbent",
				DisableInlineASPDAG: rollback,
			})
		require.NoError(t, err)

		productionSQL, err := Translated(production)
		require.NoError(t, err)
		require.Equal(t, legacySQL.Statement, productionSQL.Statement)
		require.Equal(t, legacySQL.Parameters, productionSQL.Parameters)
		require.Equal(t, legacy.Parameters, production.Parameters)
		require.Equal(t, optimize.ShortestPathExecutorASPA1DAG,
			production.Optimization.LoweringPlan.ShortestPathExecutor[0].SelectedExecutor)
	}
}

// budgetedMissingKindsCase describes the distinction between a wildcard and
// explicit relationship kinds that may be absent from the graph schema.
type budgetedMissingKindsCase struct {
	// name identifies the kind-resolution behavior shown by the subtest.
	name string

	// relationship contains the pattern's optional relationship type syntax.
	relationship string

	// missing indicates that the result must report an unresolved kind name.
	missing bool

	// empty requires an unsatisfiable endpoint predicate instead of a wildcard.
	empty bool
}

// TestBudgetedAllShortestDistinguishesMissingKindsFromWildcard verifies absent kinds produce no
// path and remain visible to cache invalidation.
func TestBudgetedAllShortestDistinguishesMissingKindsFromWildcard(t *testing.T) {
	mapper := pgutil.NewInMemoryKindMapper()
	knownID := mapper.Put(graph.StringKind("KnownCompactStep"))
	for _, testCase := range []budgetedMissingKindsCase{
		{
			name: "wildcard",
		},
		{
			name:         "all missing",
			relationship: ":MissingCompactStep",
			missing:      true,
			empty:        true,
		},
		{
			name:         "known and missing",
			relationship: ":KnownCompactStep|MissingCompactStep",
			missing:      true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(),
				"MATCH p=allShortestPaths((s)-["+testCase.relationship+"*1..]->(e)) WHERE s.name='source' AND e.name='target' RETURN p")
			require.NoError(t, err)

			result, err := Translate(context.Background(), query, mapper, nil, DefaultGraphID)
			require.NoError(t, err)
			require.Equal(t, testCase.missing, result.HasMissingKinds)

			sql, err := Translated(result)
			require.NoError(t, err)
			require.Contains(t, sql.Statement, "all_shortest_paths_dag_v2")
			if testCase.empty {
				require.Contains(t, sql.Statement, "and false")
			} else {
				require.NotContains(t, sql.Statement, "and false")
			}
			if testCase.name == "known and missing" {
				require.Contains(t, sql.Statement, fmt.Sprintf("array [%d]::int2[]", knownID))
			}
		})
	}
	query, err := frontend.ParseCypher(frontend.NewContext(),
		"MATCH p=allShortestPaths((s)-[:MissingCompactStep*1..]->(e)) WHERE s.name='source' AND e.name='target' RETURN p")
	require.NoError(t, err)
	mapper.Put(graph.StringKind("MissingCompactStep"))
	registered, err := Translate(context.Background(), query, mapper, nil, DefaultGraphID)
	require.NoError(t, err)
	require.False(t, registered.HasMissingKinds)

	sql, err := Translated(registered)
	require.NoError(t, err)
	require.NotContains(t, sql.Statement, "and false")
}
