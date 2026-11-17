package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
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

// TestBudgetedAllShortestEndpointInequalityUsesBudgetedPairs verifies direct
// same-MATCH inequalities select V2 without changing the cached source AST.
func TestBudgetedAllShortestEndpointInequalityUsesBudgetedPairs(t *testing.T) {
	for _, predicate := range []string{
		"id(s)<>id(e)",
		"id(e)<>id(s)",
		"((id(s)) <> (id(e)))",
		"((id(e)) <> (id(s)))",
		"(id(s)<>id(e) AND (id(e)<>id(s)))",
	} {
		for _, relationship := range []string{"-[*1..]->", "<-[*1..]-", "-[*1..128]->"} {
			t.Run(predicate+"/"+relationship, func(t *testing.T) {
				source := fmt.Sprintf(
					"MATCH p=allShortestPaths((s)%s(e)) WHERE s.name IN ['left','overlap'] AND e.name IN ['overlap','right'] AND %s RETURN p",
					relationship,
					predicate,
				)

				parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
				require.NoError(t, err)

				original := cypher.Copy(parsed)
				require.True(t, UsesBudgetedAllShortestPaths(parsed))
				require.Equal(t, original, parsed)

				automatic, err := Translate(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID)
				require.NoError(t, err)

				automaticSQL, err := Translated(automatic)
				require.NoError(t, err)
				require.Contains(t, automaticSQL.Statement, "all_shortest_paths_dag_v2")
				require.Contains(t, automaticSQL.Statement, "asp_resolved_endpoints(root_id, terminal_id) as materialized")
				require.Contains(t, automaticSQL.Statement, "case when asp_root_endpoints.root_id != asp_terminal_endpoints.terminal_id then dawgs_admit_traversal_work_v2(1, 64,")
				require.Contains(t, automaticSQL.Statement, "else false end")
				require.NotContains(t, automaticSQL.Statement, "shortest_path_self_endpoint_error")

				var (
					executor = automatic.Optimization.LoweringPlan.ShortestPathExecutor[0]
					endpoint = automatic.Optimization.LoweringPlan.EndpointResolution[0]
				)

				require.Equal(t, optimize.EndpointPairPredicateDistinctIDs, executor.EndpointPairPredicate)
				require.Equal(t, optimize.ShortestPathExecutorASPA1DAGV2, executor.SelectedExecutor)
				require.Equal(t, optimize.EndpointResolutionPlanBudgeted, endpoint.SelectedPlan)
				require.Empty(t, endpoint.PairClass)
				require.NotContains(t, endpoint.PlannedClasses, optimize.EndpointResolutionClassCorrelatedPair)
				for _, fact := range endpoint.EligibilityFacts {
					require.True(t, fact.Eligible, fact.Name)
				}

				require.Equal(t, original, parsed)
			})
		}
	}
}

// TestBudgetedAllShortestUnsupportedPairFiltersRetainLegacyLowering verifies
// that the distinct-ID proof cannot excuse other endpoint correlations.
func TestBudgetedAllShortestUnsupportedPairFiltersRetainLegacyLowering(t *testing.T) {
	for _, predicate := range []string{
		"NOT(id(s)=id(e))",
		"(id(s)<>id(e) OR true)",
		"(id(s)<>id(e) OR s.enabled=true)",
		"id(s)<>id(e) AND s.flag=e.flag",
		"id(s)<>id(e) AND id(s)=id(e)",
		"s.name<>e.name",
		"id(s)+1<>id(e)",
		"id(s)<>id(e)<>42",
	} {
		t.Run(predicate, func(t *testing.T) {
			source := "MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name IN ['left','overlap'] AND e.name IN ['overlap','right'] AND (" + predicate + ") RETURN p"
			parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
			require.NoError(t, err)
			require.False(t, UsesBudgetedAllShortestPaths(parsed))

			automatic, err := Translate(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID)
			require.NoError(t, err)

			legacy, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID,
				Options{
					DisableBudgetedAllShortestPaths: true,
				})
			require.NoError(t, err)

			automaticSQL, err := Translated(automatic)
			require.NoError(t, err)

			legacySQL, err := Translated(legacy)
			require.NoError(t, err)
			require.Equal(t, legacySQL.Statement, automaticSQL.Statement)
			require.Equal(t, legacy.Parameters, automatic.Parameters)
			require.NotContains(t, automaticSQL.Statement, "all_shortest_paths_dag_v2")
		})
	}
}

// TestBudgetedAllShortestEndpointInequalityPreservesUnsupportedIdentitySyntax verifies
// proof selection cannot introduce grammar or function-argument changes.
func TestBudgetedAllShortestEndpointInequalityPreservesUnsupportedIdentitySyntax(t *testing.T) {
	_, err := frontend.ParseCypher(frontend.NewContext(),
		"MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name='source' AND e.name='target' AND id(s)!=id(e) RETURN p")
	require.ErrorContains(t, err, "token recognition error at: '!'")

	parsed, err := frontend.ParseCypher(frontend.NewContext(),
		"MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name='source' AND e.name='target' AND id((s))<>id(e) RETURN p")
	require.NoError(t, err)
	require.False(t, UsesBudgetedAllShortestPaths(parsed))

	_, automaticErr := Translate(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	_, legacyErr := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID,
		Options{
			DisableBudgetedAllShortestPaths: true,
		})
	require.Error(t, legacyErr)
	require.EqualError(t, automaticErr, legacyErr.Error())
}

// TestBudgetedAllShortestEndpointInequalitySurvivesReversalAndKindResolution verifies
// pair filtering remains independent of physical path orientation and
// preserves empty and partially known typed relationship domains.
func TestBudgetedAllShortestEndpointInequalitySurvivesReversalAndKindResolution(t *testing.T) {
	for _, kinds := range []string{"", ":MemberOf", ":MissingCompactStep", ":MemberOf|MissingCompactStep"} {
		t.Run(kinds, func(t *testing.T) {
			source := "MATCH p=allShortestPaths((s)<-[" + kinds + "*1..]-(e)) WHERE s.name IN ['left','overlap'] AND e.objectid='overlap' AND id(s)<>id(e) RETURN p"
			parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
			require.NoError(t, err)
			require.True(t, UsesBudgetedAllShortestPaths(parsed))

			translation, err := Translate(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID)
			require.NoError(t, err)

			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Equal(t, optimize.EndpointPairPredicateDistinctIDs,
				translation.Optimization.LoweringPlan.ShortestPathExecutor[0].EndpointPairPredicate)
			require.Equal(t, strings.Contains(kinds, "Missing"), translation.HasMissingKinds)
			require.Contains(t, formatted.Statement, "case when asp_root_endpoints.root_id != asp_terminal_endpoints.terminal_id then dawgs_admit_traversal_work_v2")
			require.Contains(t, formatted.Statement, "order by m0_path_index desc")
			if kinds == ":MissingCompactStep" {
				require.Contains(t, formatted.Statement, "and false")
			} else {
				require.NotContains(t, formatted.Statement, "and false")
			}
		})
	}
}

// TestBudgetedAllShortestEndpointInequalityPreservesProductionPolicies verifies
// that V2's proof does not broaden historical production or forced qualification.
func TestBudgetedAllShortestEndpointInequalityPreservesProductionPolicies(t *testing.T) {
	parsed, err := frontend.ParseCypher(frontend.NewContext(),
		"MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name='source' AND e.name='target' AND id(s)<>id(e) RETURN p")
	require.NoError(t, err)

	original := cypher.Copy(parsed)
	legacy, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID,
		Options{
			DisableBudgetedAllShortestPaths: true,
		})
	require.NoError(t, err)

	legacySQL, err := Translated(legacy)
	require.NoError(t, err)

	for _, rollback := range []bool{false, true} {
		production, err := TranslateWithProductionOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID,
			ProductionOptions{
				SelectorVersion:     "frozen-inequality-policy",
				DisableInlineASPDAG: rollback,
			})
		require.NoError(t, err)

		productionSQL, err := Translated(production)
		require.NoError(t, err)
		require.Equal(t, legacySQL.Statement, productionSQL.Statement)
		require.Equal(t, legacy.Parameters, production.Parameters)
		require.Empty(t, production.Optimization.LoweringPlan.ShortestPathExecutor[0].EndpointPairPredicate)
	}

	_, err = TranslateForTool(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID,
		ToolOptions{
			ForceShortestPathExecutor: optimize.ShortestPathExecutorASPA1DAG,
		})
	require.ErrorContains(t, err, "no structurally eligible")
	require.Equal(t, original, parsed)
}

// TestBudgetedAllShortestLaterEndpointFilterPreservesErrorBoundary verifies
// that a later WITH filter cannot remove the traversal's self-endpoint check.
func TestBudgetedAllShortestLaterEndpointFilterPreservesErrorBoundary(t *testing.T) {
	parsed, err := frontend.ParseCypher(frontend.NewContext(),
		"MATCH p=allShortestPaths((s)-[*1..]->(e)) WHERE s.name IN ['left','overlap'] AND e.name IN ['overlap','right'] WITH p,s,e WHERE id(s)<>id(e) RETURN p")
	require.NoError(t, err)
	require.True(t, UsesBudgetedAllShortestPaths(parsed))

	automatic, err := Translate(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	legacy, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID,
		Options{
			DisableBudgetedAllShortestPaths: true,
		})
	require.NoError(t, err)

	automaticSQL, err := Translated(automatic)
	require.NoError(t, err)

	legacySQL, err := Translated(legacy)
	require.NoError(t, err)
	require.Contains(t, automaticSQL.Statement, "all_shortest_paths_dag_v2")
	require.Empty(t, automatic.Optimization.LoweringPlan.ShortestPathExecutor[0].EndpointPairPredicate)
	require.NotContains(t, automaticSQL.Statement, "asp_root_endpoints.root_id <> asp_terminal_endpoints.terminal_id")
	require.Contains(t, legacySQL.Statement, "shortest_path_self_endpoint_error")
	require.Contains(t, automaticSQL.Statement, "where ((s1.n0).id <> (s1.n1).id)")
	require.Contains(t, legacySQL.Statement, "where ((s1.n0).id <> (s1.n1).id)")
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
