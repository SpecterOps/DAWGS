package optimize

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func optimizeTraversalEnvelope(t *testing.T, query string) LoweringPlan {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	return plan.LoweringPlan
}

func TestEndpointResolutionClassifiesBoundedInputsWithoutSelectingThem(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		where         string
		rootClass     EndpointResolutionClass
		terminalClass EndpointResolutionClass
		valueCount    int
		runtimeCount  bool
	}{
		{
			name:          "ID equality",
			where:         "id(s) = $source_id AND id(e) = $terminal_id",
			rootClass:     EndpointResolutionClassIDEquality,
			terminalClass: EndpointResolutionClassIDEquality,
			valueCount:    1,
		},
		{
			name:          "property name is not uniqueness proof",
			where:         "s.objectid = $source_id AND e.objectid = $terminal_id",
			rootClass:     EndpointResolutionClassNonUniquePropertyEquality,
			terminalClass: EndpointResolutionClassNonUniquePropertyEquality,
			valueCount:    1,
		},
		{
			name:          "nonunique property equality",
			where:         "s.name = $source_name AND e.name = $terminal_name",
			rootClass:     EndpointResolutionClassNonUniquePropertyEquality,
			terminalClass: EndpointResolutionClassNonUniquePropertyEquality,
			valueCount:    1,
		},
		{
			name:          "explicit small set",
			where:         "id(s) IN [1, 2] AND id(e) IN [3, 4]",
			rootClass:     EndpointResolutionClassExplicitSmallSet,
			terminalClass: EndpointResolutionClassExplicitSmallSet,
			valueCount:    2,
		},
		{
			name:          "parameterized explicit small set",
			where:         "id(s) IN $source_ids AND e.name IN $terminal_names",
			rootClass:     EndpointResolutionClassExplicitSmallSet,
			terminalClass: EndpointResolutionClassExplicitSmallSet,
			runtimeCount:  true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			plan := optimizeTraversalEnvelope(t, fmt.Sprintf(`
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE %s
				RETURN length(p)
			`, testCase.where))

			require.Len(t, plan.EndpointResolution, 1)
			decision := plan.EndpointResolution[0]
			require.Equal(t, testCase.rootClass, decision.Root.Class)
			require.Equal(t, testCase.terminalClass, decision.Terminal.Class)
			require.Equal(t, testCase.valueCount, decision.Root.StaticValueCount)
			require.Equal(t, testCase.valueCount, decision.Terminal.StaticValueCount)
			require.Equal(t, testCase.runtimeCount, decision.Root.ParameterizedSet)
			require.Equal(t, testCase.runtimeCount, decision.Terminal.ParameterizedSet)
			if testCase.runtimeCount {
				require.Equal(t, EndpointResolutionSmallSetLimit, decision.Root.Limit)
				require.Equal(t, EndpointResolutionSmallSetSentinel, decision.Root.Sentinel)
			}
			require.Equal(t, EndpointResolutionPlanBounded, decision.CandidatePlan)
			require.Equal(t, EndpointResolutionPlanIncumbent, decision.SelectedPlan)
			require.Equal(t, EndpointResolutionPlanIncumbent, decision.FallbackPlan)
			require.True(t, decision.StructurallyEligible)
			require.False(t, decision.StaticallyEligible)
			require.Equal(t, "analysis_only", decision.SelectionMode)
			require.Equal(t, EndpointResolutionFallbackPlannedOnly, decision.FallbackReason)
			require.Contains(t, plan.Decisions(), LoweringDecision{Name: LoweringEndpointResolution})
		})
	}
}

func TestEndpointResolutionRecordsCapsAndSentinelsInJSON(t *testing.T) {
	t.Parallel()

	plan := optimizeTraversalEnvelope(t, `
		MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) IN [1, 2] AND id(e) IN [3, 4]
		RETURN p
	`)
	require.Len(t, plan.EndpointResolution, 1)

	diagnostic, err := json.Marshal(plan.EndpointResolution[0])
	require.NoError(t, err)
	require.JSONEq(t, `{
		"target":{"query_part_index":0,"clause_index":0,"pattern_index":0,"step_index":0},
		"family":"ASP",
		"root":{"symbol":"s","class":"explicit_small_set","static_value_count":2,"limit":32,"sentinel":33},
		"terminal":{"symbol":"e","class":"explicit_small_set","static_value_count":2,"limit":32,"sentinel":33},
		"planned_classes":["explicit_small_set","explicit_small_set"],
		"caps":{"singleton_limit":1,"singleton_sentinel":2,"small_set_limit":32,"small_set_sentinel":33},
		"planned_candidates":["ENDPOINT-RESOLUTION-INCUMBENT","ENDPOINT-RESOLUTION-BOUNDED"],
		"candidate_plan":"ENDPOINT-RESOLUTION-BOUNDED",
		"selected_plan":"ENDPOINT-RESOLUTION-INCUMBENT",
		"fallback_plan":"ENDPOINT-RESOLUTION-INCUMBENT",
		"eligibility_facts":[
			{"name":"supported_shortest_path_mode","eligible":true},
			{"name":"single_traversal_step","eligible":true},
			{"name":"read_only","eligible":true},
			{"name":"non_optional","eligible":true},
			{"name":"bounded_endpoint_classes","eligible":true},
			{"name":"within_static_endpoint_caps","eligible":true},
			{"name":"uncorrelated_pair","eligible":true}
		],
		"structurally_eligible":true,
		"statically_eligible":false,
		"selection_mode":"analysis_only",
		"selector_version":"endpoint-resolution-v1",
		"fallback_reason":"planned_only"
	}`, string(diagnostic))
}

func TestEndpointResolutionReportsConservativeFallbackReasons(t *testing.T) {
	t.Parallel()

	values := make([]string, EndpointResolutionSmallSetSentinel)
	for index := range values {
		values[index] = fmt.Sprint(index + 1)
	}

	testCases := []struct {
		name       string
		query      string
		reason     string
		pairClass  EndpointResolutionClass
		structural bool
	}{
		{
			name: "read only remains planned",
			query: `
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = 1 AND id(e) = 2
				RETURN p
			`,
			reason:     EndpointResolutionFallbackPlannedOnly,
			structural: true,
		},
		{
			name: "mutation",
			query: `
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = 1 AND id(e) = 2
				CREATE (:Audit)
				RETURN p
			`,
			reason: EndpointResolutionFallbackMutation,
		},
		{
			name: "optional match",
			query: `
				OPTIONAL MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = 1 AND id(e) = 2
				RETURN p
			`,
			reason: EndpointResolutionFallbackOptionalMatch,
		},
		{
			name: "correlated pair",
			query: `
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = 1 AND id(e) = 2 AND s.tenant = e.tenant
				RETURN p
			`,
			reason:    EndpointResolutionFallbackCorrelatedPair,
			pairClass: EndpointResolutionClassCorrelatedPair,
		},
		{
			name: "small set cap plus one",
			query: fmt.Sprintf(`
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) IN [%s] AND id(e) IN [100]
				RETURN p
			`, strings.Join(values, ",")),
			reason: EndpointResolutionFallbackSmallSetOverflow,
		},
		{
			name: "unsupported endpoint syntax",
			query: `
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE s.name STARTS WITH 'source' AND e.name STARTS WITH 'terminal'
				RETURN p
			`,
			reason: EndpointResolutionFallbackUnsupported,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			plan := optimizeTraversalEnvelope(t, testCase.query)
			require.Len(t, plan.EndpointResolution, 1)
			decision := plan.EndpointResolution[0]
			require.Equal(t, testCase.reason, decision.FallbackReason)
			require.Equal(t, testCase.pairClass, decision.PairClass)
			require.Equal(t, testCase.structural, decision.StructurallyEligible)
			require.False(t, decision.StaticallyEligible)
			require.Equal(t, EndpointResolutionPlanIncumbent, decision.SelectedPlan)
		})
	}
}

func TestTraversalPredicateClassifiesLocalUniversalAndWholePathForms(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		predicate     string
		class         TraversalPredicateClass
		bindingSymbol string
		fallback      string
		structural    bool
	}{
		{
			name:       "endpoint WHERE predicate is not step local",
			predicate:  "s.enabled = true",
			class:      TraversalPredicateClassUnsupported,
			fallback:   TraversalPredicateFallbackUnsupported,
			structural: false,
		},
		{
			name:       "range binding WHERE predicate is not step local",
			predicate:  "rels.enabled = true",
			class:      TraversalPredicateClassUnsupported,
			fallback:   TraversalPredicateFallbackUnsupported,
			structural: false,
		},
		{
			name:          "all nodes",
			predicate:     "all(n IN nodes(p) WHERE n.enabled = true)",
			class:         TraversalPredicateClassUniversalAllNodes,
			bindingSymbol: "n",
			fallback:      TraversalPredicateFallbackPlannedOnly,
			structural:    true,
		},
		{
			name:          "none nodes",
			predicate:     "none(n IN nodes(p) WHERE n.disabled = true)",
			class:         TraversalPredicateClassUniversalNoneNodes,
			bindingSymbol: "n",
			fallback:      TraversalPredicateFallbackPlannedOnly,
			structural:    true,
		},
		{
			name:          "all relationships",
			predicate:     "all(r IN relationships(p) WHERE type(r) = 'MemberOf')",
			class:         TraversalPredicateClassUniversalAllRelationships,
			bindingSymbol: "r",
			fallback:      TraversalPredicateFallbackPlannedOnly,
			structural:    true,
		},
		{
			name:          "none relationships",
			predicate:     "none(r IN relationships(p) WHERE type(r) = 'AdminTo')",
			class:         TraversalPredicateClassUniversalNoneRelationships,
			bindingSymbol: "r",
			fallback:      TraversalPredicateFallbackPlannedOnly,
			structural:    true,
		},
		{
			name:       "whole path",
			predicate:  "length(p) > 2",
			class:      TraversalPredicateClassWholePath,
			fallback:   TraversalPredicateFallbackWholePath,
			structural: false,
		},
		{
			name:       "correlated endpoints",
			predicate:  "s.tenant = e.tenant",
			class:      TraversalPredicateClassUnsupported,
			fallback:   TraversalPredicateFallbackCorrelation,
			structural: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			plan := optimizeTraversalEnvelope(t, fmt.Sprintf(`
				MATCH p = shortestPath((s)-[rels:MemberOf*1..4]->(e))
				WHERE %s
				RETURN p
			`, testCase.predicate))

			require.Len(t, plan.TraversalPredicate, 1)
			decision := plan.TraversalPredicate[0]
			require.Equal(t, testCase.class, decision.Class)
			require.Equal(t, testCase.bindingSymbol, decision.BindingSymbol)
			require.Equal(t, testCase.fallback, decision.FallbackReason)
			require.Equal(t, testCase.structural, decision.StructurallyEligible)
			require.False(t, decision.StaticallyEligible)
			require.Equal(t, TraversalPredicatePlanIncumbent, decision.SelectedPlan)
			require.Equal(t, TraversalPredicatePlanIncumbent, decision.FallbackPlan)
			require.Equal(t, "analysis_only", decision.SelectionMode)
			require.Contains(t, plan.Decisions(), LoweringDecision{Name: LoweringTraversalPredicateClassification})
		})
	}
}

func TestTraversalPredicateOnlyClaimsInlineRelationshipPropertiesAsStepLocal(t *testing.T) {
	t.Parallel()

	plan := optimizeTraversalEnvelope(t, `
		MATCH p = shortestPath((s {enabled: true})-[rels:MemberOf*1..4{active: true}]->(e))
		RETURN p
	`)
	require.Len(t, plan.TraversalPredicate, 2)

	require.Equal(t, "relationship_pattern", plan.TraversalPredicate[0].Source)
	require.Equal(t, TraversalPredicateClassStepLocalRelationship, plan.TraversalPredicate[0].Class)
	require.True(t, plan.TraversalPredicate[0].StructurallyEligible)
	require.Equal(t, TraversalPredicateFallbackPlannedOnly, plan.TraversalPredicate[0].FallbackReason)

	require.Equal(t, "node_pattern", plan.TraversalPredicate[1].Source)
	require.Equal(t, TraversalPredicateClassUnsupported, plan.TraversalPredicate[1].Class)
	require.False(t, plan.TraversalPredicate[1].StructurallyEligible)
	require.Equal(t, TraversalPredicateFallbackUnsupported, plan.TraversalPredicate[1].FallbackReason)
}

func TestTraversalPredicateReportsMutationOptionalAndCorrelationFallbacks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		query      string
		reason     string
		structural bool
	}{
		{
			name: "read only remains planned",
			query: `
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE all(n IN nodes(p) WHERE n.enabled = true)
				RETURN p
			`,
			reason:     TraversalPredicateFallbackPlannedOnly,
			structural: true,
		},
		{
			name: "mutation",
			query: `
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE all(n IN nodes(p) WHERE n.enabled = true)
				CREATE (:Audit)
				RETURN p
			`,
			reason: TraversalPredicateFallbackMutation,
		},
		{
			name: "optional match",
			query: `
				OPTIONAL MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE all(n IN nodes(p) WHERE n.enabled = true)
				RETURN p
			`,
			reason: TraversalPredicateFallbackOptional,
		},
		{
			name: "correlated predicate",
			query: `
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE s.tenant = e.tenant
				RETURN p
			`,
			reason: TraversalPredicateFallbackCorrelation,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			plan := optimizeTraversalEnvelope(t, testCase.query)
			require.Len(t, plan.TraversalPredicate, 1)
			decision := plan.TraversalPredicate[0]
			require.Equal(t, testCase.reason, decision.FallbackReason)
			require.Equal(t, testCase.structural, decision.StructurallyEligible)
			require.False(t, decision.StaticallyEligible)
			require.Equal(t, TraversalPredicatePlanIncumbent, decision.SelectedPlan)
		})
	}
}

func TestTraversalPredicateJSONKeepsCandidatePlannedOnly(t *testing.T) {
	t.Parallel()

	plan := optimizeTraversalEnvelope(t, `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE all(n IN nodes(p) WHERE n.enabled = true)
		RETURN p
	`)
	require.Len(t, plan.TraversalPredicate, 1)

	diagnostic, err := json.Marshal(plan.TraversalPredicate[0])
	require.NoError(t, err)
	require.Contains(t, string(diagnostic), `"class":"universal_all_nodes"`)
	require.Contains(t, string(diagnostic), `"planned_candidates":["TRAVERSAL-PREDICATE-INCUMBENT","TRAVERSAL-PREDICATE-STEP"]`)
	require.Contains(t, string(diagnostic), `"selected_plan":"TRAVERSAL-PREDICATE-INCUMBENT"`)
	require.Contains(t, string(diagnostic), `"statically_eligible":false`)
	require.Contains(t, string(diagnostic), `"fallback_reason":"planned_only"`)
}
