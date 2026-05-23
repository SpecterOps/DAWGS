package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const optimizerADCSQuery = `
MATCH (n:Group)
WHERE n.objectid = 'S-1-5-21-2643190041-1319121918-239771340-513'
MATCH p1 = (n)-[:MemberOf*0..]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
MATCH p2 = (n)-[:MemberOf*0..]->()-[:GenericAll|Enroll|AllExtendedRights]->(ct:CertTemplate)-[:PublishedTo]->(ca)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(:RootCA)-[:RootCAFor]->(d)
WHERE ct.authenticationenabled = true
AND ct.requiresmanagerapproval = false
AND ct.enrolleesuppliessubject = true
AND (ct.schemaversion = 1 OR ct.authorizedsignatures = 0)
RETURN p1, p2
`

func optimizerSafetyKindMapper() *pgutil.InMemoryKindMapper {
	mapper := pgutil.NewInMemoryKindMapper()

	for _, kind := range graph.StringsToKinds([]string{
		"AllExtendedRights",
		"CertTemplate",
		"Domain",
		"Enroll",
		"EnterpriseCA",
		"EnterpriseCAFor",
		"GenericAll",
		"Group",
		"IssuedSignedBy",
		"MemberOf",
		"NTAuthStore",
		"NTAuthStoreFor",
		"PublishedTo",
		"RootCA",
		"RootCAFor",
		"TrustedForNTAuth",
		"AdminTo",
		"Computer",
		"Tag_Tier_Zero",
		"User",
	}) {
		mapper.Put(kind)
	}

	return mapper
}

func optimizerSafetySQL(t *testing.T, cypherQuery string) string {
	t.Helper()

	translation := optimizerSafetyTranslation(t, cypherQuery)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	return strings.Join(strings.Fields(formattedQuery), " ")
}

func optimizerSafetyTranslation(t *testing.T, cypherQuery string) Result {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	return translation
}

func requireOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.Lowerings {
		if lowering.Name == name {
			return
		}
	}

	require.Failf(t, "missing optimization lowering", "expected lowering %q in %#v", name, summary.Lowerings)
}

func requireNoOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.Lowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected applied lowering %q in %#v", name, summary.Lowerings)
	}
}

func requirePlannedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.PlannedLowerings {
		if lowering.Name == name {
			return
		}
	}

	require.Failf(t, "missing planned optimization lowering", "expected planned lowering %q in %#v", name, summary.PlannedLowerings)
}

func requireNoPlannedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.PlannedLowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected planned lowering %q in %#v", name, summary.PlannedLowerings)
	}
}

func requireSkippedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string, reason string) {
	t.Helper()

	for _, lowering := range summary.SkippedLowerings {
		if lowering.Name == name {
			require.Equal(t, reason, lowering.Reason)
			return
		}
	}

	require.Failf(t, "missing skipped optimization lowering", "expected skipped lowering %q in %#v", name, summary.SkippedLowerings)
}

func requireSkippedOptimizationLoweringCount(t *testing.T, summary OptimizationSummary, name string, count int) {
	t.Helper()

	for _, lowering := range summary.SkippedLowerings {
		if lowering.Name == name {
			require.Equal(t, count, lowering.Count)
			return
		}
	}

	require.Failf(t, "missing skipped optimization lowering", "expected skipped lowering %q in %#v", name, summary.SkippedLowerings)
}

func requireNoSkippedOptimizationLowering(t *testing.T, summary OptimizationSummary, name string) {
	t.Helper()

	for _, lowering := range summary.SkippedLowerings {
		require.NotEqualf(t, name, lowering.Name, "unexpected skipped lowering %q in %#v", name, summary.SkippedLowerings)
	}
}

func TestOptimizerSafetyReportsPartiallySkippedLowerings(t *testing.T) {
	t.Parallel()

	translator := NewTranslator(context.Background(), optimizerSafetyKindMapper(), nil, DefaultGraphID)
	translator.translation.Optimization.LoweringPlan = &optimize.LoweringPlan{
		PredicatePlacement: []optimize.PredicatePlacementDecision{
			{Target: optimize.TraversalStepTarget{StepIndex: 0}},
			{Target: optimize.TraversalStepTarget{StepIndex: 1}},
		},
	}

	translator.recordLowering(optimize.LoweringPredicatePlacement)
	translator.recordSkippedLowerings()

	requireOptimizationLowering(t, translator.translation.Optimization, optimize.LoweringPredicatePlacement)
	requireSkippedOptimizationLowering(t, translator.translation.Optimization, optimize.LoweringPredicatePlacement, "planned predicate placements were not consumed by this translation shape")
	requireSkippedOptimizationLoweringCount(t, translator.translation.Optimization, optimize.LoweringPredicatePlacement, 1)
}

func requireSQLContainsInOrder(t *testing.T, sql string, parts ...string) {
	t.Helper()

	offset := 0
	for _, part := range parts {
		nextIndex := strings.Index(sql[offset:], part)
		require.NotEqualf(t, -1, nextIndex, "expected SQL to contain %q after offset %d:\n%s", part, offset, sql)
		offset += nextIndex + len(part)
	}
}

func TestOptimizerSafetyCountStoreFastPathUsesBaseNodeCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH (n) RETURN count(n)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	require.Empty(t, translation.Optimization.SkippedLowerings)
	require.Equal(t, "select count(*)::int8 from node n0;", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyCountStoreFastPathKeepsKindConstraintAndAlias(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH (n:Group) RETURN count(n) AS total`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	require.Equal(t, "select count(*)::int8 as total from node n0 where n0.kind_ids operator (pg_catalog.@>) array [8]::int2[];", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyCountStoreFastPathSupportsNodeCountStar(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH (:Group) RETURN count(*) AS total`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	require.Equal(t, "select count(*)::int8 as total from node n0 where n0.kind_ids operator (pg_catalog.@>) array [8]::int2[];", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyCountStoreFastPathUsesBaseEdgeCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[r:MemberOf]->() RETURN count(r)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [10]::int2[]);", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyCountStoreFastPathUsesSparseEdgeKindCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[r:Enroll]->() RETURN count(r)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.NotContains(t, normalizedQuery, "with recursive")
	require.NotContains(t, normalizedQuery, "ordered_edges_to_path")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [4]::int2[]);", normalizedQuery)
}

func TestOptimizerSafetyCountStoreFastPathUsesUntypedEdgeCount(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[r]->() RETURN count(r)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.NotContains(t, normalizedQuery, "with recursive")
	require.NotContains(t, normalizedQuery, "ordered_edges_to_path")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id;", normalizedQuery)
}

func TestOptimizerSafetyCountStoreFastPathSupportsEdgeCountStar(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `MATCH ()-[:MemberOf]->() RETURN count(*)`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringCountStoreFastPath)
	requireSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringProjectionPruning, "superseded by CountStoreFastPath")
	require.Equal(t, "select count(*)::int8 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [10]::int2[]);", strings.Join(strings.Fields(formattedQuery), " "))
}

func TestOptimizerSafetyADCSQueryPrunesExpansionEdgeCarry(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, optimizerADCSQuery)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requirePlannedOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")
	requireOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requireOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")

	require.Contains(t, normalizedQuery, "select distinct (s0.n0).id as root_id from s0")
	require.Contains(t, normalizedQuery, "select distinct (s5.n0).id as root_id from s5")
	require.Contains(t, normalizedQuery, "select distinct (s9.n2).id as root_id from s9")
	require.Contains(t, normalizedQuery, "s5.ep0 as ep0")
	require.NotContains(t, normalizedQuery, "s5.e0 as e0")
	require.Contains(t, normalizedQuery, "from unnest(s12.ep0)")
	require.Contains(t, normalizedQuery, "from unnest(array [s12.e1]::int8[])")
	require.NotContains(t, normalizedQuery, "array [s12.e1]::edgecomposite[]")
	require.Contains(t, normalizedQuery, "from s5, s7")
	requireSQLContainsInOrder(t, normalizedQuery,
		"where s7.satisfied and exists (select 1 from edge e5 join node n6",
		"properties -> 'authenticationenabled'",
		"join edge e6 on n6.id = e6.start_id",
		"e6.end_id = (s5.n2).id",
		"and (s5.n0).id = s7.root_id",
	)
	requireSQLContainsInOrder(t, normalizedQuery,
		"where s11.satisfied and (s9.n2).id = s11.root_id and exists",
		"from edge e8 where n7.id = e8.start_id",
		"e8.end_id = (s9.n4).id",
	)
}

func assertOptimizerSafetyRelationshipStaysComposite(t *testing.T, cypherQuery string) {
	t.Helper()

	normalizedQuery := optimizerSafetySQL(t, cypherQuery)

	require.Contains(t, normalizedQuery, "(e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0")
	require.Contains(t, normalizedQuery, "::edgecomposite")
	require.NotContains(t, normalizedQuery, "e0.id as e0")
	require.NotContains(t, normalizedQuery, "::int8[]")
}

func TestOptimizerSafetyReferencedRelationshipStaysComposite(t *testing.T) {
	t.Parallel()

	assertOptimizerSafetyRelationshipStaysComposite(t, `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
RETURN p, r
`)
}

func TestOptimizerSafetyRelationshipExpressionReferencesStayComposite(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name: "type return",
			query: `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
RETURN p, type(r)
`,
		},
		{
			name: "property predicate",
			query: `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
WHERE r.label = 'member'
RETURN p
`,
		},
		{
			name: "start node return",
			query: `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
RETURN p, startNode(r)
`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			assertOptimizerSafetyRelationshipStaysComposite(t, testCase.query)
		})
	}
}

func TestOptimizerSafetyOptionalMatchPathStaysComposite(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (n:Group)
OPTIONAL MATCH p = (n)-[:MemberOf]->(m:Group)
RETURN n, p
`)

	require.Contains(t, normalizedQuery, "::edgecomposite[]")
	require.NotContains(t, normalizedQuery, "::int8[]")
}

func TestOptimizerSafetyFixedHopExpandIntoUsesBoundEndpoints(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
MATCH (a:Group)
MATCH (b:Group)
MATCH p = (a)-[:MemberOf]->(b)
RETURN p
`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "(s1.n0).id = e0.start_id")
	require.Contains(t, normalizedQuery, "(s1.n1).id = e0.end_id")
	require.NotContains(t, normalizedQuery, "join node")
	require.NotNil(t, translation.Optimization.LoweringPlan)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.ExpandInto)
	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")
	requireOptimizationLowering(t, translation.Optimization, "ExpandIntoDetection")
}

func TestOptimizerSafetyReordersIndependentNodeAnchor(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (a)
MATCH (b:EnterpriseCA {name: 'target'})
MATCH p = (a)-[:MemberOf]->(b)
RETURN p
`)
	enterpriseAnchorIndex := strings.Index(normalizedQuery, "array [5]::int2[]")
	broadScanIndex := strings.Index(normalizedQuery, "from s0, node n1")

	require.NotEqual(t, -1, enterpriseAnchorIndex)
	require.NotEqual(t, -1, broadScanIndex)
	require.Less(t, enterpriseAnchorIndex, broadScanIndex)
	require.Contains(t, normalizedQuery, "(s1.n1).id = e0.start_id")
	require.Contains(t, normalizedQuery, "(s1.n0).id = e0.end_id")
}

func TestOptimizerSafetyExpansionTerminalPushdownForFixedSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:Enroll]->(ca:EnterpriseCA)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
	require.Contains(t, normalizedQuery, "n1.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any (array [4]::int2[])")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>) array [5]::int2[]")
}

func TestOptimizerSafetySuffixPredicatePlacementStaysInsideTerminalExists(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:Enroll]->(ca:EnterpriseCA)
WHERE ca.name = 'target'
RETURN p
`)

	requireSQLContainsInOrder(t, normalizedQuery,
		"exists (select 1 from edge e1 join node n2",
		"properties -> 'name'",
		"where n1.id = e1.start_id",
	)
}

func TestOptimizerSafetyPredicatePlacementRecordsExpansionRootConstraint(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (src:Group)-[:MemberOf*1..]->(mid)-[:Enroll]->(ca:EnterpriseCA)
WHERE src.name = 'source'
RETURN p
`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireSQLContainsInOrder(t, normalizedQuery,
		"select n0.id as root_id from node n0 where",
		"properties -> 'name'",
	)
}

func TestOptimizerSafetyPredicatePlacementRecordsFixedTraversalConstraint(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (src:Group)-[:MemberOf]->(dst)
WHERE src.name = 'source'
RETURN dst
`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireNoSkippedOptimizationLowering(t, translation.Optimization, optimize.LoweringPredicatePlacement)
	requireSQLContainsInOrder(t, normalizedQuery,
		"join node n0 on",
		"properties -> 'name'",
		"join node n1",
	)
}

func TestOptimizerSafetyPatternPredicateExistencePlacementIsPlanned(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (s)
WHERE NOT (s)-[]-()
RETURN s
`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "not exists (select 1 from edge e0")
	requirePlannedOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requireOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
}

func TestOptimizerSafetyContinuationRelationshipsExcludePriorPathRelationships(t *testing.T) {
	t.Parallel()

	expandedPrefixQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:Enroll]-(ca:EnterpriseCA)
RETURN p
`)

	require.Contains(t, expandedPrefixQuery, "e1.id != all")
	require.Contains(t, expandedPrefixQuery, "ep0")

	fixedPrefixQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf]->(m)-[:Enroll]->(ca:EnterpriseCA)
RETURN p
`)

	require.Contains(t, fixedPrefixQuery, "e1.id != s0.e0")
}

func TestOptimizerSafetyDirectionBalancedExpansionDoesNotPlanStaleSuffixPushdown(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (n)-[:MemberOf*1..]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(d:Domain)
RETURN p
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireNoPlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireNoOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
}

func TestOptimizerSafetyTraversalDirectionUsesRightEndpointPredicate(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (n)-[:MemberOf*1..]->(ca)
WHERE ca.name = 'target'
RETURN p
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requireOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	require.Contains(t, normalizedQuery, "jsonb_typeof((n1.properties -> 'name')) = 'string'")
	require.Contains(t, normalizedQuery, "(n1.properties ->> 'name') = 'target'")
	require.Contains(t, normalizedQuery, "join edge e0 on e0.end_id = s1_seed.root_id")
}

func TestOptimizerSafetyAggregateTraversalCountUsesIDOnlySourceAnchoredShape(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
	`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")
	lowerQuery := strings.ToLower(normalizedQuery)

	requireNoPlannedOptimizationLowering(t, translation.Optimization, "TraversalDirectionSelection")
	requirePlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	requireOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	require.Contains(t, lowerQuery, "with recursive candidate_sources(root_id)")
	require.Contains(t, lowerQuery, "traversal(root_id, next_id, depth, path)")
	require.Contains(t, lowerQuery, "terminal_nodes(id) as materialized")
	require.Contains(t, lowerQuery, "terminal_hits(root_id)")
	require.Contains(t, lowerQuery, "ranked(root_id, admincount)")
	require.Contains(t, lowerQuery, "join edge e on e.start_id = candidate_sources.root_id")
	require.Contains(t, lowerQuery, "e.start_id = traversal.next_id")
	require.Contains(t, lowerQuery, "e.id != all (traversal.path)")
	require.Contains(t, lowerQuery, "join terminal_nodes on terminal_nodes.id = traversal.next_id")
	require.Contains(t, lowerQuery, "count(*)::int8 as admincount")
	require.Contains(t, lowerQuery, "group by terminal_hits.root_id")
	require.Contains(t, lowerQuery, "from ranked join node source_node on source_node.id = ranked.root_id")
	require.NotContains(t, lowerQuery, "group by (")
	require.NotContains(t, lowerQuery, "::nodecomposite as n0 from")
}

func TestOptimizerSafetyAggregateTraversalCountSkipsObservedTerminal(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, c, COUNT(c) AS adminCount
RETURN u, c
ORDER BY adminCount DESC
LIMIT 100
	`)

	requireNoPlannedOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
	requireNoOptimizationLowering(t, translation.Optimization, optimize.LoweringAggregateTraversalCount)
}

func TestOptimizerSafetyShortestPathStrategyUsesPlannedBidirectionalSearch(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = allShortestPaths((s)-[:MemberOf*1..]->(e))
WHERE s.name = 'source' AND e.name = 'target'
RETURN p
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "bidirectional_asp_harness")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathStrategySelection")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathStrategySelection")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
}

func TestOptimizerSafetyShortestPathTerminalFilterUsesPlannedMaterialization(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (s:Group {name: 'source'})
MATCH p = shortestPath((s)-[:MemberOf*1..]->(e))
WHERE e.name = 'target'
RETURN p
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "unidirectional_sp_harness")
	require.Contains(t, normalizedQuery, "traversal_terminal_filter")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
}

func TestOptimizerSafetyShortestPathKindOnlyTerminalFilterUsesPlannedMaterialization(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = shortestPath((s:Group)-[:MemberOf|GenericAll|AdminTo*1..]->(t:Tag_Tier_Zero))
WHERE s.objectid ENDS WITH '-513' AND s <> t
RETURN p
LIMIT 1000
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "unidirectional_sp_harness")
	require.Contains(t, normalizedQuery, "traversal_terminal_filter")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ShortestPathFilterMaterialization")
}

func TestOptimizerSafetyLimitPushdownUsesPlannedTraversalFrame(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = (n:Group)-[:MemberOf]->(m:Group)
RETURN p
LIMIT 1
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "LimitPushdown")
	requireOptimizationLowering(t, translation.Optimization, "LimitPushdown")
}

func TestOptimizerSafetyShortestPathLimitPushdownUsesPlannedHarness(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH p = shortestPath((s)-[:MemberOf*1..]->(e))
WHERE s.name = 'source' AND e.name = 'target'
RETURN p
LIMIT 1
	`)

	requirePlannedOptimizationLowering(t, translation.Optimization, "LimitPushdown")
	requireOptimizationLowering(t, translation.Optimization, "LimitPushdown")
}

func TestOptimizerSafetyShortestPathRootCarriesUnwindSources(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
		UNWIND ['source'] AS sourceName
		MATCH p = shortestPath((s:Group)-[:MemberOf*1..]->(e:Group))
		WHERE s.name = sourceName AND e.name = 'target'
		RETURN sourceName, p
	`)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")
	primerQuery, hasPrimerQuery := translation.Parameters["pi0"].(string)

	require.True(t, hasPrimerQuery)
	require.Contains(t, normalizedQuery, "unidirectional_sp_harness")
	require.Contains(t, normalizedQuery, "unnest(array ['source']::text[]) as i0")
	require.Contains(t, primerQuery, "jsonb_typeof((n1.properties -> 'name')) = 'string'")
	require.Contains(t, primerQuery, "(n0.properties ->> 'name') = i0")
}

func TestOptimizerSafetyTranslationReportsOptimizerMetadata(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:Enroll]->(ca:EnterpriseCA)
WHERE ca.name = 'target'
RETURN p
`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	require.NotEmpty(t, translation.Optimization.Rules)
	require.NotEmpty(t, translation.Optimization.PredicateAttachments)
	require.NotNil(t, translation.Optimization.LoweringPlan)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.ProjectionPruning)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.LatePathMaterialization)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.ExpansionSuffixPushdown)
	require.NotEmpty(t, translation.Optimization.LoweringPlan.PredicatePlacement)
	requirePlannedOptimizationLowering(t, translation.Optimization, "ProjectionPruning")
	requirePlannedOptimizationLowering(t, translation.Optimization, "LatePathMaterialization")
	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requirePlannedOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
	requireOptimizationLowering(t, translation.Optimization, "ProjectionPruning")
	requireOptimizationLowering(t, translation.Optimization, "LatePathMaterialization")
	requireOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireOptimizationLowering(t, translation.Optimization, "PredicatePlacement")
}

func TestOptimizerSafetyExpansionTerminalPushdownForZeroDepthExpansion(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:Enroll]->(ca:EnterpriseCA)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
	require.Contains(t, normalizedQuery, "n1.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any (array [4]::int2[])")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>) array [5]::int2[]")
}

func TestOptimizerSafetyExpansionTerminalPushdownForBoundEndpointSuffixChain(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (ca:EnterpriseCA {name: 'target'})
MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:Enroll]->(ct:CertTemplate)-[:PublishedTo]->(ca)
WHERE ct.authenticationenabled = true
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n3")
	require.Contains(t, normalizedQuery, "join edge e2 on n3.id = e2.start_id")
	require.Contains(t, normalizedQuery, "n2.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any")
	require.Contains(t, normalizedQuery, "n3.kind_ids operator (pg_catalog.@>)")
	require.Contains(t, normalizedQuery, "e2.kind_id = any")
	require.Contains(t, normalizedQuery, "e2.end_id = (s0.n0).id")
	requireSQLContainsInOrder(t, normalizedQuery,
		"exists (select 1 from edge e1 join node n3",
		"properties -> 'authenticationenabled'",
		"join edge e2 on n3.id = e2.start_id",
		"e2.end_id = (s0.n0).id",
	)
}

func TestOptimizerSafetyExpansionTerminalPushdownIncludesConstrainedBoundEndpoint(t *testing.T) {
	t.Parallel()

	translation := optimizerSafetyTranslation(t, `
MATCH (ca)
MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:Enroll]->(ct:CertTemplate)-[:PublishedTo]->(ca:EnterpriseCA)
RETURN p
`)
	formattedQuery, err := Translated(translation)
	require.NoError(t, err)
	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	requirePlannedOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
	requireSQLContainsInOrder(t, normalizedQuery,
		"exists (select 1 from edge e1 join node n3",
		"join edge e2 on n3.id = e2.start_id",
		"e2.end_id = (s0.n0).id",
	)
	require.Contains(t, normalizedQuery, "(s0.n0).kind_ids operator (pg_catalog.@>)")
}

func TestOptimizerSafetyExpansionTerminalPushdownForBoundDomainSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (d:Domain {name: 'target'})
MATCH p = (ca:EnterpriseCA)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(root:RootCA)-[:RootCAFor]->(d)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1")
	require.Contains(t, normalizedQuery, "e1.kind_id = any")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>)")
	require.Contains(t, normalizedQuery, "n2.id = e1.start_id")
	require.Contains(t, normalizedQuery, "e1.end_id = (s0.n0).id")
}

func TestOptimizerSafetyExpansionTerminalPushdownForInboundFixedSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (ca:EnterpriseCA)<-[:PublishedTo*1..]-(ct)<-[:Enroll]-(m:Group)
RETURN p
`)

	require.Contains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
	require.Contains(t, normalizedQuery, "n1.id = e1.end_id")
	require.Contains(t, normalizedQuery, "e1.kind_id = any (array [4]::int2[])")
	require.Contains(t, normalizedQuery, "n2.kind_ids operator (pg_catalog.@>)")
}

func TestOptimizerSafetyExpansionTerminalPushdownSkipsDirectionlessSuffix(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, `
MATCH p = (n:Group)-[:MemberOf*1..]->(m)-[:Enroll]-(ca:EnterpriseCA)
RETURN p
`)

	require.NotContains(t, normalizedQuery, "exists (select 1 from edge e1 join node n2")
}
