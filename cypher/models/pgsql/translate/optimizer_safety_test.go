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
	}) {
		mapper.Put(kind)
	}

	return mapper
}

func optimizerSafetySQL(t *testing.T, cypherQuery string) string {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	return strings.Join(strings.Fields(formattedQuery), " ")
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

func TestOptimizerSafetyADCSQueryPrunesExpansionEdgeCarry(t *testing.T) {
	t.Parallel()

	normalizedQuery := optimizerSafetySQL(t, optimizerADCSQuery)

	require.Contains(t, normalizedQuery, "select distinct (s5.n0).id as root_id from s5")
	require.Contains(t, normalizedQuery, "s5.ep0 as ep0")
	require.NotContains(t, normalizedQuery, "s5.e0 as e0")
	require.Contains(t, normalizedQuery, "from unnest(s12.ep0)")
	require.Contains(t, normalizedQuery, "from unnest(array [s12.e1]::int8[])")
	require.NotContains(t, normalizedQuery, "array [s12.e1]::edgecomposite[]")
	require.Contains(t, normalizedQuery, "from s5, s7")
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

	normalizedQuery := optimizerSafetySQL(t, `
MATCH (a:Group)
MATCH (b:Group)
MATCH p = (a)-[:MemberOf]->(b)
RETURN p
`)

	require.Contains(t, normalizedQuery, "(s1.n0).id = e0.start_id")
	require.Contains(t, normalizedQuery, "(s1.n1).id = e0.end_id")
	require.NotContains(t, normalizedQuery, "join node")
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
	requireOptimizationLowering(t, translation.Optimization, "ExpansionSuffixPushdown")
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
	require.Contains(t, normalizedQuery, "properties -> 'authenticationenabled'")
	require.Contains(t, normalizedQuery, "n3.kind_ids operator (pg_catalog.@>)")
	require.Contains(t, normalizedQuery, "e2.kind_id = any")
	require.Contains(t, normalizedQuery, "e2.end_id = (s0.n0).id")
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
