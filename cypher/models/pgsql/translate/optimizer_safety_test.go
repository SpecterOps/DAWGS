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

func TestOptimizerSafetyADCSQueryPrunesExpansionEdgeCarry(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), optimizerADCSQuery)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "select distinct (s5.n0).id as root_id from s5")
	require.Contains(t, normalizedQuery, "s5.ep0 as ep0")
	require.NotContains(t, normalizedQuery, "s5.e0 as e0")
	require.Contains(t, normalizedQuery, "from unnest(s12.ep0)")
	require.Contains(t, normalizedQuery, "from unnest(array [s12.e1]::int8[])")
	require.NotContains(t, normalizedQuery, "array [s12.e1]::edgecomposite[]")
	require.Contains(t, normalizedQuery, "from s5, s7")
}

func TestOptimizerSafetyReferencedRelationshipStaysComposite(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
MATCH p = (n:Group)-[r:MemberOf]->(m:Group)
RETURN p, r
`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "(e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0")
	require.Contains(t, normalizedQuery, "array [s0.e0]::edgecomposite[]")
	require.NotContains(t, normalizedQuery, "e0.id as e0")
	require.NotContains(t, normalizedQuery, "array [s0.e0]::int8[]")
}

func TestOptimizerSafetyOptionalMatchPathStaysComposite(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
MATCH (n:Group)
OPTIONAL MATCH p = (n)-[:MemberOf]->(m:Group)
RETURN n, p
`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), regularQuery, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	formattedQuery, err := Translated(translation)
	require.NoError(t, err)

	normalizedQuery := strings.Join(strings.Fields(formattedQuery), " ")

	require.Contains(t, normalizedQuery, "::edgecomposite[]")
	require.NotContains(t, normalizedQuery, "::int8[]")
}
