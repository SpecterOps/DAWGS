package optimize

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

const adcsQuery = `
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

func analyzeCypher(t *testing.T, query string) Analysis {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)

	return Analyze(regularQuery)
}

func requireBinding(t *testing.T, bindings []Binding, symbol string, kind BindingKind) {
	t.Helper()

	for _, binding := range bindings {
		if binding.Symbol == symbol && binding.Kind == kind {
			return
		}
	}

	t.Fatalf("expected binding %s:%s in %#v", symbol, kind, bindings)
}

func requirePathVariable(t *testing.T, pathVariables []PathVariable, symbol string, relationshipCount int) {
	t.Helper()

	for _, pathVariable := range pathVariables {
		if pathVariable.Symbol == symbol {
			require.Equal(t, relationshipCount, pathVariable.RelationshipCount)
			require.True(t, pathVariable.VariableLength)
			return
		}
	}

	t.Fatalf("expected path variable %s in %#v", symbol, pathVariables)
}

func TestAnalyzeIdentifiesEligibleADCSRegion(t *testing.T) {
	t.Parallel()

	analysis := analyzeCypher(t, adcsQuery)

	require.Len(t, analysis.QueryParts, 1)

	queryPart := analysis.QueryParts[0]
	require.Equal(t, QueryPartKindSingle, queryPart.Kind)
	require.Equal(t, []string{"p1", "p2"}, queryPart.ProjectionDependencies)
	require.Len(t, queryPart.Regions, 1)
	require.Len(t, queryPart.Barriers, 1)
	require.Equal(t, BarrierKindReturn, queryPart.Barriers[0].Kind)
	require.Equal(t, []string{"p1", "p2"}, queryPart.Barriers[0].Dependencies)

	region := queryPart.Regions[0]
	require.Equal(t, 0, region.StartClause)
	require.Equal(t, 2, region.EndClause)
	require.Len(t, region.Clauses, 3)
	require.Len(t, region.BindingOccurrences, 10)
	require.Len(t, region.Predicates, 2)
	require.Equal(t, []string{"n"}, region.Predicates[0].Dependencies)
	require.Equal(t, []string{"ct"}, region.Predicates[1].Dependencies)

	requireBinding(t, region.Bindings, "n", BindingKindNode)
	requireBinding(t, region.Bindings, "ca", BindingKindNode)
	requireBinding(t, region.Bindings, "ct", BindingKindNode)
	requireBinding(t, region.Bindings, "d", BindingKindNode)
	requireBinding(t, region.Bindings, "p1", BindingKindPath)
	requireBinding(t, region.Bindings, "p2", BindingKindPath)

	requirePathVariable(t, region.PathVariables, "p1", 4)
	requirePathVariable(t, region.PathVariables, "p2", 5)
}

func TestAnalyzeSegmentsRegionsAtSemanticBarriers(t *testing.T) {
	t.Parallel()

	analysis := analyzeCypher(t, `
		MATCH (n:Group)
		WITH n
		MATCH (n)-[:MemberOf]->(m)
		OPTIONAL MATCH (m)-[:MemberOf]->(x)
		RETURN m
	`)

	require.Len(t, analysis.QueryParts, 2)

	firstPart := analysis.QueryParts[0]
	require.Equal(t, QueryPartKindMulti, firstPart.Kind)
	require.Len(t, firstPart.Regions, 1)
	require.Equal(t, []string{"n"}, firstPart.ProjectionDependencies)
	require.Len(t, firstPart.Barriers, 1)
	require.Equal(t, BarrierKindWith, firstPart.Barriers[0].Kind)
	require.Equal(t, []string{"n"}, firstPart.Barriers[0].Dependencies)

	secondPart := analysis.QueryParts[1]
	require.Equal(t, QueryPartKindSingle, secondPart.Kind)
	require.Len(t, secondPart.Regions, 1)
	require.Equal(t, 0, secondPart.Regions[0].StartClause)
	require.Equal(t, 0, secondPart.Regions[0].EndClause)
	require.Len(t, secondPart.Barriers, 2)
	require.Equal(t, BarrierKindOptionalMatch, secondPart.Barriers[0].Kind)
	require.Equal(t, BarrierKindReturn, secondPart.Barriers[1].Kind)
	require.Equal(t, []string{"m"}, secondPart.ProjectionDependencies)
}

func TestAnalysisDiagnosticsAreStable(t *testing.T) {
	t.Parallel()

	var (
		analysis    = analyzeCypher(t, adcsQuery)
		diagnostics = strings.Join(analysis.Diagnostics(), "\n")
	)

	require.Contains(t, diagnostics, "query_part[0] kind=single projection_deps=p1,p2")
	require.Contains(t, diagnostics, "region[0] part=0 clauses=0..2 matches=3")
	require.Contains(t, diagnostics, "bindings=n:node,p1:path,ca:node,d:node,p2:path,ct:node")
	require.Contains(t, diagnostics, "paths=p1,p2")
	require.Contains(t, diagnostics, "predicates=n,ct")
	require.Contains(t, diagnostics, "barrier[0] part=0 clause=3 kind=return deps=p1,p2")
}
