package optimize

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

const fixedSuffixExpansionQuery = `
MATCH (root:ExpansionRoot)
WHERE root.root_key = 'root'
MATCH p1 = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
MATCH p2 = (root)-[:Expand*0..16]->()-[:OptionA|OptionB|OptionC]->(predicate:PredicateNode)-[:JoinSuffix]->(head)-[:HeadToBridge|HeadToAlternateBridge*1..16]->(:BridgeNode)-[:ReachTerminal]->(terminal)
WHERE predicate.eligible = true
AND predicate.requires_review = false
AND predicate.allows_direct = true
AND (predicate.version = 1 OR predicate.required_approvals = 0)
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

func requirePathVariable(t *testing.T, pathVariables []PathVariable, symbol string, relationshipCount int, expectedVariableLength bool) {
	t.Helper()

	for _, pathVariable := range pathVariables {
		if pathVariable.Symbol == symbol {
			require.Equal(t, relationshipCount, pathVariable.RelationshipCount)
			require.Equal(t, expectedVariableLength, pathVariable.VariableLength)
			return
		}
	}

	t.Fatalf("expected path variable %s in %#v", symbol, pathVariables)
}

func TestAnalyzeIdentifiesEligibleFixedSuffixExpansionRegion(t *testing.T) {
	t.Parallel()

	analysis := analyzeCypher(t, fixedSuffixExpansionQuery)

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
	require.Equal(t, []string{"root"}, region.Predicates[0].Dependencies)
	require.Equal(t, []string{"predicate"}, region.Predicates[1].Dependencies)

	requireBinding(t, region.Bindings, "root", BindingKindNode)
	requireBinding(t, region.Bindings, "head", BindingKindNode)
	requireBinding(t, region.Bindings, "predicate", BindingKindNode)
	requireBinding(t, region.Bindings, "terminal", BindingKindNode)
	requireBinding(t, region.Bindings, "p1", BindingKindPath)
	requireBinding(t, region.Bindings, "p2", BindingKindPath)

	requirePathVariable(t, region.PathVariables, "p1", 4, true)
	requirePathVariable(t, region.PathVariables, "p2", 5, true)
}

func TestAnalyzeReadingClausesSkipsNilClauses(t *testing.T) {
	t.Parallel()

	regions, barriers := analyzeReadingClauses(0, []*cypher.ReadingClause{nil})

	require.Empty(t, regions)
	require.Empty(t, barriers)
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
		analysis    = analyzeCypher(t, fixedSuffixExpansionQuery)
		diagnostics = strings.Join(analysis.Diagnostics(), "\n")
	)

	require.Contains(t, diagnostics, "query_part[0] kind=single projection_deps=p1,p2")
	require.Contains(t, diagnostics, "region[0] part=0 clauses=0..2 matches=3")
	require.Contains(t, diagnostics, "bindings=root:node,p1:path,head:node,terminal:node,p2:path,predicate:node")
	require.Contains(t, diagnostics, "paths=p1,p2")
	require.Contains(t, diagnostics, "predicates=root,predicate")
	require.Contains(t, diagnostics, "barrier[0] part=0 clause=3 kind=return deps=p1,p2")
}
