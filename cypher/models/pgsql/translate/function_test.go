package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPathComponentFunctionsResolvePathAliases(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = (a)-[r]->(b) WITH p AS q RETURN nodes(q), relationships(q)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, ".nodes")
	require.Contains(t, formatted, ".edges")
}

func TestNodesFunctionTranslatesBoundPathToNodeArray(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = ()-[]->() RETURN nodes(p)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "nodecomposite[]")
	require.Contains(t, formatted, ".nodes")
}

func TestPathComponentFunctionsTranslateNullArguments(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `RETURN nodes(null), relationships(null)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "(null)::nodecomposite[]")
	require.Contains(t, formatted, "(null)::edgecomposite[]")
}

func TestListSizeGuardsDynamicJSONPropertiesByType(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("TestNode"))

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n:TestNode) RETURN size(n.values)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "case when jsonb_typeof")
	require.Contains(t, formatted, "= 'array' then jsonb_array_length")
	require.Contains(t, formatted, "else null end")
}

func TestTailFunctionDoesNotDuplicatePathComponentExpression(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = ()-[*1..]->() RETURN tail(tail(nodes(p)))`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(formatted, "ordered_edge_ids_to_path"), formatted)
	require.NotContains(t, formatted, "ordered_edges_to_path")
	require.NotContains(t, formatted, "cardinality(((case when")
}

func TestTailPredicateStagesPathComponentExpression(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = ()-[*1..]->() WHERE NONE(n IN TAIL(TAIL(NODES(p))) WHERE true) RETURN p`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(formatted, "ordered_edge_ids_to_path"))
	require.NotContains(t, formatted, "ordered_edges_to_path")
	require.Contains(t, formatted, "lateral (select")
	require.Contains(t, formatted, ".nodes")
}

func TestProjectionStagesPathBeforeReadingComponents(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = ()-[*1..]->() RETURN p, nodes(p), relationships(p)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "lateral (select")
	require.Equal(t, 1, strings.Count(formatted, "ordered_edge_ids_to_path"), formatted)
	require.NotContains(t, formatted, "ordered_edges_to_path")
	require.Contains(t, formatted, ".nodes")
	require.Contains(t, formatted, ".edges")
}

func TestProjectionStagesRepeatedPathComponents(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = ()-[*1..]->() RETURN size(relationships(p)), nodes(p), relationships(p)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "lateral (select")
	require.Equal(t, 1, strings.Count(formatted, "ordered_edge_ids_to_path"), formatted)
	require.NotContains(t, formatted, "ordered_edges_to_path")
	require.NotContains(t, formatted, "from unnest")
	require.Contains(t, formatted, ".nodes")
	require.Contains(t, formatted, ".edges")
}

func TestPathLengthUsesOrderedEdgeIDsWithoutHydration(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = shortestPath((s)-[*1..]->(e)) WHERE id(s) = 1 AND id(e) = 2 RETURN length(p)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, "cardinality(s0.ep0)")
	require.NotContains(t, formatted, "ordered_edge_ids_to_path")
	require.NotContains(t, formatted, "ordered_edges_to_path")
	require.NotContains(t, formatted, "from unnest")
}

func TestIDOnlyTerminalProjectionCarriesScalarID(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("TestNode"))

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH ()-[]->(e:TestNode) RETURN id(e)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "n1.id as n1")
	require.Contains(t, formatted, "select s0.n1 as \"id(e)\"")
	require.NotContains(t, formatted, "(n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1")
	require.Contains(t, formatted, "n1.kind_ids operator")
}

func TestIDOnlyTerminalProjectionRetainsCompositeForMixedUse(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH ()-[]->(e) RETURN id(e), e.name`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "(n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1")
	require.Contains(t, formatted, "(s0.n1).id")
	require.Contains(t, formatted, "(s0.n1).properties")
}

func TestIDOnlyTerminalProjectionRetainsCompositeForLaterPatternReuse(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH ()-[]->(e) MATCH (e)-[]->() RETURN id(e)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "(n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1")
	require.NotContains(t, formatted, "n1.id as n1")
}

func TestIDOnlyTerminalProjectionRetainsCompositeForObservedPath(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = ()-[*1..]->(e) WHERE id(e) = 2 RETURN p`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Contains(t, formatted, "(n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1")
	require.Contains(t, formatted, "ordered_edge_ids_to_path")
}

func TestBoundPairShortestPathUsesStableSingletonArrays(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	translateQuery := func(cypherQuery string) (Result, string) {
		query, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
		require.NoError(t, err)
		translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
		require.NoError(t, err)
		formatted, err := Translated(translation)
		require.NoError(t, err)
		return translation, formatted
	}

	first, firstSQL := translateQuery(`MATCH p = shortestPath((s)-[*1..]->(e)) WHERE id(s) = 1 AND id(e) = 2 RETURN p LIMIT 1`)
	second, secondSQL := translateQuery(`MATCH p = shortestPath((s)-[*1..]->(e)) WHERE id(s) = 41 AND id(e) = 42 RETURN p LIMIT 1`)

	require.Equal(t, firstSQL, secondSQL)
	require.Contains(t, firstSQL, "::int8[]")
	require.NotContains(t, firstSQL, "insert into pg_temp.bsp_pair_filter")
	require.NotContains(t, firstSQL, "traversal_pair_filter")
	require.Contains(t, firstSQL, "limit 1")
	require.Contains(t, firstSQL, "with singleton_endpoints as")
	require.Contains(t, firstSQL, "array [singleton_endpoints.root_id]::int8[]")
	require.Contains(t, firstSQL, "array [singleton_endpoints.terminal_id]::int8[]")
	require.NotContains(t, firstSQL, "n0.id = 1")
	require.NotContains(t, secondSQL, "n0.id = 41")
	var firstEndpointValues, secondEndpointValues []any
	for _, value := range first.Parameters {
		if _, isString := value.(string); !isString {
			firstEndpointValues = append(firstEndpointValues, value)
		}
	}
	for _, value := range second.Parameters {
		if _, isString := value.(string); !isString {
			secondEndpointValues = append(secondEndpointValues, value)
		}
	}
	require.ElementsMatch(t, []any{int64(1), int64(2)}, firstEndpointValues)
	require.ElementsMatch(t, []any{int64(41), int64(42)}, secondEndpointValues)

	var hasRootArraySeed, hasTerminalArraySeed bool
	for _, value := range first.Parameters {
		fragment, isString := value.(string)
		if !isString {
			continue
		}
		hasRootArraySeed = hasRootArraySeed || strings.Contains(fragment, "unnest($1::int8[])")
		hasTerminalArraySeed = hasTerminalArraySeed || strings.Contains(fragment, "unnest($2::int8[])")
	}
	require.True(t, hasRootArraySeed)
	require.True(t, hasTerminalArraySeed)
}

func TestRelationshipEndpointFunctionsUseEdgeCompositeArguments(t *testing.T) {
	t.Parallel()

	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH ()-[r]->() RETURN startNode(r), endNode(r)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	normalized := strings.Join(strings.Fields(formatted), " ")

	require.Contains(t, normalized, "start_node(((s0.e0).id, (s0.e0).start_id, (s0.e0).end_id, (s0.e0).kind_id, (s0.e0).properties)::edgecomposite)")
	require.Contains(t, normalized, "end_node(((s0.e0).id, (s0.e0).start_id, (s0.e0).end_id, (s0.e0).kind_id, (s0.e0).properties)::edgecomposite)")
	require.NotContains(t, normalized, "start_node(s0.e0)")
	require.NotContains(t, normalized, "end_node(s0.e0)")
}

func TestPathRelationshipPredicateEndpointFunctionUsesEdgeCompositeArguments(t *testing.T) {
	t.Parallel()

	query, err := frontend.ParseCypher(frontend.NewContext(), `
MATCH p = shortestPath((s:Group)-[:MemberOf*1..]->(d:Group))
WHERE NONE(r IN relationships(p) WHERE type(r) = 'MemberOf' AND startNode(r).name = 'blocked-session-host')
RETURN p
`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	normalized := strings.Join(strings.Fields(formatted), " ")

	require.Contains(t, normalized, "from edge i0")
	require.Contains(t, normalized, "start_node((i0.id, i0.start_id, i0.end_id, i0.kind_id, i0.properties)::edgecomposite)")
	require.NotContains(t, normalized, "start_node(i0)")
}

func TestPrepareCollectExpressionMissingBindingErrorNamesArgument(t *testing.T) {
	t.Parallel()

	_, _, err := prepareCollectExpression(NewScope(), pgsql.Identifier("missing"), cypher.CollectFunction)

	require.EqualError(t, err, "binding not found for collect function argument missing")
}

func TestCollectMembershipOnlyProjectionUsesIDs(t *testing.T) {
	t.Parallel()

	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (s)
		WITH collect(s) AS exclude
		MATCH (c)
		WHERE NOT c IN exclude
		RETURN c
	`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	normalized := strings.Join(strings.Fields(formatted), " ")

	require.Contains(t, normalized, "array_agg((n0).id)")
	require.Contains(t, normalized, "array []::int8[]")
	require.Contains(t, normalized, "not n1.id = any (s0.")
	require.NotContains(t, normalized, "array []::nodecomposite[]")
	requireOptimizationLowering(t, translation.Optimization, "CollectIDMembership")
}

func TestReturnedCollectNodeKeepsCompositeArray(t *testing.T) {
	t.Parallel()

	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (s) RETURN collect(s) AS nodes`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	normalized := strings.Join(strings.Fields(formatted), " ")

	require.Contains(t, normalized, "array []::nodecomposite[]")
	require.NotContains(t, normalized, "array_agg((n0).id)")
	requireNoOptimizationLowering(t, translation.Optimization, "CollectIDMembership")
}
