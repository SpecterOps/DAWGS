package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
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

func TestTailFunctionDoesNotDuplicatePathComponentExpression(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = ()-[*1..]->() RETURN tail(tail(nodes(p)))`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(formatted, "ordered_edges_to_path"), formatted)
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
	require.Equal(t, 1, strings.Count(formatted, "ordered_edges_to_path"))
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
	require.Equal(t, 1, strings.Count(formatted, "ordered_edges_to_path"), formatted)
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
	require.Equal(t, 1, strings.Count(formatted, "ordered_edges_to_path"), formatted)
	require.Equal(t, 1, strings.Count(formatted, "from unnest"), formatted)
	require.Contains(t, formatted, ".nodes")
	require.Contains(t, formatted, ".edges")
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
