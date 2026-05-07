package v2_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
	v2 "github.com/specterops/dawgs/query/v2"
	"github.com/stretchr/testify/require"
)

func renderPrepared(t *testing.T, preparedQuery *v2.PreparedQuery) string {
	t.Helper()

	cypherQueryStr, err := format.RegularQuery(preparedQuery.Query, false)
	require.NoError(t, err)

	return cypherQueryStr
}

func TestQuery(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.Not(v2.Relationship().Kind().Is(graph.StringKind("test"))),
		v2.Not(v2.Relationship().Kind().IsOneOf(graph.Kinds{graph.StringKind("A"), graph.StringKind("B")})),
		v2.Relationship().Property("rel_prop").LessThanOrEqualTo(1234),
		v2.Relationship().Property("other_prop").Equals(5678),
		v2.Start().Kinds().HasOneOf(graph.Kinds{graph.StringKind("test")}),
	).Update(
		v2.Start().Property("this_prop").Set(1234),
		v2.End().Kinds().Remove(graph.Kinds{graph.StringKind("A"), graph.StringKind("B")}),
	).Delete(
		v2.Start(),
	).Return(
		v2.Relationship(),
		v2.Start().Property("node_prop"),
	).Skip(10).Limit(10).Build()
	require.NoError(t, err)

	cypherQueryStr, err := format.RegularQuery(preparedQuery.Query, false)
	require.NoError(t, err)

	require.Equal(t, "match (s)-[r]->() where not r:test and not (r:A or r:B) and r.rel_prop <= $p0 and r.other_prop = $p1 and s:test set s.this_prop = $p2 remove e:A:B delete s return r, s.node_prop skip 10 limit 10", cypherQueryStr)
	require.Equal(t, map[string]any{
		"p0": 1234,
		"p1": 5678,
		"p2": 1234,
	}, preparedQuery.Parameters)

	preparedQuery, err = v2.New().Create(
		v2.Node().NodePattern(graph.Kinds{graph.StringKind("A")}, cypher.NewParameter("props", map[string]any{})),
	).Build()

	require.NoError(t, err)

	cypherQueryStr, err = format.RegularQuery(preparedQuery.Query, false)
	require.NoError(t, err)

	require.Equal(t, "create (n:A $props)", cypherQueryStr)
	require.Equal(t, map[string]any{
		"props": map[string]any{},
	}, preparedQuery.Parameters)
}

func TestCreateRelationshipWithMatchedEndpoints(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.Start().ID().Equals(1),
		v2.End().ID().Equals(2),
	).Create(
		v2.Relationship().RelationshipPattern(graph.StringKind("A"), v2.NamedParameter("props", map[string]any{"name": "rel"}), graph.DirectionOutbound),
	).Return(
		v2.Relationship().ID(),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (s), (e) where id(s) = $p0 and id(e) = $p1 create (s)-[r:A $props]->(e) return id(r)", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0":    1,
		"p1":    2,
		"props": map[string]any{"name": "rel"},
	}, preparedQuery.Parameters)
}

func TestCreateNodeReturnDoesNotCreateMatch(t *testing.T) {
	preparedQuery, err := v2.New().Create(
		v2.Node().NodePattern(graph.Kinds{graph.StringKind("A")}, v2.NamedParameter("props", map[string]any{"name": "node"})),
	).Return(
		v2.Node().ID(),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "create (n:A $props) return id(n)", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"props": map[string]any{"name": "node"},
	}, preparedQuery.Parameters)
}
