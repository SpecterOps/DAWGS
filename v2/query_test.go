package v2_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
	v2 "github.com/specterops/dawgs/v2"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	preparedQuery, err := v2.Query().Where(
		v2.Not(v2.Relationship().HasKind(graph.StringKind("test"))),
		v2.Not(v2.Relationship().HasKindIn(graph.Kinds{graph.StringKind("A"), graph.StringKind("B")})),
		v2.Relationship().Property("rel_prop").LessThanOrEqualTo(1234),
		v2.Relationship().Property("other_prop").Equals(5678),
		v2.Start().HasKindIn(graph.Kinds{graph.StringKind("test")}),
	).Update(
		v2.Start().Property("this_prop").Set(1234),
		v2.End().RemoveKinds(graph.Kinds{graph.StringKind("A"), graph.StringKind("B")}),
	).Delete(
		v2.Start(),
	).Return(
		v2.Relationship(),
		v2.Start().Property("node_prop"),
	).Skip(10).Limit(10).Build()
	require.NoError(t, err)

	cypherQueryStr, err := format.RegularQuery(preparedQuery.Query, false)
	require.NoError(t, err)

	require.Equal(t, "match (s)-[r]->() where type(r) <> 'test' and not type(r) in ['A', 'B'] and r.rel_prop <= 1234 and r.other_prop = 5678 and s:test set s.this_prop = 1234 remove e:A:B delete s return r, s.node_prop skip 10 limit 10", cypherQueryStr)

	preparedQuery, err = v2.Query().Create(
		v2.Node().NodePattern(graph.Kinds{graph.StringKind("A")}, cypher.NewParameter("props", map[string]any{})),
	).Build()

	require.NoError(t, err)

	cypherQueryStr, err = format.RegularQuery(preparedQuery.Query, false)
	require.NoError(t, err)

	require.Equal(t, "create (n:A $props)", cypherQueryStr)

	preparedQuery, err = v2.Query().Where(
		v2.Start().ID().Equals(1234),
	).Create(
		v2.Relationship().RelationshipPattern(graph.StringKind("A"), cypher.NewParameter("props", map[string]any{}), graph.DirectionOutbound),
	).Build()

	require.NoError(t, err)

	cypherQueryStr, err = format.RegularQuery(preparedQuery.Query, false)
	require.NoError(t, err)

	require.Equal(t, "match (s), (e) where id(s) = 1234 create (s)-[r:A $props]->(e)", cypherQueryStr)
}
