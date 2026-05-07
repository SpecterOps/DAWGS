package v2_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
	v2 "github.com/specterops/dawgs/query/v2"
	"github.com/stretchr/testify/require"
)

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

	require.Equal(t, "match (s)-[r]->() where not r:test and not (r:A or r:B) and r.rel_prop <= 1234 and r.other_prop = 5678 and s:test set s.this_prop = 1234 remove e:A:B delete s return r, s.node_prop skip 10 limit 10", cypherQueryStr)

	preparedQuery, err = v2.New().Create(
		v2.Node().NodePattern(graph.Kinds{graph.StringKind("A")}, cypher.NewParameter("props", map[string]any{})),
	).Build()

	require.NoError(t, err)

	cypherQueryStr, err = format.RegularQuery(preparedQuery.Query, false)
	require.NoError(t, err)

	require.Equal(t, "create (n:A $props)", cypherQueryStr)
}
