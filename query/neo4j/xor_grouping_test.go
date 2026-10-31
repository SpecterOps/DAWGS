package neo4j_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/query/neo4j"
	"github.com/stretchr/testify/require"
)

func TestXorGrouping(t *testing.T) {
	a, b, c := query.Literal(true), query.Literal(false), query.Literal(false)
	for _, tc := range []struct {
		name      string
		predicate graph.Criteria
		expected  string
	}{
		{"left", query.And(query.Xor(a, b), c), "(true xor false) and false"},
		{"right", query.And(c, query.Xor(a, b)), "false and (true xor false)"},
		{"nested", query.And(query.And(query.Xor(a, b), c), a), "(true xor false) and false and true"},
		{"negated", query.Not(query.And(query.Xor(a, b), c)), "not ((true xor false) and false)"},
		{"three operands", query.And(query.Xor(a, b, a), c), "(true xor false xor true) and false"},
		{"standalone", query.Xor(a, b), "true xor false"},
		{"and inside xor", query.Xor(query.And(a, b), c), "true and false xor false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := neo4j.NewQueryBuilder(query.SinglePartQuery(query.Where(tc.predicate), query.Returning(query.Node())))
			require.NoError(t, builder.Prepare())
			rendered, err := builder.Render()
			require.NoError(t, err)
			require.Equal(t, "match (n) where "+tc.expected+" return n", rendered)
		})
	}
}
