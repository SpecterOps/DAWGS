package neo4j_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/query/neo4j"
	"github.com/stretchr/testify/require"
)

func TestRelationshipKindConjunction(t *testing.T) {
	kind := func(names ...string) *cypher.KindMatcher {
		return query.KindIn(query.Relationship(), graph.StringsToKinds(names)...)
	}
	for _, tc := range []struct {
		name           string
		predicate      graph.Criteria
		pattern, where string
	}{
		{"disjoint", query.And(kind("R"), kind("T")), "R", " where false"},
		{"overlap", query.And(kind("R", "T"), kind("T", "U")), "T", ""},
		{"reverse overlap", query.And(kind("T", "U"), kind("R", "T")), "T", ""},
		{"identical", query.And(kind("R"), kind("R")), "R", ""},
		{"three predicates", query.And(kind("R", "T"), query.And(kind("T", "U"), kind("U"))), "T", " where false"},
		{"nested conjunction", query.And(kind("R", "T"), query.And(kind("T", "U"), kind("T"))), "T", ""},
		{"disjunction", query.Or(kind("R"), kind("T")), "", " where (r:R or r:T)"},
		{"xor", query.Xor(kind("R"), kind("T")), "", " where r:R xor r:T"},
		{"negation", query.And(kind("R", "T"), query.Not(kind("R"))), "R|T", " where not (r:R)"},
	} {
		for _, action := range []string{"read", "set", "delete", "shortest"} {
			// OR/NOT on a variable-length relationship list have separate semantics.
			if action == "shortest" && (tc.name == "disjunction" || tc.name == "xor" || tc.name == "negation") {
				continue
			}
			t.Run(tc.name+"/"+action, func(t *testing.T) {
				criteria := []graph.Criteria{query.Where(tc.predicate)}
				suffix := " return r"
				switch action {
				case "set":
					criteria = append(criteria, query.SetProperty(query.RelationshipProperty("marked"), true))
					suffix = " set r.marked = $p0 return r"
				case "delete":
					criteria = append(criteria, query.Delete(query.Relationship()))
					suffix = " delete r return r"
				}
				criteria = append(criteria, query.Returning(query.Relationship()))
				builder := neo4j.NewQueryBuilder(query.SinglePartQuery(criteria...))
				prefix := "match ()-[r"
				tail := "]->()"
				if tc.pattern != "" {
					prefix += ":" + tc.pattern
				}
				if action == "shortest" {
					require.NoError(t, builder.PrepareAllShortestPaths())
					prefix = "match p = allShortestPaths(()-[r:" + tc.pattern
					tail = "*]->())"
				} else {
					require.NoError(t, builder.Prepare())
				}
				rendered, err := builder.Render()
				require.NoError(t, err)
				require.Equal(t, prefix+tail+tc.where+suffix, rendered)
			})
		}
	}
}
