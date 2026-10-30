//go:build manual_integration

package integration

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	queryneo4j "github.com/specterops/dawgs/query/neo4j"
	"github.com/stretchr/testify/require"
)

// Exercise the builder rewrite against exactly the same fixtures and state
// assertions as the backend-equivalent text template corpus.
func TestBuilderRelationshipKindConjunction(t *testing.T) {
	source, err := os.ReadFile("testdata/templates/relationship_kind_conjunction_shapes.json")
	require.NoError(t, err)
	var corpus cypherTemplateFile
	require.NoError(t, json.Unmarshal(source, &corpus))
	nodeKinds, edgeKinds := cypherTemplateKinds([]cypherTemplateFile{corpus})
	session := Open(t, Options{ExtraNodeKinds: nodeKinds, ExtraEdgeKinds: edgeKinds, CleanupMode: CleanupGraph})
	family := corpus.Families[0]
	kind := func(names ...string) *cypher.KindMatcher {
		kinds := make(graph.Kinds, len(names))
		for i, name := range names {
			kinds[i] = graph.StringKind("Conj" + name)
		}
		return query.KindIn(query.Relationship(), kinds...)
	}
	predicates := map[string]graph.Criteria{
		"disjoint":       query.And(kind("R"), kind("T")),
		"overlap":        query.And(kind("R", "T"), kind("T", "U")),
		"identical":      query.And(kind("T"), kind("T")),
		"nested":         query.And(kind("R", "T"), query.And(kind("T", "U"), kind("T"))),
		"three disjoint": query.And(kind("R", "T"), kind("T", "U"), kind("U")),
		"or":             query.Or(kind("R"), kind("T")),
		"not":            query.And(kind("R", "T"), query.Not(kind("R"))),
	}
	modes := []bool{true}
	if _, ok := session.DB.(*pg.Driver); ok {
		modes = append(modes, false)
	}
	for _, optimized := range modes {
		previous := pg.SetOptimizedTranslation(optimized)
		for _, variant := range family.Variants {
			t.Run(variant.Name+map[bool]string{true: "/optimized", false: "/generic"}[optimized], func(t *testing.T) {
				shape, action, found := strings.Cut(variant.Name, "/")
				require.True(t, found)
				predicate, found := predicates[shape]
				require.True(t, found)
				criteria := []graph.Criteria{query.Where(query.And(
					query.Equals(query.StartProperty("name"), "root"),
					query.Equals(query.EndProperty("name"), "target"),
					query.Equals(query.RelationshipProperty("role"), "candidate"), predicate,
				))}
				if action != "read" {
					criteria = append(criteria, query.SetProperty(query.RelationshipProperty("marked"), true))
				}
				if action != "set without return" {
					criteria = append(criteria, query.Returning(query.Relationship()))
				}
				builder := queryneo4j.NewQueryBuilder(query.SinglePartQuery(criteria...))
				require.NoError(t, builder.Prepare())
				rendered, err := builder.Render()
				require.NoError(t, err)
				runWithFixture(t, session.Ctx, session.DB, testCase{Fixture: family.Fixture, Cypher: rendered, Params: builder.Parameters, PostAssertions: variant.PostAssertions}, parseAssertion(t, variant.Assert))
			})
		}
		pg.SetOptimizedTranslation(previous)
	}
}

func TestPostgreSQLRelationshipKindConjunctionCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "relationship_kind_conjunction_shapes")
}
