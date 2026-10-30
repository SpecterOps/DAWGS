//go:build manual_integration

package integration

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	queryneo4j "github.com/specterops/dawgs/query/neo4j"
	"github.com/stretchr/testify/require"
)

// Exercise the builder rewrite against exactly the same fixtures and state
// assertions as the backend-equivalent text template corpus.
func TestBuilderXorGrouping(t *testing.T) {
	source, err := os.ReadFile("testdata/templates/xor_grouping_shapes.json")
	require.NoError(t, err)
	var corpus cypherTemplateFile
	require.NoError(t, json.Unmarshal(source, &corpus))
	nodeKinds, edgeKinds := cypherTemplateKinds([]cypherTemplateFile{corpus})
	session := Open(t, Options{ExtraNodeKinds: nodeKinds, ExtraEdgeKinds: edgeKinds, CleanupMode: CleanupGraph})
	family := corpus.Families[0]
	a := query.Equals(query.NodeProperty("a"), true)
	b := query.Equals(query.NodeProperty("b"), true)
	c := query.Equals(query.NodeProperty("c"), true)
	predicates := map[string]graph.Criteria{
		"left":    query.And(query.Xor(a, b), c),
		"right":   query.And(c, query.Xor(a, b)),
		"negated": query.Not(query.And(query.Xor(a, b), c)),
		"three":   query.And(query.Xor(a, b, c), a),
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
				criteria := []graph.Criteria{query.Where(query.And(query.Kind(query.Node(), graph.StringKind("XorNode")), predicate))}
				if action != "read" {
					criteria = append(criteria, query.SetProperty(query.NodeProperty("marked"), true))
				}
				if action != "set without return" {
					criteria = append(criteria, query.Returning(query.Node()))
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

func TestPostgreSQLXorGroupingCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "xor_grouping_shapes")
}
