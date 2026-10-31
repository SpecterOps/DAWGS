package neo4j

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestingNode(existingKinds, addedKinds, deletedKinds graph.Kinds) *graph.Node {
	node := graph.PrepareNode(
		graph.AsProperties(map[string]any{
			"name":                "tomfoolery jim",
			"tomfoolery_detected": true,
		}),
		existingKinds...,
	)

	node.AddKinds(addedKinds...)
	node.DeleteKinds(deletedKinds...)

	return node
}

func Test_nodeToNodeUpdateKey(t *testing.T) {
	var (
		// Digests 1 and 2 should have the same update key as the query that updates both
		// entities should be structurally the same
		digest1 = nodeToNodeUpdateKey(
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Fool"), graph.StringKind("Asset")},
				graph.Kinds{},
			),
		)
		digest2 = nodeToNodeUpdateKey(
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Asset"), graph.StringKind("Fool")},
				graph.Kinds{},
			),
		)

		// Digest 3 and 4 should have a distinct update key from digests 1 and 2 becaus the query
		// structure must be different
		digest3 = nodeToNodeUpdateKey(
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Fool")},
				graph.Kinds{graph.StringKind("User")},
			),
		)
		digest4 = nodeToNodeUpdateKey(
			newTestingNode(
				graph.Kinds{graph.StringKind("User"), graph.StringKind("Tom")},
				graph.Kinds{graph.StringKind("Fool")},
				graph.Kinds{graph.StringKind("User")},
			),
		)

		// Digest 5 should deviate from digest 4 because it no longer includes the User kind in the deleted set
		digest5 = nodeToNodeUpdateKey(
			newTestingNode(
				graph.Kinds{graph.StringKind("User"), graph.StringKind("Tom")},
				graph.Kinds{graph.StringKind("Fool")},
				graph.Kinds{},
			),
		)
	)

	assert.Equal(t, digest1, digest2)
	assert.Equal(t, digest3, digest4)

	assert.NotEqual(t, digest1, digest3)
	assert.NotEqual(t, digest4, digest5)
}

func TestNodeUpdateBatchSeparatesOperationsAndKindBoundaries(t *testing.T) {
	for _, pair := range [][2]*graph.Node{
		{newTestingNode(nil, graph.Kinds{graph.StringKind("A")}, nil), newTestingNode(graph.Kinds{graph.StringKind("A")}, nil, graph.Kinds{graph.StringKind("A")})},
		{newTestingNode(nil, graph.Kinds{graph.StringKind("AB"), graph.StringKind("C")}, nil), newTestingNode(nil, graph.Kinds{graph.StringKind("A"), graph.StringKind("BC")}, nil)},
	} {
		queries, params := cypherBuildNodeUpdateQueryBatch(pair[:])
		require.Len(t, queries, 2)
		require.Len(t, params, 2)
		for _, p := range params {
			require.Len(t, p["p"], 1)
		}
	}
}

func TestUpdateByBatchSeparatesShapeFields(t *testing.T) {
	require.NotEqual(t, newUpdateKey(graph.StringKind("AB"), []string{"C"}, nil), newUpdateKey(graph.StringKind("A"), []string{"BC"}, nil))
	require.NotEqual(t, newUpdateKey(graph.StringKind("A"), []string{"B"}, graph.Kinds{graph.StringKind("C")}), newUpdateKey(graph.StringKind("B"), []string{"A"}, graph.Kinds{graph.StringKind("C")}))

	base := graph.NodeUpdate{Node: newTestingNode(graph.Kinds{graph.StringKind("Base")}, nil, nil), IdentityKind: graph.StringKind("Base"), IdentityProperties: []string{"name"}}
	removed := base
	removed.Node = newTestingNode(graph.Kinds{graph.StringKind("Base"), graph.StringKind("A")}, nil, graph.Kinds{graph.StringKind("A")})
	queries, params := cypherBuildNodeUpdateQueryByBatch([]graph.NodeUpdate{base, removed})
	require.Len(t, queries, 2)
	require.Len(t, params, 2)
}

func TestNodeUpdateBatchPropertyDeletionParameters(t *testing.T) {
	deleted := graph.NewProperties().Set("keep", true).Set("delete", "old").Delete("delete").Delete("absent")
	reset := graph.NewProperties().Set("delete", "old").Delete("delete").Set("delete", "new")
	nodes := []*graph.Node{
		graph.NewNode(1, deleted),
		graph.NewNode(2, reset),
		graph.NewNode(3, graph.NewProperties()),
	}
	queries, params := cypherBuildNodeUpdateQueryBatch(nodes)
	require.Equal(t, []string{"unwind $p as p match (n) where id(n) = p.id set n += p.properties;"}, queries)
	require.Len(t, params, 1, "deletions are values in each row, not shared query structure")
	rows := params[0]["p"].([]map[string]any)
	require.Equal(t, map[string]any{"keep": true, "delete": nil, "absent": nil}, rows[0]["properties"])
	require.Equal(t, map[string]any{"delete": "new"}, rows[1]["properties"])
	require.Empty(t, rows[2]["properties"])
	require.Equal(t, map[string]any{"keep": true}, deleted.Map, "batch preparation must not mutate source properties")
}

func Test_cypherBuildNodeUpdateQueryBatch(t *testing.T) {
	var (
		nodes = []*graph.Node{
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Fool"), graph.StringKind("Asset")},
				graph.Kinds{},
			),
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Asset"), graph.StringKind("Fool")},
				graph.Kinds{},
			),
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Fool")},
				graph.Kinds{graph.StringKind("User")},
			),
			newTestingNode(
				graph.Kinds{graph.StringKind("User"), graph.StringKind("Tom")},
				graph.Kinds{graph.StringKind("Fool")},
				graph.Kinds{graph.StringKind("User")},
			),
		}

		expectedQueries = []string{
			"unwind $p as p match (n) where id(n) = p.id set n += p.properties, n:`Fool`, n:`Asset`;",
			"unwind $p as p match (n) where id(n) = p.id set n += p.properties, n:`Fool` remove n:`User`;",
		}

		queries, parameters = cypherBuildNodeUpdateQueryBatch(nodes)
	)

	for _, expectedQuery := range expectedQueries {
		assert.Contains(t, queries, expectedQuery)
	}

	assert.Equal(t, 2, len(parameters))
}

func Test_relUpdateKey(t *testing.T) {
	updateKey := relUpdateKey(graph.RelationshipUpdate{
		Relationship: &graph.Relationship{
			ID:         1,
			StartID:    1,
			EndID:      2,
			Kind:       graph.StringKind("MemberOf"),
			Properties: graph.NewProperties(),
		},
		Start: &graph.Node{
			ID:    1,
			Kinds: graph.Kinds{graph.StringKind("User")},
			Properties: graph.AsProperties(map[string]any{
				"objectid": "OID-1",
			}),
		},
		StartIdentityKind:       graph.StringKind("Base"),
		StartIdentityProperties: []string{"objectid"},
		End: &graph.Node{
			ID:    2,
			Kinds: graph.Kinds{graph.StringKind("Group")},
			Properties: graph.AsProperties(map[string]any{
				"objectid": "OID-2",
			}),
		},
		EndIdentityKind:       graph.StringKind("Base"),
		EndIdentityProperties: []string{"objectid"},
	})

	// Order must be preserved to make each key unique. This is required as the batch insert is authored as an unwound
	// merge statement. The update key groups like updates so that the generated query can address an entire batch of
	// upsert entries at-once:
	//
	// unwind $p as p merge (s:Base {objectid: p.s.objectid}) merge (e:Base {objectid: p.e.objectid}) merge (s)-[r:MemberOf]->(e) set s += p.s, e += p.e, r += p.r, s:User, e:Group
	require.NotEmpty(t, updateKey)
	firstKey := updateKey

	updateKey = relUpdateKey(graph.RelationshipUpdate{
		Relationship: &graph.Relationship{
			ID:         1,
			StartID:    1,
			EndID:      2,
			Kind:       graph.StringKind("GenericAll"),
			Properties: graph.NewProperties(),
		},
		Start: &graph.Node{
			ID:    1,
			Kinds: graph.Kinds{graph.StringKind("User")},
			Properties: graph.AsProperties(map[string]any{
				"objectid": "OID-1",
			}),
		},
		StartIdentityKind:       graph.StringKind("Base"),
		StartIdentityProperties: []string{"objectid"},
		End: &graph.Node{
			ID:    2,
			Kinds: graph.Kinds{graph.StringKind("Group")},
			Properties: graph.AsProperties(map[string]any{
				"objectid": "OID-2",
			}),
		},
		EndIdentityKind:       graph.StringKind("Base"),
		EndIdentityProperties: []string{"objectid"},
	})

	// unwind $p as p merge (s:Base {objectid: p.s.objectid}) merge (e:Base {objectid: p.e.objectid}) merge (s)-[r:GenericAll]->(e) set s += p.s, e += p.e, r += p.r, s:User, e:Group
	require.NotEqual(t, firstKey, updateKey)
}

func Test_StripCypher(t *testing.T) {
	var (
		query = "match (u1:User {domain: \"DOMAIN1\"}), (u2:User {domain: \"DOMAIN2\"}) where u1.samaccountname <> \"krbtgt\" and u1.samaccountname = u2.samaccountname with u2 match p1 = (u2)-[*1..]->(g:Group) with p1 match p2 = (u2)-[*1..]->(g:Group) return p1, p2"
	)

	result := stripCypherQuery(query)

	require.Equalf(t, false, strings.Contains(result, "DOMAIN1"), "Cypher query not sanitized. Contains sensitive value: %s", result)
	require.Equalf(t, false, strings.Contains(result, "DOMAIN2"), "Cypher query not sanitized. Contains sensitive value: %s", result)
}
