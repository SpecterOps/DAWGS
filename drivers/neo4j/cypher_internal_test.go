package neo4j

import (
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
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
		digester = xxhash.New()

		// Digests 1 and 2 should have the same update key as the query that updates both
		// entities should be structurally the same
		digest1 = nodeToNodeUpdateKey(digester,
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Fool"), graph.StringKind("Asset")},
				graph.Kinds{},
			),
		)
		digest2 = nodeToNodeUpdateKey(digester,
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Asset"), graph.StringKind("Fool")},
				graph.Kinds{},
			),
		)

		// Digest 3 and 4 should have a distinct update key from digests 1 and 2 becaus the query
		// structure must be different
		digest3 = nodeToNodeUpdateKey(digester,
			newTestingNode(
				graph.Kinds{graph.StringKind("Jim"), graph.StringKind("User")},
				graph.Kinds{graph.StringKind("Fool")},
				graph.Kinds{graph.StringKind("User")},
			),
		)
		digest4 = nodeToNodeUpdateKey(digester,
			newTestingNode(
				graph.Kinds{graph.StringKind("User"), graph.StringKind("Tom")},
				graph.Kinds{graph.StringKind("Fool")},
				graph.Kinds{graph.StringKind("User")},
			),
		)
	)

	assert.Equal(t, digest1, digest2)
	assert.Equal(t, digest3, digest4)

	assert.NotEqual(t, digest1, digest3)
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
			"unwind $p as p match (n) where id(n) = p.id set n += p.properties, n:Fool, n:Asset;",
			"unwind $p as p match (n) where id(n) = p.id set n += p.properties, n:Fool remove n:User;",
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
	require.Equal(t, "BaseUserobjectidMemberOfBaseGroupobjectid", updateKey)

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
	require.Equal(t, "BaseUserobjectidGenericAllBaseGroupobjectid", updateKey)
}

func Test_StripCypher(t *testing.T) {
	var (
		query = "match (u1:User {domain: \"DOMAIN1\"}), (u2:User {domain: \"DOMAIN2\"}) where u1.samaccountname <> \"krbtgt\" and u1.samaccountname = u2.samaccountname with u2 match p1 = (u2)-[*1..]->(g:Group) with p1 match p2 = (u2)-[*1..]->(g:Group) return p1, p2"
	)

	result := stripCypherQuery(query)

	require.Equalf(t, false, strings.Contains(result, "DOMAIN1"), "Cypher query not sanitized. Contains sensitive value: %s", result)
	require.Equalf(t, false, strings.Contains(result, "DOMAIN2"), "Cypher query not sanitized. Contains sensitive value: %s", result)
}
