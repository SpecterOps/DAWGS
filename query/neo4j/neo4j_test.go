package neo4j_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/query/neo4j"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

var (
	// SystemTags is the synthetic system-tags property used by query-builder tests.
	SystemTags = "system_tags"

	// User is the user node kind used by query-builder fixtures.
	User = graph.StringKind("User")

	// Domain is the domain node kind used by query-builder fixtures.
	Domain = graph.StringKind("Domain")

	// Computer is the computer node kind used by query-builder fixtures.
	Computer = graph.StringKind("Computer")

	// Group is the group node kind used by query-builder fixtures.
	Group = graph.StringKind("Group")

	// HasSession is the relationship kind used by session-path fixtures.
	HasSession = graph.StringKind("HasSession")

	// GenericWrite is the relationship kind used by generic-write fixtures.
	GenericWrite = graph.StringKind("GenericWrite")
)

// QueryOutputAssertion contains one accepted query rendering and parameter map.
type QueryOutputAssertion struct {
	// Query is the expected rendered Cypher text.
	Query string

	// Parameters contains the expected query parameters.
	Parameters map[string]any
}

// expectAnalysisError returns an assertion that requires query preparation to report an analysis error.
func expectAnalysisError(rawQuery *cypher.RegularQuery) func(t *testing.T) {
	return func(t *testing.T) {
		require.NotNil(t, neo4j.NewQueryBuilder(rawQuery).Prepare())
	}
}

// assertQueryShortestPathResult prepares a shortest-path query and compares its rendered text and optional parameters.
func assertQueryShortestPathResult(rawQuery *cypher.RegularQuery, expectedOutput string, expectedParameters ...map[string]any) func(t *testing.T) {
	return func(t *testing.T) {
		builder := neo4j.NewQueryBuilder(rawQuery)

		// Validate that building the query didn't throw an error
		require.Nil(t, builder.PrepareAllShortestPaths())

		if len(expectedParameters) == 1 {
			require.Equal(t, expectedParameters[0], builder.Parameters)
		}

		output, err := builder.Render()

		require.Nil(t, err)
		require.Equal(t, expectedOutput, output)
	}
}

// assertQueryResult prepares a query and compares its rendered text and optional parameters.
func assertQueryResult(rawQuery *cypher.RegularQuery, expectedOutput string, expectedParameters ...map[string]any) func(t *testing.T) {
	return func(t *testing.T) {
		var (
			builder    = neo4j.NewQueryBuilder(rawQuery)
			prepareErr = builder.Prepare()
		)

		// Validate that building the query didn't throw an error
		if prepareErr != nil {
			require.Nilf(t, prepareErr, prepareErr.Error())
		}

		if len(expectedParameters) == 1 {
			require.Equal(t, expectedParameters[0], builder.Parameters)
		}

		output, err := builder.Render()

		require.Nil(t, err)
		require.Equal(t, expectedOutput, output)
	}
}

// assertOneOfQueryResult requires a prepared query to match one accepted rendering and parameter set.
func assertOneOfQueryResult(rawQuery *cypher.RegularQuery, expectations []QueryOutputAssertion) func(t *testing.T) {
	return func(t *testing.T) {
		builder := neo4j.NewQueryBuilder(rawQuery)

		// Validate that building the query didn't throw an error
		require.Nil(t, builder.Prepare())

		output, err := builder.Render()
		require.Nil(t, err)

		var matchingExpectation *QueryOutputAssertion

		for _, expectation := range expectations {
			if expectation.Query == output {
				matchingExpectation = &expectation
				break
			}
		}

		if matchingExpectation == nil {
			msg := fmt.Sprintf("Rendered query did not match any given options.\nActual:\n\t%s\nExpected one of: ", output)

			for _, expectation := range expectations {
				msg += "\n\t" + expectation.Query
			}

			t.Fatal(msg)
		} else if matchingExpectation.Parameters != nil {
			require.Equal(t, matchingExpectation.Parameters, builder.Parameters)
		}
	}
}

func TestQueryBuilder_RenderShortestPaths(t *testing.T) {
	t.Run("Shortest Paths with Unbound Relationship", assertQueryShortestPathResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.StartProperty("objectid"), "12345"),
				query.KindIn(query.Start(), graph.StringKind("A"), graph.StringKind("B")),

				query.Equals(query.EndProperty("objectid"), "56789"),
				query.KindIn(query.End(), graph.StringKind("B")),
			),
		),

		query.Returning(
			query.Path(),
		),
	), "match p = allShortestPaths((s)-[*]->(e)) where s.objectid = $p0 and (s:A or s:B) and e.objectid = $p1 and e:B return p", map[string]any{
		"p0": "12345",
		"p1": "56789",
	}))

	t.Run("Shortest Paths with Bound Relationship", assertQueryShortestPathResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.StartProperty("objectid"), "12345"),
				query.KindIn(query.Start(), graph.StringKind("A"), graph.StringKind("B")),
				query.KindIn(query.Relationship(), graph.StringKind("R1"), graph.StringKind("R2")),
				query.Equals(query.EndProperty("objectid"), "56789"),
				query.KindIn(query.End(), graph.StringKind("B")),
			),
		),

		query.Returning(
			query.Path(),
		),
	), "match p = allShortestPaths((s)-[r:R1|R2*]->(e)) where s.objectid = $p0 and (s:A or s:B) and e.objectid = $p1 and e:B return p", map[string]any{
		"p0": "12345",
		"p1": "56789",
	}))
}

func TestQueryBuilderProjectionModifiersAreOrderIndependent(t *testing.T) {
	testCases := map[string][]graph.Criteria{
		"before return": {
			query.OrderBy(query.NodeID()),
			query.Limit(7),
			query.Offset(3),
			query.Returning(query.Node()),
		},
		"after return": {
			query.Returning(query.Node()),
			query.Offset(3),
			query.Limit(7),
			query.OrderBy(query.NodeID()),
		},
		"criteria slice": {
			[]graph.Criteria{
				query.OrderBy(query.NodeID()),
				query.Limit(7),
				query.Offset(3),
			},
			query.Returning(query.Node()),
		},
		"last modifier and replacement return win": {
			query.Returning(query.NodeID()),
			query.OrderBy(query.NodeProperty("name")),
			query.OrderBy(query.NodeID()),
			query.Limit(2),
			query.Limit(7),
			query.Offset(1),
			query.Offset(3),
			query.Returning(query.Node()),
		},
	}

	for name, criteria := range testCases {
		t.Run(name, func(t *testing.T) {
			builder := neo4j.NewEmptyQueryBuilder()
			for _, criterion := range criteria {
				builder.Apply(criterion)
			}

			if err := builder.Prepare(); err != nil {
				t.Fatalf("prepare query: %v", err)
			}

			rendered, err := builder.Render()
			if err != nil {
				t.Fatalf("render query: %v", err)
			}

			if expected := "match (n) return n order by id(n) asc skip 3 limit 7"; rendered != expected {
				t.Fatalf("rendered query = %q, want %q", rendered, expected)
			}
		})
	}
}

// TestQueryBuilder_LOGIC01PreservesBranchLocalRelationshipKinds verifies disjunctive branches retain their own relationship-kind predicates.
func TestQueryBuilder_LOGIC01PreservesBranchLocalRelationshipKinds(t *testing.T) {
	rawQuery := query.SinglePartQuery(
		query.Where(
			query.Or(
				query.And(
					query.Equals(query.StartID(), graph.ID(101)),
					query.Equals(query.EndID(), graph.ID(202)),
					query.KindIn(query.Relationship(), graph.StringKind("KindA")),
				),
				query.And(
					query.Equals(query.StartID(), graph.ID(202)),
					query.Equals(query.EndID(), graph.ID(101)),
					query.KindIn(query.Relationship(), graph.StringKind("KindB")),
				),
			),
		),
		query.Returning(query.RelationshipID()),
	)

	assertQueryResult(
		rawQuery,
		"match (s)-[r]->(e) where (id(s) = $p0 and id(e) = $p1 and r:KindA or id(s) = $p2 and id(e) = $p3 and r:KindB) return id(r)",
		map[string]any{
			"p0": graph.ID(101),
			"p1": graph.ID(202),
			"p2": graph.ID(202),
			"p3": graph.ID(101),
		},
	)(t)
}

// TestQueryBuilder_LogicalForms verifies Neo4j rendering preserves supported logical expression shapes and precedence.
func TestQueryBuilder_LogicalForms(t *testing.T) {
	temporalThreshold := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

	t.Run("LOGIC-02 cross-binding temporal disjunction", assertQueryResult(
		query.SinglePartQuery(
			query.Where(
				query.Or(
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.StartProperty("lastcollected")),
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.EndProperty("lastcollected")),
				),
			),
			query.Returning(query.RelationshipID()),
		),
		"match (s)-[r]->(e) where (r.lastseen < s.lastcollected or r.lastseen < e.lastcollected) return id(r)",
	))

	t.Run("LOGIC-03 scoped negation and null-aware age predicate", assertQueryResult(
		query.SinglePartQuery(
			query.Where(
				query.And(
					query.Not(query.KindIn(query.Node(), graph.StringKind("Protected"))),
					query.Or(
						query.Not(query.Exists(query.NodeProperty("lastseen"))),
						query.Before(query.NodeProperty("lastseen"), temporalThreshold),
					),
				),
			),
			query.Returning(query.NodeID()),
		),
		"match (n) where not (n:Protected) and (not (n.lastseen is not null) or n.lastseen < $p0) return id(n)",
		map[string]any{"p0": temporalThreshold},
	))
}

// TestQueryBuilder_LOGIC05ProjectionOrder verifies projection ordering remains stable for the LOGIC-05 regression form.
func TestQueryBuilder_LOGIC05ProjectionOrder(t *testing.T) {
	testCases := map[string]struct {
		// projection is the return clause under test.
		projection *cypher.Return

		// expected is the rendered Cypher query.
		expected string
	}{
		"full opposite node plus relationship": {
			projection: query.Returning(query.Relationship(), query.End()),
			expected:   "match ()-[r]->(e) return r, e",
		},
		"opposite ID and kinds plus relationship ID and kind": {
			projection: query.Returning(query.EndID(), query.KindsOf(query.End()), query.RelationshipID(), query.KindsOf(query.Relationship())),
			expected:   "match ()-[r]->(e) return id(e), labels(e), id(r), type(r)",
		},
		"start relationship end triple": {
			projection: query.Returning(query.Start(), query.Relationship(), query.End()),
			expected:   "match (s)-[r]->(e) return s, r, e",
		},
		"relationship ID only": {
			projection: query.Returning(query.RelationshipID()),
			expected:   "match ()-[r]->() return id(r)",
		},
		"full relationship": {
			projection: query.Returning(query.Relationship()),
			expected:   "match ()-[r]->() return r",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, assertQueryResult(
			query.SinglePartQuery(testCase.projection),
			testCase.expected,
		))
	}
}

// TestQueryBuilder_ReconciliationForms verifies reconciliation forms render the expected predicates and projections.
func TestQueryBuilder_ReconciliationForms(t *testing.T) {
	reconciliationKinds := func(count int) graph.Kinds {
		kinds := make(graph.Kinds, count)
		for idx := range count {
			kinds[idx] = graph.StringKind(fmt.Sprintf("ReconcileKind%02d", idx+1))
		}
		return kinds
	}

	for _, count := range []int{1, 2, 9, 30} {
		kinds := reconciliationKinds(count)
		renderedKinds := "ReconcileKind01"
		for idx := 1; idx < count; idx++ {
			renderedKinds += fmt.Sprintf("|ReconcileKind%02d", idx+1)
		}

		t.Run(fmt.Sprintf("REC-01 inbound relationship delete with %d kinds", count), assertQueryResult(
			query.SinglePartQuery(
				query.Where(query.And(
					query.Kind(query.End(), graph.StringKind("ADEntity")),
					query.Equals(query.EndProperty("objectid"), "target-id"),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Delete(query.Relationship()),
			),
			fmt.Sprintf("match ()-[r:%s]->(e) where e:ADEntity and e.objectid = $p0 delete r", renderedKinds),
			map[string]any{"p0": "target-id"},
		))

		t.Run(fmt.Sprintf("REC-02 outbound relationship delete with %d kinds", count), assertQueryResult(
			query.SinglePartQuery(
				query.Where(query.And(
					query.Kind(query.Start(), graph.StringKind("ADEntity")),
					query.Equals(query.StartProperty("objectid"), "target-id"),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Delete(query.Relationship()),
			),
			fmt.Sprintf("match (s)-[r:%s]->() where s:ADEntity and s.objectid = $p0 delete r", renderedKinds),
			map[string]any{"p0": "target-id"},
		))
	}

	t.Run("REC-03 inbound primary-group relationship delete", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.End(), graph.StringKind("Group")),
				query.Equals(query.EndProperty("objectid"), "group-id"),
				query.Kind(query.Relationship(), graph.StringKind("MemberOf")),
				query.Equals(query.RelationshipProperty("isprimarygroup"), false),
			)),
			query.Delete(query.Relationship()),
		),
		"match ()-[r:MemberOf]->(e) where e:Group and e.objectid = $p0 and r.isprimarygroup = $p1 delete r",
		map[string]any{"p0": "group-id", "p1": false},
	))

	t.Run("REC-03 outbound primary-group relationship delete", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Start(), graph.StringKind("Computer")),
				query.Equals(query.StartProperty("objectid"), "computer-id"),
				query.Kind(query.Relationship(), graph.StringKind("MemberOf")),
				query.Equals(query.RelationshipProperty("isprimarygroup"), true),
			)),
			query.Delete(query.Relationship()),
		),
		"match (s)-[r:MemberOf]->() where s:Computer and s.objectid = $p0 and r.isprimarygroup = $p1 delete r",
		map[string]any{"p0": "computer-id", "p1": true},
	))

	t.Run("REC-04 endpoint object ID list relationship delete", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Relationship(), graph.StringKind("ReconcileKind01")),
				query.Kind(query.End(), graph.StringKind("ADEntity")),
				query.In(query.EndProperty("objectid"), []string{"target-1", "target-2"}),
			)),
			query.Delete(query.Relationship()),
		),
		"match ()-[r:ReconcileKind01]->(e) where e:ADEntity and e.objectid in $p0 delete r",
		map[string]any{"p0": []string{"target-1", "target-2"}},
	))

	t.Run("REC-05 delegated enrollment discovery projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.In(query.EndProperty("objectid"), []string{"ca-1", "ca-2"}),
				query.Kind(query.Relationship(), graph.StringKind("PublishedTo")),
				query.Kind(query.Start(), graph.StringKind("CertTemplate")),
			)),
			query.Returning(query.Relationship(), query.Start()),
		),
		"match (s)-[r:PublishedTo]->(e) where e.objectid in $p0 and s:CertTemplate return r, s",
		map[string]any{"p0": []string{"ca-1", "ca-2"}},
	))

	t.Run("REC-06 delegated enrollment relationship delete by end IDs", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.End(), graph.StringKind("CertTemplate")),
				query.InIDs(query.EndID(), graph.ID(101), graph.ID(202)),
				query.KindIn(query.Relationship(), graph.StringKind("DelegatedEnrollmentAgent")),
			)),
			query.Delete(query.Relationship()),
		),
		"match ()-[r:DelegatedEnrollmentAgent]->(e) where e:CertTemplate and id(e) in $p0 delete r",
		map[string]any{"p0": []graph.ID{101, 202}},
	))

	t.Run("REC-07 HostsCAService relationship delete", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.End(), graph.StringKind("EnterpriseCA")),
				query.Equals(query.EndProperty("objectid"), "ca-id"),
				query.KindIn(query.Relationship(), graph.StringKind("HostsCAService")),
			)),
			query.Delete(query.Relationship()),
		),
		"match ()-[r:HostsCAService]->(e) where e:EnterpriseCA and e.objectid = $p0 delete r",
		map[string]any{"p0": "ca-id"},
	))

	t.Run("REC-08 AD entity detach delete", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("ADEntity")),
				query.In(query.NodeProperty("objectid"), []string{"target-1", "target-2"}),
			)),
			query.Delete(query.Node()),
		),
		"match (n) where n:ADEntity and n.objectid in $p0 detach delete n",
		map[string]any{"p0": []string{"target-1", "target-2"}},
	))
}

// TestQueryBuilder_TrustAndPruningForms verifies trust and pruning forms preserve selector and mutation semantics.
func TestQueryBuilder_TrustAndPruningForms(t *testing.T) {
	threshold := time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC)
	domain := graph.StringKind("Domain")

	t.Run("TRUST-01 SameForestTrust ID projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Start(), domain),
				query.Kind(query.End(), domain),
				query.Kind(query.Relationship(), graph.StringKind("SameForestTrust")),
				query.Or(
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.StartProperty("lastcollected")),
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.EndProperty("lastcollected")),
				),
			)),
			query.Returning(query.RelationshipID()),
		),
		"match (s)-[r:SameForestTrust]->(e) where s:Domain and e:Domain and (r.lastseen < s.lastcollected or r.lastseen < e.lastcollected) return id(r)",
	))

	t.Run("TRUST-02 CrossForestTrust full relationship projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Start(), domain),
				query.Kind(query.End(), domain),
				query.KindIn(query.Relationship(), graph.StringKind("CrossForestTrust")),
				query.Or(
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.StartProperty("lastcollected")),
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.EndProperty("lastcollected")),
				),
			)),
			query.Returning(query.Relationship()),
		),
		"match (s)-[r:CrossForestTrust]->(e) where s:Domain and e:Domain and (r.lastseen < s.lastcollected or r.lastseen < e.lastcollected) return r",
	))

	t.Run("TRUST-03 directional derived trust disjunction", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Start(), domain),
				query.Kind(query.End(), domain),
				query.Or(
					query.And(
						query.Equals(query.StartID(), graph.ID(101)),
						query.Equals(query.EndID(), graph.ID(202)),
						query.KindIn(query.Relationship(), graph.StringKind("AbuseTGTDelegation")),
					),
					query.And(
						query.Equals(query.StartID(), graph.ID(202)),
						query.Equals(query.EndID(), graph.ID(101)),
						query.KindIn(query.Relationship(), graph.StringKind("SpoofSIDHistory")),
					),
				),
			)),
			query.Returning(query.RelationshipID()),
		),
		"match (s)-[r]->(e) where s:Domain and e:Domain and (id(s) = $p0 and id(e) = $p1 and r:AbuseTGTDelegation or id(s) = $p2 and id(e) = $p3 and r:SpoofSIDHistory) return id(r)",
		map[string]any{"p0": graph.ID(101), "p1": graph.ID(202), "p2": graph.ID(202), "p3": graph.ID(101)},
	))

	t.Run("PRUNE-01 relationship TTL excludes several kinds", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Not(query.KindIn(query.Relationship(), graph.StringKind("MetaIncludes"), graph.StringKind("HasSession"))),
				query.Before(query.RelationshipProperty("lastseen"), threshold),
			)),
			query.Returning(query.RelationshipID()),
		),
		"match ()-[r]->() where not ((r:MetaIncludes or r:HasSession)) and r.lastseen < $p0 return id(r)",
		map[string]any{"p0": threshold},
	))

	t.Run("PRUNE-02 HasSession missing or stale TTL", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.KindIn(query.Relationship(), graph.StringKind("HasSession")),
				query.Or(
					query.Not(query.Exists(query.RelationshipProperty("lastseen"))),
					query.Before(query.RelationshipProperty("lastseen"), threshold),
				),
			)),
			query.Returning(query.RelationshipID()),
		),
		"match ()-[r:HasSession]->() where (not (r.lastseen is not null) or r.lastseen < $p0) return id(r)",
		map[string]any{"p0": threshold},
	))

	t.Run("PRUNE-03 node TTL excludes several kinds", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Not(query.KindIn(query.Node(), graph.StringKind("Domain"), graph.StringKind("Tenant"), graph.StringKind("Meta"), graph.StringKind("MetaIncludes"), graph.StringKind("MigrationData"))),
				query.Or(
					query.Not(query.Exists(query.NodeProperty("lastseen"))),
					query.Before(query.NodeProperty("lastseen"), threshold),
				),
			)),
			query.Returning(query.NodeID()),
		),
		"match (n) where not ((n:Domain or n:Tenant or n:Meta or n:MetaIncludes or n:MigrationData)) and (not (n.lastseen is not null) or n.lastseen < $p0) return id(n)",
		map[string]any{"p0": threshold},
	))

	t.Run("PRUNE-04 orphan SID prefix", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Not(query.KindIn(query.Node(), graph.StringKind("Domain"), graph.StringKind("Tenant"), graph.StringKind("Meta"), graph.StringKind("MetaIncludes"), graph.StringKind("MigrationData"))),
				query.Not(query.Exists(query.NodeProperty("name"))),
				query.StringStartsWith(query.NodeProperty("objectid"), "S-1-5"),
			)),
			query.Returning(query.NodeID()),
		),
		"match (n) where not ((n:Domain or n:Tenant or n:Meta or n:MetaIncludes or n:MigrationData)) and not (n.name is not null) and n.objectid starts with $p0 return id(n)",
		map[string]any{"p0": "S-1-5"},
	))
}

// TestQueryBuilder_StandaloneHopForms verifies one-hop forms preserve direction, kinds, and endpoint projections.
func TestQueryBuilder_StandaloneHopForms(t *testing.T) {
	hopKinds := func(count int) graph.Kinds {
		kinds := make(graph.Kinds, count)
		for idx := range count {
			kinds[idx] = graph.StringKind(fmt.Sprintf("HopKind%02d", idx+1))
		}
		return kinds
	}

	t.Run("HOP-01 outbound exact start anchor with full directional projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("HopKind01")),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopKind01]->(e) where id(s) = $p0 return r, e",
		map[string]any{"p0": graph.ID(101)},
	))

	t.Run("HOP-01 outbound one-element start IN anchor", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.StartID(), graph.ID(101)),
				query.KindIn(query.Relationship(), graph.StringKind("HopKind01")),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopKind01]->(e) where id(s) in $p0 return r, e",
		map[string]any{"p0": []graph.ID{101}},
	))

	t.Run("HOP-02 inbound exact end anchor with full directional projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.EndID(), graph.ID(202)),
				query.Kind(query.Relationship(), graph.StringKind("HopKind01")),
			)),
			query.Returning(query.Relationship(), query.Start()),
		),
		"match (s)-[r:HopKind01]->(e) where id(e) = $p0 return r, s",
		map[string]any{"p0": graph.ID(202)},
	))

	for _, count := range []int{2, 5, 9, 30} {
		kinds := hopKinds(count)
		renderedKinds := "HopKind01"
		for idx := 1; idx < count; idx++ {
			renderedKinds += fmt.Sprintf("|HopKind%02d", idx+1)
		}

		t.Run(fmt.Sprintf("HOP-03 outbound %d relationship kinds", count), assertQueryResult(
			query.SinglePartQuery(
				query.Where(query.And(
					query.InIDs(query.StartID(), graph.ID(101)),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Returning(query.Relationship(), query.End()),
			),
			fmt.Sprintf("match (s)-[r:%s]->(e) where id(s) in $p0 return r, e", renderedKinds),
			map[string]any{"p0": []graph.ID{101}},
		))

		t.Run(fmt.Sprintf("HOP-03 inbound %d relationship kinds", count), assertQueryResult(
			query.SinglePartQuery(
				query.Where(query.And(
					query.InIDs(query.EndID(), graph.ID(202)),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Returning(query.Relationship(), query.Start()),
			),
			fmt.Sprintf("match (s)-[r:%s]->(e) where id(e) in $p0 return r, s", renderedKinds),
			map[string]any{"p0": []graph.ID{202}},
		))
	}

	t.Run("HOP-04 opposite endpoint kind disjunction", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.StartID(), graph.ID(101)),
				query.KindIn(query.Relationship(), graph.StringKind("HopTypedEdge")),
				query.KindIn(query.End(), graph.StringKind("HopEndA"), graph.StringKind("HopEndB")),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopTypedEdge]->(e) where id(s) in $p0 and (e:HopEndA or e:HopEndB) return r, e",
		map[string]any{"p0": []graph.ID{101}},
	))

	t.Run("HOP-05 endpoint IDs through variable spelling", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("HopIDEdge")),
				query.InIDs(query.End(), graph.ID(202), graph.ID(303)),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopIDEdge]->(e) where id(s) = $p0 and id(e) in $p1 return r, e",
		map[string]any{"p0": graph.ID(101), "p1": []graph.ID{202, 303}},
	))

	t.Run("HOP-05 endpoint IDs through identity-function spelling", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.Start(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("HopIDEdge")),
				query.InIDs(query.EndID(), graph.ID(202), graph.ID(303)),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopIDEdge]->(e) where id(s) in $p0 and id(e) in $p1 return r, e",
		map[string]any{"p0": []graph.ID{101}, "p1": []graph.ID{202, 303}},
	))

	t.Run("HOP-06 opposite endpoint scalar properties", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("HopPropertyEdge")),
				query.Equals(query.EndProperty("enabled"), true),
				query.Equals(query.EndProperty("score"), 7),
				query.Equals(query.EndProperty("name"), "target"),
				query.Equals(query.EndProperty("isassignabletorole"), "true"),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopPropertyEdge]->(e) where id(s) = $p0 and e.enabled = $p1 and e.score = $p2 and e.name = $p3 and e.isassignabletorole = $p4 return r, e",
		map[string]any{"p0": graph.ID(101), "p1": true, "p2": 7, "p3": "target", "p4": "true"},
	))

	t.Run("HOP-07 nested production-style endpoint predicate", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.KindIn(query.Relationship(), graph.StringKind("HopNestedEdge")),
				query.Kind(query.End(), graph.StringKind("HopTemplate")),
				query.Or(
					query.And(
						query.Equals(query.EndProperty("requiresmanagerapproval"), false),
						query.GreaterThan(query.EndProperty("schemaversion"), 1),
						query.Equals(query.EndProperty("authorizedsignatures"), 0),
						query.Equals(query.EndProperty("authenticationenabled"), true),
					),
					query.And(
						query.Equals(query.EndProperty("requiresmanagerapproval"), false),
						query.Equals(query.EndProperty("schemaversion"), 1),
						query.Equals(query.EndProperty("authenticationenabled"), true),
					),
				),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopNestedEdge]->(e) where id(s) = $p0 and e:HopTemplate and (e.requiresmanagerapproval = $p1 and e.schemaversion > $p2 and e.authorizedsignatures = $p3 and e.authenticationenabled = $p4 or e.requiresmanagerapproval = $p5 and e.schemaversion = $p6 and e.authenticationenabled = $p7) return r, e",
		map[string]any{"p0": graph.ID(101), "p1": false, "p2": 1, "p3": 0, "p4": true, "p5": false, "p6": 1, "p7": true},
	))

	t.Run("HOP-08 collection predicates nested with scalar fallback", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("HopCollectionEdge")),
				query.Or(
					query.Equals(query.EndProperty("schannelauthenticationenabled"), true),
					query.Equals(query.Size(query.EndProperty("effectiveekus")), 0),
					query.InInverted(query.EndProperty("effectiveekus"), "1.3.6.1.5.5.7.3.2"),
				),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopCollectionEdge]->(e) where id(s) = $p0 and (e.schannelauthenticationenabled = $p1 or size(e.effectiveekus) = $p2 or $p3 in e.effectiveekus) return r, e",
		map[string]any{"p0": graph.ID(101), "p1": true, "p2": 0, "p3": "1.3.6.1.5.5.7.3.2"},
	))

	t.Run("HOP-09 two-sided endpoint ID lists", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.StartID(), graph.ID(101), graph.ID(202)),
				query.InIDs(query.EndID(), graph.ID(303), graph.ID(404)),
				query.Kind(query.Relationship(), graph.StringKind("HopSetEdge")),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopSetEdge]->(e) where id(s) in $p0 and id(e) in $p1 return r, e",
		map[string]any{"p0": []graph.ID{101, 202}, "p1": []graph.ID{303, 404}},
	))

	t.Run("HOP-10 outbound endpoint kind property and start anchor", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("HopProjectionEdge")),
				query.Kind(query.End(), graph.StringKind("HopProjectionEnd")),
				query.Equals(query.EndProperty("active"), true),
			)),
			query.Returning(query.Relationship(), query.End()),
		),
		"match (s)-[r:HopProjectionEdge]->(e) where id(s) in $p0 and e:HopProjectionEnd and e.active = $p1 return r, e",
		map[string]any{"p0": []graph.ID{101}, "p1": true},
	))

	t.Run("HOP-10 inbound endpoint kind property and end anchor", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.EndID(), graph.ID(202)),
				query.Kind(query.Relationship(), graph.StringKind("HopProjectionEdge")),
				query.Kind(query.Start(), graph.StringKind("HopProjectionStart")),
				query.Equals(query.StartProperty("active"), true),
			)),
			query.Returning(query.Relationship(), query.Start()),
		),
		"match (s)-[r:HopProjectionEdge]->(e) where id(e) in $p0 and s:HopProjectionStart and s.active = $p1 return r, s",
		map[string]any{"p0": []graph.ID{202}, "p1": true},
	))

	t.Run("HOP-10 explicit start-node projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.EndID(), graph.ID(202)),
				query.Kind(query.Relationship(), graph.StringKind("HopProjectionEdge")),
			)),
			query.Returning(query.Start()),
		),
		"match (s)-[r:HopProjectionEdge]->(e) where id(e) in $p0 return s",
		map[string]any{"p0": []graph.ID{202}},
	))

	t.Run("HOP-10 explicit end-ID and relationship projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.InIDs(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("HopProjectionEdge")),
			)),
			query.Returning(query.EndID(), query.Relationship()),
		),
		"match (s)-[r:HopProjectionEdge]->(e) where id(s) in $p0 return id(e), r",
		map[string]any{"p0": []graph.ID{101}},
	))
}

// TestQueryBuilder_Render verifies legacy query criteria render the expected Neo4j Cypher and parameters.
func TestQueryBuilder_Render(t *testing.T) {
	temporalThreshold := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

	// Node Queries
	t.Run("Node Count", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.In(query.NodeID(), []graph.ID{1, 2, 3, 4}),
		),

		query.Returning(
			query.Count(query.Node()),
		),

		query.Limit(10),
		query.Offset(20),
	), "match (n) where id(n) in $p0 return count(n) skip 20 limit 10", map[string]any{
		"p0": []graph.ID{1, 2, 3, 4},
	}))

	t.Run("Node Item", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.In(query.NodeProperty("prop"), []int{1, 2, 3, 4}),
		),

		query.Returning(
			query.Count(query.Node()),
		),
	), "match (n) where n.prop in $p0 return count(n)"))

	// TODO: Revisit parameter reuse
	//
	//reusedLiteral := query3.Literal([]int{1, 2, 3, 4})
	//
	//t.Run("Node Item with Reused Literal", assertQueryResult(query3.Query(
	//	query3.Where(
	//		query3.And(
	//			query3.In(query3.NodeProperty("prop"), reusedLiteral),
	//			query3.In(query3.NodeProperty("other_prop"), reusedLiteral),
	//		),
	//	),
	//
	//	query3.Returning(
	//		query3.Count(query3.Node()),
	//	),
	//), "match (n) where n.prop in $p0 and n.other_prop in $p0 return count(n)"))

	t.Run("Distinct Item", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.In(query.NodeProperty("prop"), []int{1, 2, 3, 4}),
		),

		query.ReturningDistinct(
			query.NodeProperty("prop"),
		),
	), "match (n) where n.prop in $p0 return distinct n.prop"))

	t.Run("Count Distinct Item", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.In(query.NodeProperty("prop"), []int{1, 2, 3, 4}),
		),

		query.Returning(
			query.CountDistinct(query.NodeProperty("prop")),
		),
	), "match (n) where n.prop in $p0 return count(distinct n.prop)"))

	t.Run("Set Node Labels", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.In(query.NodeProperty("prop"), []int{1, 2, 3, 4}),
		),

		query.Update(
			query.AddKind(query.Node(), Domain),
			query.AddKind(query.Node(), User),
		),

		query.Returning(
			query.Count(query.Node()),
		),
	), "match (n) where n.prop in $p0 set n:Domain set n:User return count(n)"))

	t.Run("Remove Node Labels", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.In(query.NodeProperty("prop"), []int{1, 2, 3, 4}),
		),

		query.Update(
			query.DeleteKind(query.Node(), Domain),
			query.DeleteKind(query.Node(), User),
		),

		query.Returning(
			query.Count(query.Node()),
		),
	), "match (n) where n.prop in $p0 remove n:Domain remove n:User return count(n)"))

	t.Run("Multiple Node ID References", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.NodeProperty("name"), "name"),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.Identity(query.Node()),
			query.Property(query.Node(), "value"),
		),

		query.Limit(10),
		query.Offset(20),
	), "match (n) where n.name = $p0 and id(n) in $p1 return id(n), n.value skip 20 limit 10"))

	// Create node
	t.Run("Create Node", assertQueryResult(query.SinglePartQuery(
		query.Create(
			query.NodePattern(
				graph.Kinds{Domain, Computer},
				query.Parameter(map[string]any{
					"prop1": 1234,
				}),
			),
		),

		query.Returning(
			query.Identity(query.Node()),
		),
	),
		"create (n:Domain:Computer $p0) return id(n)",
		map[string]any{
			"p0": map[string]any{
				"prop1": 1234,
			},
		},
	))

	// Set with node

	t.Run("DeleteProperty with Multiple Node ID References", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.NodeProperty("name"), "name"),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Update(
			query.DeleteProperty(query.NodeProperty("other")),
			query.DeleteProperty(query.NodeProperty("other2")),
		),

		query.Returning(
			query.Identity(query.Node()),
			query.Property(query.Node(), "value"),
		),

		query.Limit(10),
		query.Offset(20),
	), "match (n) where n.name = $p0 and id(n) in $p1 remove n.other remove n.other2 return id(n), n.value skip 20 limit 10"))

	properties := graph.NewProperties()
	properties.Set("test_1", "value_1")
	properties.Set("test_2", "value_2")

	t.Run("Set from Map", assertOneOfQueryResult(query.SinglePartQuery(
		query.Where(
			query.Equals(query.NodeProperty("objectid"), "12345"),
		),

		query.Update(
			query.SetProperties(query.Node(), properties.ModifiedProperties()),
		),
	), []QueryOutputAssertion{
		{
			Query: "match (n) where n.objectid = $p0 set n.test_1 = $p1, n.test_2 = $p2",
			Parameters: map[string]any{
				"p0": "12345",
				"p1": "value_1",
				"p2": "value_2",
			},
		},
		{
			Query: "match (n) where n.objectid = $p0 set n.test_2 = $p1, n.test_1 = $p2",
			Parameters: map[string]any{
				"p0": "12345",
				"p1": "value_2",
				"p2": "value_1",
			},
		},
	}))

	properties.Delete("test_1")
	properties.Delete("test_2")

	t.Run("DeleteProperty from Map", assertOneOfQueryResult(query.SinglePartQuery(
		query.Where(
			query.Equals(query.NodeProperty("objectid"), "12345"),
		),

		query.Update(
			query.DeleteProperties(query.Node(), properties.DeletedProperties()...),
		),
	), []QueryOutputAssertion{
		{
			Query: "match (n) where n.objectid = $p0 remove n.test_2, n.test_1",
			Parameters: map[string]any{
				"p0": "12345",
			},
		},
		{
			Query: "match (n) where n.objectid = $p0 remove n.test_1, n.test_2",
			Parameters: map[string]any{
				"p0": "12345",
			},
		},
	}))

	t.Run("Set with Multiple Node ID References", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.NodeProperty("name"), "name"),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Update(
			query.SetProperty(query.NodeProperty("other"), "value"),
		),

		query.Returning(
			query.Identity(query.Node()),
			query.Property(query.Node(), "value"),
		),

		query.Limit(10),
		query.Offset(20),
	), "match (n) where n.name = $p0 and id(n) in $p1 set n.other = $p2 return id(n), n.value skip 20 limit 10"))

	updatedNode := graph.NewNode(graph.ID(1), graph.NewProperties(), User, Domain, Computer)
	updatedNode.Properties.Set("test_1", "value_1")
	updatedNode.Properties.Delete("test_2")

	t.Run("Node Set and Remove Multiple Kinds and Properties", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.Equals(query.NodeID(), updatedNode.ID),
		),

		query.Updatef(func() graph.Criteria {
			var (
				properties       = updatedNode.Properties
				updateStatements = []graph.Criteria{
					query.AddKinds(query.Node(), updatedNode.Kinds),
				}
			)

			if modifiedProperties := properties.ModifiedProperties(); len(modifiedProperties) > 0 {
				updateStatements = append(updateStatements, query.SetProperties(query.Node(), modifiedProperties))
			}

			if deletedProperties := properties.DeletedProperties(); len(deletedProperties) > 0 {
				updateStatements = append(updateStatements, query.DeleteProperties(query.Node(), deletedProperties...))
			}

			return updateStatements
		}),
	), "match (n) where id(n) = $p0 set n:User:Domain:Computer set n.test_1 = $p1 remove n.test_2"))

	t.Run("Node has Relationships", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.HasRelationships(query.Node()),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where (n)-[]-() return n"))

	t.Run("Node has Relationships Order by Node Item", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.HasRelationships(query.Node()),
		),

		query.Returning(
			query.Node(),
		),

		query.OrderBy(
			query.Order(query.NodeProperty("value"), query.Ascending()),
		),
	), "match (n) where (n)-[]-() return n order by n.value asc"))

	t.Run("Node has Relationships Order by Direct Node ID", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.HasRelationships(query.Node()),
		),

		query.Returning(
			query.Node(),
		),

		query.OrderBy(
			query.NodeID(),
		),
	), "match (n) where (n)-[]-() return n order by id(n) asc"))

	t.Run("Node has Relationships Order by Node Item", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.HasRelationships(query.Node()),
		),

		query.Returning(
			query.Node(),
		),

		query.OrderBy(
			query.Order(query.NodeProperty("value_1"), query.Ascending()),
			query.Order(query.NodeProperty("value_2"), query.Descending()),
		),
	), "match (n) where (n)-[]-() return n order by n.value_1 asc, n.value_2 desc"))

	t.Run("Node has Relationships Order by Node Item with Limit and Offset", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.HasRelationships(query.Node()),
		),

		query.Returning(
			query.Node(),
		),

		query.OrderBy(
			query.Order(query.NodeProperty("value_1"), query.Ascending()),
			query.Order(query.NodeProperty("value_2"), query.Descending()),
		),

		query.Limit(10),
		query.Offset(20),
	), "match (n) where (n)-[]-() return n order by n.value_1 asc, n.value_2 desc skip 20 limit 10"))

	t.Run("Node has no Relationships", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.Not(query.HasRelationships(query.Node())),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where not ((n)-[]-()) return n"))

	t.Run("Node Datetime Before", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Before(query.NodeProperty("lastseen"), temporalThreshold),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.lastseen < $p0 and id(n) in $p1 return n", map[string]any{
		"p0": temporalThreshold,
		"p1": []int{1, 2, 3, 4},
	}))

	t.Run("Node Datetime Before or Equal to", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.LessThanOrEquals(query.NodeProperty("lastseen"), time.Now().UTC()),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.lastseen <= $p0 and id(n) in $p1 return n"))

	t.Run("Node Datetime After", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.After(query.NodeProperty("lastseen"), time.Now().UTC()),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.lastseen > $p0 and id(n) in $p1 return n"))

	t.Run("Node Datetime After or Equal to", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.GreaterThanOrEquals(query.NodeProperty("lastseen"), time.Now().UTC()),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.lastseen >= $p0 and id(n) in $p1 return n"))

	t.Run("Node PropertyExists", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Exists(query.NodeProperty("lastseen")),
				query.In(query.NodeID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.lastseen is not null and id(n) in $p0 return n"))

	t.Run("Select Node Kinds", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Kind(query.Node(), Domain),
			),
		),

		query.Returning(
			query.KindsOf(query.Node()),
		),
	), "match (n) where n:Domain return labels(n)"))

	t.Run("Select Node ID and Kinds", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Kind(query.Node(), Domain),
			),
		),

		query.Returning(
			query.NodeID(),
			query.KindsOf(query.Node()),
		),
	), "match (n) where n:Domain return id(n), labels(n)"))

	t.Run("Node Kind Match", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Kind(query.Node(), Domain),
			),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n:Domain return n"))

	t.Run("Node Kind In", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.KindIn(query.Node(), Domain, User, Group),
			),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where (n:Domain or n:User or n:Group) return n"))

	t.Run("Node String Item Contains", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.StringContains(query.NodeProperty("tags"), "tag_1"),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.tags contains $p0 return n"))

	t.Run("Node String Item Starts With", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.StringStartsWith(query.NodeProperty("tags"), "tag_1"),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.tags starts with $p0 return n"))

	t.Run("Node String Item Ends With", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.StringEndsWith(query.NodeProperty("tags"), "tag_1"),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where n.tags ends with $p0 return n"))

	t.Run("Node String Item Case Insensitive Contains", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.CaseInsensitiveStringContains(query.NodeProperty("tags"), "tag_1"),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where toLower(n.tags) contains $p0 return n"))

	t.Run("Node String Item Case Insensitive Starts With", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.CaseInsensitiveStringStartsWith(query.NodeProperty("tags"), "tag_1"),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where toLower(n.tags) starts with $p0 return n"))

	t.Run("Node String Item Case Insensitive Ends With", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.CaseInsensitiveStringEndsWith(query.NodeProperty("tags"), "tag_1"),
		),

		query.Returning(
			query.Node(),
		),
	), "match (n) where toLower(n.tags) ends with $p0 return n"))

	t.Run("Node Delete", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.In(query.Node(), []graph.ID{1, 2, 3}),
		),

		query.Delete(
			query.Node(),
		),
	), "match (n) where n in $p0 detach delete n"))

	// Relationship Queries
	t.Run("Empty Relationship Query", assertQueryResult(query.SinglePartQuery(
		query.Returning(
			query.RelationshipID(),
		),
	), "match ()-[r]->() return id(r)"))

	t.Run("Empty Start Node Query", assertQueryResult(query.SinglePartQuery(
		query.Returning(
			query.StartID(),
		),
	), "match (s)-[]->() return id(s)"))

	t.Run("Empty End Node Query", assertQueryResult(query.SinglePartQuery(
		query.Returning(
			query.EndID(),
		),
	), "match ()-[]->(e) return id(e)"))

	t.Run("Returning Relationship Kind Query", assertQueryResult(query.SinglePartQuery(
		query.Returning(
			query.RelationshipID(),
			query.KindsOf(query.Relationship()),
		),
	), "match ()-[r]->() return id(r), type(r)"))

	t.Run("Returning Start and Relationship Kind Query", assertQueryResult(query.SinglePartQuery(
		query.Returning(
			query.RelationshipID(),
			query.KindsOf(query.Relationship()),
			query.KindsOf(query.Start()),
		),
	), "match (s)-[r]->() return id(r), type(r), labels(s)"))

	t.Run("Returning End and Relationship Kind Query", assertQueryResult(query.SinglePartQuery(
		query.Returning(
			query.RelationshipID(),
			query.KindsOf(query.Relationship()),
			query.KindsOf(query.End()),
		),
	), "match ()-[r]->(e) return id(r), type(r), labels(e)"))

	t.Run("Returning Start, End and Relationship Kind Query", assertQueryResult(query.SinglePartQuery(
		query.Returning(
			query.RelationshipID(),
			query.KindsOf(query.Relationship()),
			query.KindsOf(query.Start()),
			query.KindsOf(query.End()),
		),
	), "match (s)-[r]->(e) return id(r), type(r), labels(s), labels(e)"))

	t.Run("Relationship Item and ID References", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),

		query.Offset(20),
	), "match ()-[r]->() where r.name = $p0 and id(r) in $p1 return id(r), r.value skip 20"))

	t.Run("Relationship Select Start References", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.StartID(),
			query.Property(query.Relationship(), "value"),
		),

		query.Offset(20),
	), "match (s)-[r]->() where r.name = $p0 and id(r) in $p1 return id(s), r.value skip 20"))

	t.Run("Relationship Start Node ID Reference", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.StartID(), 1),
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),

		query.Offset(20),
	), "match (s)-[r]->() where id(s) = $p0 and r.name = $p1 and id(r) in $p2 return id(r), r.value skip 20"))

	t.Run("Relationship End Node ID Reference", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.EndID(), 1),
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),

		query.Offset(20),
	), "match ()-[r]->(e) where id(e) = $p0 and r.name = $p1 and id(r) in $p2 return id(r), r.value skip 20"))

	t.Run("Relationship Start and End Node ID References", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.StartID(), 1),
				query.Equals(query.EndID(), 1),
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),
	), "match (s)-[r]->(e) where id(s) = $p0 and id(e) = $p1 and r.name = $p2 and id(r) in $p3 return id(r), r.value"))

	t.Run("Relationship Kind Match without Joining Expression", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.KindIn(query.Relationship(), Domain, User, GenericWrite, HasSession),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),
	), "match ()-[r:Domain|User|GenericWrite|HasSession]->() return id(r), r.value"))

	t.Run("Relationship Kind Match", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.StartID(), 1),
				query.KindIn(query.Relationship(), HasSession),
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),
	), "match (s)-[r:HasSession]->() where id(s) = $p0 and r.name = $p1 and id(r) in $p2 return id(r), r.value"))

	updatedRelationship := graph.NewRelationship(graph.ID(1), graph.ID(2), graph.ID(3), graph.NewProperties(), HasSession)
	updatedRelationship.Properties.Set("test_1", "value_1")
	updatedRelationship.Properties.Delete("test_2")

	t.Run("Relationship Set and Remove Multiple Kinds and Properties", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.Equals(query.RelationshipID(), updatedRelationship.ID),
		),

		query.Updatef(func() graph.Criteria {
			var (
				properties       = updatedRelationship.Properties
				updateStatements []graph.Criteria
			)

			if modifiedProperties := properties.ModifiedProperties(); len(modifiedProperties) > 0 {
				updateStatements = append(updateStatements, query.SetProperties(query.Relationship(), modifiedProperties))
			}

			if deletedProperties := properties.DeletedProperties(); len(deletedProperties) > 0 {
				updateStatements = append(updateStatements, query.DeleteProperties(query.Relationship(), deletedProperties...))
			}

			return updateStatements
		}),
	), "match ()-[r]->() where id(r) = $p0 set r.test_1 = $p1 remove r.test_2"))

	t.Run("Relationship Kind Match in", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.StartID(), 1),
				query.KindIn(query.Relationship(), HasSession, GenericWrite),
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),
	), "match (s)-[r:HasSession|GenericWrite]->() where id(s) = $p0 and r.name = $p1 and id(r) in $p2 return id(r), r.value"))

	t.Run("Relationship Kind Match in and Start Node Kind Match in", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.KindIn(query.Start(), User, Computer),
				query.KindIn(query.Relationship(), HasSession, GenericWrite),
				query.Equals(query.RelationshipProperty("name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),
	), "match (s)-[r:HasSession|GenericWrite]->() where (s:User or s:Computer) and r.name = $p0 and id(r) in $p1 return id(r), r.value"))

	t.Run("Relationship Kind Match in and Delete Start Node and Relationship", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.KindIn(query.Relationship(), HasSession, GenericWrite),
			),
		),

		query.Delete(
			query.Start(),
			query.Relationship(),
		),
	), "match (s)-[r:HasSession|GenericWrite]->() delete s, r"))

	t.Run("Relationship Kind Match in and Delete Start Node and Relationship Returning Count Relationships Deleted", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.KindIn(query.Relationship(), HasSession, GenericWrite),
			),
		),

		query.Delete(
			query.Start(),
			query.Relationship(),
		),

		query.Returning(
			query.Count(query.Relationship()),
		),
	), "match (s)-[r:HasSession|GenericWrite]->() delete s, r return count(r)"))

	t.Run("Create Relationship", assertQueryResult(query.SinglePartQuery(
		query.Create(
			query.StartNodePattern(
				graph.Kinds{Computer},
				query.Parameter(map[string]any{
					"prop1": 1234,
				}),
			),
			query.RelationshipPattern(
				HasSession,
				query.Parameter(map[string]any{
					"prop1": 1234,
				}),
				graph.DirectionOutbound,
			),
			query.EndNodePattern(
				graph.Kinds{User},
				query.Parameter(map[string]any{
					"prop1": 1234,
				}),
			),
		),

		query.Returning(
			query.Identity(query.Relationship()),
		),
	),
		"create (s:Computer $p0)-[r:HasSession $p1]->(e:User $p2) return id(r)",
		map[string]any{
			"p0": map[string]any{
				"prop1": 1234,
			},
			"p1": map[string]any{
				"prop1": 1234,
			},
			"p2": map[string]any{
				"prop1": 1234,
			},
		},
	))

	t.Run("Create Relationship with Match", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.StartID(), 1),
				query.Equals(query.EndID(), 2),
			),
		),

		query.Create(
			query.Start(),
			query.RelationshipPattern(
				HasSession,
				query.Parameter(map[string]any{
					"prop1": 1234,
				}),
				graph.DirectionOutbound,
			),
			query.End(),
		),

		query.Returning(
			query.Identity(query.Relationship()),
		),
	),
		"match (s), (e) where id(s) = $p0 and id(e) = $p1 create (s)-[r:HasSession $p2]->(e) return id(r)",
		map[string]any{
			"p0": 1,
			"p1": 2,
			"p2": map[string]any{
				"prop1": 1234,
			},
		},
	))

	t.Run("Not String Contains Operator Rewrite", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.Not(
				query.StringContains(query.Property(query.Node(), SystemTags), "admin_tier_0"),
			),
		),

		query.Returning(
			query.Count(query.Node()),
		),
	), "match (n) where (not (n.system_tags contains $p0) or n.system_tags is null) return count(n)"))

	t.Run("Is Not Null", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.IsNotNull(
				query.Property(query.Node(), SystemTags),
			),
		),
		query.Returning(
			query.Count(query.Node()),
		)),
		"match (n) where n.system_tags is not null return count(n)"))

	t.Run("Is Null", assertQueryResult(query.SinglePartQuery(
		query.Where(
			query.IsNull(
				query.Property(query.Node(), SystemTags),
			),
		),
		query.Returning(
			query.Count(query.Node()),
		)),
		"match (n) where n.system_tags is null return count(n)"))
}

func TestQueryBuilder_Analyze(t *testing.T) {
	// Don't allow node query references to intermingle with relationship query references
	t.Run("Should Reject Mixing Query Type References", expectAnalysisError(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.NodeID(), 1),
				query.Equals(query.Property(query.Relationship(), "name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),

		query.Offset(20),
	)))

	t.Run("Should Reject Mixing Query Type References", expectAnalysisError(query.SinglePartQuery(
		query.Where(
			query.And(
				query.Equals(query.NodeID(), 1),
				query.Equals(query.Property(query.Relationship(), "name"), "name"),
				query.In(query.RelationshipID(), []int{1, 2, 3, 4}),
			),
		),

		query.Returning(
			query.RelationshipID(),
			query.Property(query.Relationship(), "value"),
		),

		query.Offset(20),
	)))

	t.Run("Should fail on bad query criteria", expectAnalysisError(query.SinglePartQuery(
		query.Node(),
	)))

	t.Run("Should fail on bad create criteria", expectAnalysisError(query.SinglePartQuery(
		query.Create(
			query.Where(
				query.And(),
			),
		),
	)))

	t.Run("Should fail on bad variable reference types for KindOf", expectAnalysisError(query.SinglePartQuery(
		query.Where(
			query.KindsOf(
				query.Create(),
			),
		),
	)))
}

func Test_FormatCypherOrder(t *testing.T) {
	var (
		sortItems = query.SortItems{
			{SortCriteria: query.NodeID(), Direction: query.SortDirectionAscending},
			{SortCriteria: query.Node(), Direction: query.SortDirectionDescending},
			{SortCriteria: query.Relationship(), Direction: query.SortDirectionAscending},
		}
	)

	require.Equal(t, true, sortItems.FormatCypherOrder().Items[0].Ascending)
	require.Equal(t, false, sortItems.FormatCypherOrder().Items[1].Ascending)
	require.Equal(t, true, sortItems.FormatCypherOrder().Items[2].Ascending)

	require.Equal(t, query.NodeID(), sortItems.FormatCypherOrder().Items[0].Expression)
	require.Equal(t, query.Node(), sortItems.FormatCypherOrder().Items[1].Expression)
	require.Equal(t, query.Relationship(), sortItems.FormatCypherOrder().Items[2].Expression)
}
