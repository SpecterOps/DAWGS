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

func firstCreateClause(t *testing.T, preparedQuery *v2.PreparedQuery) *cypher.Create {
	t.Helper()

	updatingClauses := preparedQuery.Query.SingleQuery.SinglePartQuery.UpdatingClauses
	require.NotEmpty(t, updatingClauses)

	updatingClause, typeOK := updatingClauses[0].(*cypher.UpdatingClause)
	require.True(t, typeOK)

	createClause, typeOK := updatingClause.Clause.(*cypher.Create)
	require.True(t, typeOK)

	return createClause
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

	require.Equal(t, "match (s)-[r]->(e) where not r:test and not (r:A or r:B) and r.rel_prop <= $p0 and r.other_prop = $p1 and s:test set s.this_prop = $p2 remove e:A:B detach delete s return r, s.node_prop skip 10 limit 10", cypherQueryStr)
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

func TestCreateRelationshipWithExplicitEndpoints(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.Start().ID().Equals(1),
		v2.End().ID().Equals(2),
	).Create(
		v2.Start(),
		v2.RelationshipPattern(graph.StringKind("A"), v2.NamedParameter("props", map[string]any{"name": "rel"}), graph.DirectionOutbound),
		v2.End(),
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

func TestCreateSplitsDisjointNodePatterns(t *testing.T) {
	preparedQuery, err := v2.New().Create(
		v2.NodePattern(graph.Kinds{graph.StringKind("A")}, nil),
		v2.NodePattern(graph.Kinds{graph.StringKind("B")}, nil),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "create (n:A), (n:B)", renderPrepared(t, preparedQuery))
	require.Len(t, firstCreateClause(t, preparedQuery).Pattern, 2)
}

func TestCreateSplitsBackToBackRelationshipPatterns(t *testing.T) {
	preparedQuery, err := v2.New().Create(
		v2.RelationshipPattern(graph.StringKind("A"), nil, graph.DirectionOutbound),
		v2.RelationshipPattern(graph.StringKind("B"), nil, graph.DirectionOutbound),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "create (s)-[r:A]->(e), (s)-[r:B]->(e)", renderPrepared(t, preparedQuery))
	require.Len(t, firstCreateClause(t, preparedQuery).Pattern, 2)
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

func TestLogicalHelpersPreservePrecedence(t *testing.T) {
	a := v2.Node().Property("a").Equals("a")
	b := v2.Node().Property("b").Equals("b")
	c := v2.Node().Property("c").Equals("c")

	testCases := []struct {
		name     string
		builder  v2.QueryBuilder
		expected string
	}{
		{
			name: "or is parenthesized in isolation",
			builder: v2.New().Where(
				v2.Or(a, b),
			).Return(v2.Node()),
			expected: "match (n) where (n.a = $p0 or n.b = $p1) return n",
		},
		{
			name: "or is parenthesized when where and-chains constraints",
			builder: v2.New().Where(
				v2.Or(a, b),
				c,
			).Return(v2.Node()),
			expected: "match (n) where (n.a = $p0 or n.b = $p1) and n.c = $p2 return n",
		},
		{
			name: "nested or is parenthesized inside and",
			builder: v2.New().Where(
				v2.And(a, v2.Or(b, c)),
			).Return(v2.Node()),
			expected: "match (n) where n.a = $p0 and (n.b = $p1 or n.c = $p2) return n",
		},
		{
			name: "nested and is parenthesized inside or",
			builder: v2.New().Where(
				v2.Or(v2.And(a, b), c),
			).Return(v2.Node()),
			expected: "match (n) where ((n.a = $p0 and n.b = $p1) or n.c = $p2) return n",
		},
		{
			name: "not wraps or",
			builder: v2.New().Where(
				v2.Not(v2.Or(a, b)),
			).Return(v2.Node()),
			expected: "match (n) where not (n.a = $p0 or n.b = $p1) return n",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)
			require.Equal(t, testCase.expected, renderPrepared(t, preparedQuery))
		})
	}
}

func TestInvalidCreateQualifiedExpressionReturnsError(t *testing.T) {
	_, err := v2.New().Create(v2.Node().Property("name")).Build()
	require.ErrorContains(t, err, "invalid qualified expression for create: *cypher.PropertyLookup")
}

func TestUpdatingClausesPreserveFluentOrder(t *testing.T) {
	preparedQuery, err := v2.New().Create(
		v2.NodePattern(graph.Kinds{graph.StringKind("User")}, nil),
	).Update(
		v2.SetProperty(v2.Node().Property("name"), "created"),
	).Return(
		v2.Node().Property("name"),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "create (n:User) set n.name = $p0 return n.name", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": "created",
	}, preparedQuery.Parameters)

	preparedQuery, err = v2.New().Where(
		v2.Node().ID().Equals(1),
	).Update(
		v2.DeleteProperties(v2.Node(), "old"),
	).Update(
		v2.SetProperty(v2.Node().Property("new"), "value"),
	).Return(
		v2.Node(),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) where id(n) = $p0 remove n.old set n.new = $p1 return n", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": 1,
		"p1": "value",
	}, preparedQuery.Parameters)
}

func TestScopedRelationshipPatternControls(t *testing.T) {
	scope := v2.NewScope("path", "person", "source", "edge", "target")

	preparedQuery, err := scope.New().WithRelationshipDirection(graph.DirectionInbound).Where(
		scope.Relationship().Kind().Is(graph.StringKind("MemberOf")),
		scope.Start().ID().Equals(1),
	).Return(
		scope.Relationship().Kind(),
		scope.End().Kinds(),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (source)<-[edge:MemberOf]-(target) where id(source) = $p0 return type(edge), labels(target)", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": 1,
	}, preparedQuery.Parameters)
}

func TestScopedKindsOfCompatibilityHelper(t *testing.T) {
	scope := v2.NewScope("path", "person", "source", "edge", "target")

	preparedQuery, err := scope.New().Return(
		v2.KindsOf(scope.Relationship()),
		v2.KindsOf(scope.End()),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match ()-[edge]->(target) return type(edge), labels(target)", renderPrepared(t, preparedQuery))
}

func TestInvalidScopeAliasesReturnBuildErrors(t *testing.T) {
	emptyAliasScope := v2.NewScope("", "node", "start", "relationship", "end")
	_, err := emptyAliasScope.New().Return(emptyAliasScope.Node()).Build()
	require.ErrorContains(t, err, "scope alias path is empty")

	duplicateAliasScope := v2.NewScope("path", "node", "node", "relationship", "end")
	_, err = duplicateAliasScope.New().Return(duplicateAliasScope.Start()).Build()
	require.ErrorContains(t, err, `scope aliases node and start both use "node"`)

	invalidAliasScope := v2.NewScope("path", "bad name", "start", "relationship", "end")
	_, err = invalidAliasScope.New().Return(invalidAliasScope.Node()).Build()
	require.ErrorContains(t, err, `scope alias node has invalid symbol "bad name"`)
}

func TestUnicodeCypherSymbols(t *testing.T) {
	scope := v2.NewScope("路径", "节点", "起点", "关系", "终点")

	preparedQuery, err := scope.New().Where(
		scope.Node().Property("name").Equals(v2.NamedParameter("名字", "alice")),
	).Return(
		v2.As(scope.Node().ID(), "标识"),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (节点) where 节点.name = $名字 return id(节点) as 标识", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"名字": "alice",
	}, preparedQuery.Parameters)
}

func TestInvalidRelationshipDirectionReturnsError(t *testing.T) {
	_, err := v2.New().WithRelationshipDirection(graph.Direction(99)).Return(v2.Relationship()).Build()
	require.ErrorContains(t, err, "unsupported relationship direction: invalid")
}

func TestRelationshipDirectionBoth(t *testing.T) {
	preparedQuery, err := v2.New().WithRelationshipDirection(graph.DirectionBoth).Return(v2.Relationship()).Build()
	require.NoError(t, err)

	require.Equal(t, "match ()-[r]-() return r", renderPrepared(t, preparedQuery))
}

func TestShortestPathControls(t *testing.T) {
	preparedQuery, err := v2.New().WithShortestPaths().Where(
		v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
		v2.Start().ID().Equals(1),
		v2.End().ID().Equals(2),
	).Return(
		v2.Path(),
	).Build()
	require.NoError(t, err)
	require.Equal(t, "match p = shortestPath((s)-[r:MemberOf*]->(e)) where id(s) = $p0 and id(e) = $p1 return p", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": 1,
		"p1": 2,
	}, preparedQuery.Parameters)

	preparedQuery, err = v2.New().WithAllShortestPaths().Where(
		v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
		v2.Start().ID().Equals(1),
		v2.End().ID().Equals(2),
	).Return(
		v2.Path(),
	).Build()
	require.NoError(t, err)
	require.Equal(t, "match p = allShortestPaths((s)-[r:MemberOf*]->(e)) where id(s) = $p0 and id(e) = $p1 return p", renderPrepared(t, preparedQuery))

	preparedQuery, err = v2.New().WithShortestPaths().WithTraversalDepth(v2.MinDepth(1)).Where(
		v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
		v2.Start().ID().Equals(1),
		v2.End().ID().Equals(2),
	).Return(
		v2.Path(),
	).Build()
	require.NoError(t, err)
	require.Equal(t, "match p = shortestPath((s)-[r:MemberOf*1..]->(e)) where id(s) = $p0 and id(e) = $p1 return p", renderPrepared(t, preparedQuery))

	_, err = v2.New().WithShortestPaths().WithAllShortestPaths().Where(
		v2.Start().ID().Equals(1),
		v2.End().ID().Equals(2),
	).Return(
		v2.Path(),
	).Build()
	require.ErrorContains(t, err, "query is requesting both all shortest paths and shortest paths")

	_, err = v2.New().WithShortestPaths().Return(v2.Node()).Build()
	require.ErrorContains(t, err, "shortest path query requires relationship query identifiers")

	_, err = v2.New().WithAllShortestPaths().Return(v2.As(v2.Literal(1), "one")).Build()
	require.ErrorContains(t, err, "shortest path query requires relationship query identifiers")
}

func TestTraversalDepthControls(t *testing.T) {
	cases := map[string]struct {
		builder        v2.QueryBuilder
		expectedCypher string
		expectedParams map[string]any
	}{
		"any depth": {
			builder:        v2.New().WithTraversalDepth(v2.AnyDepth()).Return(v2.End()),
			expectedCypher: "match ()-[*]->(e) return e",
		},
		"minimum depth": {
			builder:        v2.New().WithTraversalDepth(v2.MinDepth(1)).Return(v2.End()),
			expectedCypher: "match ()-[*1..]->(e) return e",
		},
		"maximum depth": {
			builder:        v2.New().WithTraversalDepth(v2.MaxDepth(5)).Return(v2.End()),
			expectedCypher: "match ()-[*..5]->(e) return e",
		},
		"depth range": {
			builder: v2.New().WithTraversalDepth(v2.DepthRange(1, 5)).Where(
				v2.Relationship().Kind().IsOneOf(graph.Kinds{graph.StringKind("KindA"), graph.StringKind("KindB")}),
				v2.Start().ID().Equals(1),
				v2.End().Kinds().Has(graph.StringKind("User")),
			).Return(
				v2.Path(),
				v2.End(),
			),
			expectedCypher: "match p = (s)-[r:KindA|KindB*1..5]->(e) where id(s) = $p0 and e:User return p, e",
			expectedParams: map[string]any{"p0": 1},
		},
		"exact depth": {
			builder:        v2.New().WithTraversalDepth(v2.ExactDepth(3)).Return(v2.End()),
			expectedCypher: "match ()-[*3..3]->(e) return e",
		},
		"inbound depth range": {
			builder:        v2.New().WithTraversalDepth(v2.DepthRange(2, 5)).WithRelationshipDirection(graph.DirectionInbound).Return(v2.Start()),
			expectedCypher: "match (s)<-[*2..5]-() return s",
		},
		"path only": {
			builder:        v2.New().WithTraversalDepth(v2.AnyDepth()).Return(v2.Path()),
			expectedCypher: "match p = ()-[*]->() return p",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)
			require.Equal(t, testCase.expectedCypher, renderPrepared(t, preparedQuery))

			if testCase.expectedParams == nil {
				require.Empty(t, preparedQuery.Parameters)
			} else {
				require.Equal(t, testCase.expectedParams, preparedQuery.Parameters)
			}
		})
	}
}

func TestInvalidTraversalDepthControls(t *testing.T) {
	_, err := v2.New().WithTraversalDepth(v2.MinDepth(-1)).Return(v2.End()).Build()
	require.ErrorContains(t, err, "traversal depth minimum must be non-negative: -1")

	_, err = v2.New().WithTraversalDepth(v2.MaxDepth(-1)).Return(v2.End()).Build()
	require.ErrorContains(t, err, "traversal depth maximum must be non-negative: -1")

	_, err = v2.New().WithTraversalDepth(v2.DepthRange(3, 1)).Return(v2.End()).Build()
	require.ErrorContains(t, err, "traversal depth maximum 1 is less than minimum 3")

	_, err = v2.New().WithTraversalDepth(v2.AnyDepth()).Return(v2.Node()).Build()
	require.ErrorContains(t, err, "recursive traversal query requires relationship query identifiers")

	_, err = v2.New().WithTraversalDepth(v2.AnyDepth()).Where(
		v2.Relationship().Property("enabled").Equals(true),
	).Return(
		v2.End(),
	).Build()
	require.ErrorContains(t, err, "ranged relationship patterns only support top-level relationship kind constraints")

	_, err = v2.New().WithTraversalDepth(v2.AnyDepth()).Return(v2.Relationship()).Build()
	require.ErrorContains(t, err, "ranged relationship patterns do not support relationship projections or mutations")

	_, err = v2.New().WithTraversalDepth(v2.AnyDepth()).Where(
		v2.Start().ID().Equals(1),
	).Delete(
		v2.Relationship(),
	).Build()
	require.ErrorContains(t, err, "ranged relationship patterns do not support relationship projections or mutations")
}

func TestMixedNodeAndRelationshipIdentifiersReturnError(t *testing.T) {
	_, err := v2.New().Where(
		v2.Node().ID().Equals(1),
		v2.Relationship().ID().Equals(2),
	).Return(
		v2.Node(),
	).Build()
	require.ErrorContains(t, err, "query mixes node and relationship query identifiers")
}

func TestRawIdentifiersMustBeKnownToScope(t *testing.T) {
	cases := map[string]v2.QueryBuilder{
		"delete": v2.New().Where(
			v2.Node().ID().Equals(1),
		).Delete(
			v2.Variable("x"),
		),
		"projection": v2.New().Return(
			v2.Node(),
			v2.Variable("x"),
		),
		"sort": v2.New().Return(
			v2.Node(),
		).OrderBy(
			v2.Asc(v2.Variable("x")),
		),
	}

	for name, builder := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := builder.Build()
			require.ErrorContains(t, err, `query contains unknown identifier "x"`)
		})
	}
}

func TestPathIdentifierRequiresShortestPathMatch(t *testing.T) {
	_, err := v2.New().Return(
		v2.Node(),
		v2.Path(),
	).Build()
	require.ErrorContains(t, err, `query contains unbound identifier "p"`)

	_, err = v2.New().Return(
		v2.Relationship(),
		v2.Path(),
	).Build()
	require.ErrorContains(t, err, `query contains unbound identifier "p"`)
}

func TestCreatedRawIdentifiersDoNotRequireMatch(t *testing.T) {
	preparedQuery, err := v2.New().Create(&cypher.NodePattern{
		Variable: v2.Variable("created"),
	}).Return(
		v2.Variable("created"),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "create (created) return created", renderPrepared(t, preparedQuery))
}

func TestMultipleRelationshipKindMatchersRemainConjunctive(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.Relationship().Kind().Is(graph.StringKind("A")),
		v2.Relationship().Kind().Is(graph.StringKind("B")),
	).Return(
		v2.Relationship(),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match ()-[r]->() where r:A and r:B return r", renderPrepared(t, preparedQuery))
}

func TestEmptyLogicalHelpersReturnBuildErrors(t *testing.T) {
	_, err := v2.New().Where(v2.And()).Return(v2.Node()).Build()
	require.ErrorContains(t, err, "and requires at least one operand")

	_, err = v2.New().Where(v2.Or()).Return(v2.Node()).Build()
	require.ErrorContains(t, err, "or requires at least one operand")
}

func TestExplicitRelationshipPatternDirectionBoth(t *testing.T) {
	preparedQuery, err := v2.New().Create(
		v2.RelationshipPattern(graph.StringKind("Edge"), nil, graph.DirectionBoth),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "create (s)-[r:Edge]-(e)", renderPrepared(t, preparedQuery))
}

func TestInvalidExplicitRelationshipPatternDirectionReturnsError(t *testing.T) {
	_, err := v2.New().Create(
		v2.Relationship().RelationshipPattern(graph.StringKind("Edge"), nil, graph.Direction(99)),
	).Build()
	require.ErrorContains(t, err, "unsupported relationship direction: invalid")
}

func TestProjectionAndOrderHelpers(t *testing.T) {
	preparedQuery, err := v2.New().ReturnDistinct(
		v2.As(v2.Node().ID(), "node_id"),
	).OrderBy(
		v2.Node().Property("name"),
		v2.Desc(v2.Node().ID()),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) return distinct id(n) as node_id order by n.name asc, id(n) desc", renderPrepared(t, preparedQuery))
}

func TestInvalidSortDirectionReturnsError(t *testing.T) {
	_, err := v2.New().Return(v2.Node()).OrderBy(
		v2.Order(v2.Node().Property("name"), v2.SortDirection(99)),
	).Build()
	require.ErrorContains(t, err, "unsupported sort direction: 99")
}

func TestPaginationZeroValuesAndNegativeValidation(t *testing.T) {
	preparedQuery, err := v2.New().Return(v2.Node()).Skip(0).Limit(0).Build()
	require.NoError(t, err)
	require.Equal(t, "match (n) return n skip 0 limit 0", renderPrepared(t, preparedQuery))

	_, err = v2.New().Return(v2.Node()).Skip(-1).Build()
	require.ErrorContains(t, err, "skip must be non-negative: -1")

	_, err = v2.New().Return(v2.Node()).Limit(-1).Build()
	require.ErrorContains(t, err, "limit must be non-negative: -1")
}

func TestProjectionAliasDoesNotCreateMatchInference(t *testing.T) {
	preparedQuery, err := v2.New().Return(
		v2.As(v2.Literal(1), "one"),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "return 1 as one", renderPrepared(t, preparedQuery))
	require.Empty(t, preparedQuery.Parameters)
}

func TestAliasedProjectionCreatesMatchInference(t *testing.T) {
	preparedQuery, err := v2.New().Return(
		v2.As(v2.Node().ID(), "node_id"),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) return id(n) as node_id", renderPrepared(t, preparedQuery))

	preparedQuery, err = v2.New().Return(
		v2.As(v2.Node(), "alias"),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) return n as alias", renderPrepared(t, preparedQuery))
}

func TestInvalidProjectionAliasReturnsBuildError(t *testing.T) {
	_, err := v2.New().Return(v2.As(v2.Literal(1), "bad alias")).Build()
	require.ErrorContains(t, err, `projection alias has invalid symbol "bad alias"`)

	_, err = v2.New().Return(&cypher.ProjectionItem{
		Expression: v2.Literal(1),
		Alias:      cypher.NewVariableWithSymbol("1bad"),
	}).Build()
	require.ErrorContains(t, err, `projection alias has invalid symbol "1bad"`)
}

func TestUnsupportedOrderByTypeReturnsError(t *testing.T) {
	_, err := v2.New().Return(v2.Node()).OrderBy(123).Build()
	require.ErrorContains(t, err, "unsupported expression type: int")
}

func TestRawProjectionAndOrderInputsAreValidated(t *testing.T) {
	_, err := v2.New().Return(&cypher.Return{}).Build()
	require.ErrorContains(t, err, "return clause has nil projection")

	returnClause := cypher.NewReturn()
	returnClause.NewProjection(false).Items = append(returnClause.Projection.Items, &cypher.ProjectionItem{})
	_, err = v2.New().Return(returnClause).Build()
	require.ErrorContains(t, err, "projection item has nil expression")

	_, err = v2.New().Return(v2.Node()).OrderBy(&cypher.SortItem{}).Build()
	require.ErrorContains(t, err, "sort item has nil expression")

	_, err = v2.New().Return(v2.Node()).OrderBy(&cypher.Order{
		Items: []*cypher.SortItem{{}},
	}).Build()
	require.ErrorContains(t, err, "sort item has nil expression")
}

func TestRawProjectionAndOrderInputsAreNormalized(t *testing.T) {
	returnClause := cypher.NewReturn()
	returnClause.NewProjection(false).Items = append(returnClause.Projection.Items, v2.Node().ID())

	preparedQuery, err := v2.New().Return(returnClause).OrderBy(&cypher.Order{
		Items: []*cypher.SortItem{
			v2.Desc(v2.Node().Property("name")),
		},
	}).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) return id(n) order by n.name desc", renderPrepared(t, preparedQuery))
}

func TestRawReturnInputPreservesProjectionMetadata(t *testing.T) {
	returnClause := cypher.NewReturn()
	projection := returnClause.NewProjection(true)
	projection.Items = append(projection.Items, v2.Node().ID())
	projection.Order = &cypher.Order{
		Items: []*cypher.SortItem{
			v2.Desc(v2.Node().Property("name")),
		},
	}
	projection.Skip = cypher.NewSkip(5)
	projection.Limit = cypher.NewLimit(10)

	preparedQuery, err := v2.New().Return(returnClause).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) return distinct id(n) order by n.name desc skip 5 limit 10", renderPrepared(t, preparedQuery))
}

func TestRawReturnMetadataCreatesMatchInference(t *testing.T) {
	returnClause := cypher.NewReturn()
	projection := returnClause.NewProjection(false)
	projection.Items = append(projection.Items, v2.Literal(1))
	projection.Order = &cypher.Order{
		Items: []*cypher.SortItem{
			v2.Desc(v2.Node().Property("name")),
		},
	}

	preparedQuery, err := v2.New().Return(returnClause).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) return 1 order by n.name desc", renderPrepared(t, preparedQuery))
}

func TestRawReturnInputMergesWithBuilderProjectionControls(t *testing.T) {
	returnClause := cypher.NewReturn()
	projection := returnClause.NewProjection(true)
	projection.Items = append(projection.Items, v2.Node().ID())
	projection.Order = &cypher.Order{
		Items: []*cypher.SortItem{
			v2.Desc(v2.Node().Property("name")),
		},
	}
	projection.Skip = cypher.NewSkip(5)
	projection.Limit = cypher.NewLimit(10)

	preparedQuery, err := v2.New().Return(returnClause).OrderBy(
		v2.Asc(v2.Node().Property("created_at")),
	).Skip(15).Limit(20).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) return distinct id(n) order by n.name desc, n.created_at asc skip 15 limit 20", renderPrepared(t, preparedQuery))
}

func TestRawUpdatingInputsAreValidated(t *testing.T) {
	var setClause *cypher.Set
	_, err := v2.New().Update(setClause).Build()
	require.ErrorContains(t, err, "set clause is nil")

	_, err = v2.New().Update(&cypher.Set{Items: []*cypher.SetItem{nil}}).Build()
	require.ErrorContains(t, err, "set item is nil")

	_, err = v2.New().Update(&cypher.SetItem{}).Build()
	require.ErrorContains(t, err, "set item left has nil expression")

	var removeClause *cypher.Remove
	_, err = v2.New().Update(removeClause).Build()
	require.ErrorContains(t, err, "remove clause is nil")

	_, err = v2.New().Update(&cypher.Remove{Items: []*cypher.RemoveItem{nil}}).Build()
	require.ErrorContains(t, err, "remove item is nil")

	_, err = v2.New().Update(&cypher.RemoveItem{}).Build()
	require.ErrorContains(t, err, "remove item has no target")

	var deleteVariable *cypher.Variable
	_, err = v2.New().Delete(deleteVariable).Build()
	require.ErrorContains(t, err, "delete expression has nil expression")

	var nodePattern *cypher.NodePattern
	_, err = v2.New().Create(nodePattern).Build()
	require.ErrorContains(t, err, "node pattern is nil")

	var relationshipPattern *cypher.RelationshipPattern
	_, err = v2.New().Create(relationshipPattern).Build()
	require.ErrorContains(t, err, "relationship pattern is nil")
}

func TestDeleteRejectsNonTargetQualifiedExpressions(t *testing.T) {
	cases := map[string]any{
		"property continuation": v2.Node().Property("name"),
		"raw property lookup":   cypher.NewPropertyLookup("n", "name"),
		"id":                    v2.Node().ID(),
		"kinds":                 v2.Node().Kinds(),
		"kind":                  v2.Relationship().Kind(),
	}

	for name, target := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := v2.New().Delete(target).Build()
			require.ErrorContains(t, err, "delete target must be a node, relationship, or variable")
		})
	}
}

func TestDeleteRejectsPathTargets(t *testing.T) {
	_, err := v2.New().Delete(v2.Path()).Build()
	require.ErrorContains(t, err, `delete target must be a node or relationship variable, got path variable "p"`)

	_, err = v2.New().Delete(v2.Variable("p")).Build()
	require.ErrorContains(t, err, `delete target must be a node or relationship variable, got path variable "p"`)
}

func TestInvalidHelperInputsReturnBuildErrors(t *testing.T) {
	cases := map[string]struct {
		builder v2.QueryBuilder
		err     string
	}{
		"aliased projection": {
			builder: v2.New().Return(v2.As(123, "bad")),
			err:     "unsupported expression type: int",
		},
		"sort item": {
			builder: v2.New().Return(v2.Node()).OrderBy(v2.Desc(123)),
			err:     "unsupported expression type: int",
		},
		"set properties": {
			builder: v2.New().Update(v2.SetProperties(123, map[string]any{"name": "bad"})),
			err:     "unsupported expression type: int",
		},
		"delete properties": {
			builder: v2.New().Update(v2.DeleteProperties(123, "name")),
			err:     "unsupported expression type: int",
		},
		"pattern predicate": {
			builder: v2.New().Where(v2.HasRelationships(123)).Return(v2.Node()),
			err:     "unsupported expression type: int",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := testCase.builder.Build()
			require.ErrorContains(t, err, testCase.err)
		})
	}
}

func TestNamedParameterMaterialization(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.Node().Property("first").Equals("auto"),
		v2.Node().Property("second").Equals(v2.NamedParameter("p0", "named")),
	).Return(
		v2.Node(),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) where n.first = $p1 and n.second = $p0 return n", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": "named",
		"p1": "auto",
	}, preparedQuery.Parameters)

	_, err = v2.New().Where(
		v2.Node().Property("name").Equals(v2.NamedParameter("bad name", "value")),
	).Return(
		v2.Node(),
	).Build()
	require.ErrorContains(t, err, `parameter has invalid symbol "bad name"`)

	_, err = v2.New().Where(
		v2.Node().Property("first").Equals(v2.NamedParameter("same", "first")),
		v2.Node().Property("second").Equals(v2.NamedParameter("same", "second")),
	).Return(
		v2.Node(),
	).Build()
	require.ErrorContains(t, err, "parameter same is bound to multiple values")
}

func TestQualifiedExpressionValuesUseProjectionSemantics(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.Node().Property("copy").Equals(v2.Node().Property("source")),
		v2.Node().Property("kinds").Equals(v2.Node().Kinds()),
	).Return(
		v2.Node(),
	).Build()
	require.NoError(t, err)
	require.Equal(t, "match (n) where n.copy = n.source and n.kinds = labels(n) return n", renderPrepared(t, preparedQuery))

	preparedQuery, err = v2.New().Where(
		v2.Relationship().Property("kind").Equals(v2.Relationship().Kind()),
	).Return(
		v2.Relationship(),
	).Build()
	require.NoError(t, err)
	require.Equal(t, "match ()-[r]->() where r.kind = type(r) return r", renderPrepared(t, preparedQuery))
}

func TestBuildDoesNotMutateCallerOwnedAST(t *testing.T) {
	constraint := v2.Node().Property("name").Equals("alice")
	constraintParameter := constraint.(*cypher.Comparison).FirstPartial().Right.(*cypher.Parameter)

	preparedQuery, err := v2.New().Where(constraint).Return(v2.Node()).Build()
	require.NoError(t, err)
	require.Equal(t, map[string]any{"p0": "alice"}, preparedQuery.Parameters)
	require.Empty(t, constraintParameter.Symbol)

	setItem := v2.SetProperty(v2.Node().Property("status"), "active")
	setParameter := setItem.Right.(*cypher.Parameter)

	preparedQuery, err = v2.New().Where(v2.Node().ID().Equals(1)).Update(setItem).Build()
	require.NoError(t, err)
	require.Equal(t, map[string]any{"p0": 1, "p1": "active"}, preparedQuery.Parameters)
	require.Empty(t, setParameter.Symbol)

	createPattern := v2.NodePattern(graph.Kinds{graph.StringKind("User")}, v2.Parameter(map[string]any{"name": "node"}))
	createParameter := createPattern.Properties.(*cypher.Parameter)

	preparedQuery, err = v2.New().Create(createPattern).Build()
	require.NoError(t, err)
	require.Equal(t, map[string]any{"p0": map[string]any{"name": "node"}}, preparedQuery.Parameters)
	require.Empty(t, createParameter.Symbol)

	rawReturn := cypher.NewReturn()
	rawReturn.NewProjection(false).AddItem(cypher.NewProjectionItemWithExpr(v2.Parameter("projected")))
	rawReturnParameter := rawReturn.Projection.Items[0].(*cypher.ProjectionItem).Expression.(*cypher.Parameter)

	preparedQuery, err = v2.New().Return(rawReturn).Build()
	require.NoError(t, err)
	require.Equal(t, map[string]any{"p0": "projected"}, preparedQuery.Parameters)
	require.Empty(t, rawReturnParameter.Symbol)
}

func TestCompatibilityHelpers(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.And(
			v2.InIDs(v2.NodeID(), 1, 2),
			v2.KindIn(v2.Node(), graph.StringKind("User")),
			v2.CaseInsensitiveStringContains(v2.Node().Property("name"), "ADMIN"),
			v2.IsNotNull(v2.Node().Property("enabled")),
		),
	).Return(
		v2.CountDistinct(v2.Node()),
		v2.KindsOf(v2.Node()),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) where id(n) in $p0 and n:User and toLower(n.name) contains $p1 and n.enabled is not null return count(distinct n), labels(n)", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": []graph.ID{1, 2},
		"p1": "admin",
	}, preparedQuery.Parameters)
}

func TestUpdateCompatibilityHelpers(t *testing.T) {
	preparedQuery, err := v2.New().Where(
		v2.Node().ID().Equals(1),
	).Update(
		v2.AddKind(v2.Node(), graph.StringKind("Enabled")),
		v2.SetProperties(v2.Node(), map[string]any{"name": "updated"}),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "match (n) where id(n) = $p0 set n:Enabled, n.name = $p1", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": 1,
		"p1": "updated",
	}, preparedQuery.Parameters)
}

func TestFluentMutationHelpersReturnConcreteMutationTypes(t *testing.T) {
	kinds := graph.Kinds{graph.StringKind("Enabled")}

	var addItem *cypher.SetItem = v2.Node().Kinds().Add(kinds)
	require.NotNil(t, addItem)

	var removeItem *cypher.RemoveItem = v2.Node().Kinds().Remove(kinds)
	require.NotNil(t, removeItem)

	var nodeSet *cypher.Set = v2.Node().SetProperties(map[string]any{"name": "updated"})
	require.Len(t, nodeSet.Items, 1)

	var nodeRemove *cypher.Remove = v2.Node().RemoveProperties([]string{"stale"})
	require.Len(t, nodeRemove.Items, 1)

	var relationshipSet *cypher.Set = v2.Relationship().SetProperties(map[string]any{"name": "updated"})
	require.Len(t, relationshipSet.Items, 1)

	var relationshipRemove *cypher.Remove = v2.Relationship().RemoveProperties([]string{"stale"})
	require.Len(t, relationshipRemove.Items, 1)
}

func TestSetPropertiesSortsKeys(t *testing.T) {
	properties := map[string]any{
		"zeta":  3,
		"alpha": 1,
		"mid":   2,
	}

	preparedQuery, err := v2.New().Update(
		v2.SetProperties(v2.Node(), properties),
	).Build()
	require.NoError(t, err)
	require.Equal(t, "match (n) set n.alpha = $p0, n.mid = $p1, n.zeta = $p2", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": 1,
		"p1": 2,
		"p2": 3,
	}, preparedQuery.Parameters)

	preparedQuery, err = v2.New().Update(
		v2.Node().SetProperties(properties),
	).Build()
	require.NoError(t, err)
	require.Equal(t, "match (n) set n.alpha = $p0, n.mid = $p1, n.zeta = $p2", renderPrepared(t, preparedQuery))
	require.Equal(t, map[string]any{
		"p0": 1,
		"p1": 2,
		"p2": 3,
	}, preparedQuery.Parameters)
}
