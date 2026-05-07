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
}

func TestInvalidRelationshipDirectionReturnsError(t *testing.T) {
	_, err := v2.New().WithRelationshipDirection(graph.Direction(99)).Return(v2.Relationship()).Build()
	require.ErrorContains(t, err, "unsupported relationship direction: invalid")

	_, err = v2.New().WithRelationshipDirection(graph.DirectionBoth).Return(v2.Relationship()).Build()
	require.ErrorContains(t, err, "unsupported relationship direction: both")
}

func TestInvalidExplicitRelationshipPatternDirectionReturnsError(t *testing.T) {
	_, err := v2.New().Create(
		v2.RelationshipPattern(graph.StringKind("Edge"), nil, graph.DirectionBoth),
	).Build()
	require.ErrorContains(t, err, "unsupported relationship direction: both")

	_, err = v2.New().Create(
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

func TestProjectionAliasDoesNotCreateMatchInference(t *testing.T) {
	preparedQuery, err := v2.New().Return(
		v2.As(v2.Literal(1), "one"),
	).Build()
	require.NoError(t, err)

	require.Equal(t, "return 1 as one", renderPrepared(t, preparedQuery))
	require.Empty(t, preparedQuery.Parameters)
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
