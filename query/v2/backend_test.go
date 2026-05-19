package v2_test

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query/neo4j"
	v2 "github.com/specterops/dawgs/query/v2"
	"github.com/stretchr/testify/require"
)

func testKindMapper(kinds ...graph.Kind) *pgutil.InMemoryKindMapper {
	mapper := pgutil.NewInMemoryKindMapper()

	for _, kind := range kinds {
		mapper.Put(kind)
	}

	return mapper
}

func TestBackendParityNeo4jPrepare(t *testing.T) {
	cases := map[string]struct {
		builder        v2.QueryBuilder
		expectedCypher string
		expectedParams map[string]any
	}{
		"node read": {
			builder: v2.New().Where(
				v2.Node().Kinds().Has(graph.StringKind("User")),
				v2.Node().Property("name").Contains("admin"),
			).Return(
				v2.Node(),
			).OrderBy(
				v2.Node().Property("name"),
			),
			expectedCypher: "match (n) where n:User and n.name contains $p0 return n order by n.name asc",
			expectedParams: map[string]any{"p0": "admin"},
		},
		"relationship read": {
			builder: v2.New().Where(
				v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
				v2.Start().ID().Equals(1),
			).Return(
				v2.Start().ID(),
				v2.Relationship().ID(),
				v2.End().ID(),
			),
			expectedCypher: "match (s)-[r:MemberOf]->(e) where id(s) = $p0 return id(s), id(r), id(e)",
			expectedParams: map[string]any{"p0": 1},
		},
		"shortest path": {
			builder: v2.New().WithShortestPaths().Where(
				v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Return(
				v2.Path(),
			),
			expectedCypher: "match p = shortestPath((s)-[r:MemberOf*]->(e)) where id(s) = $p0 and id(e) = $p1 return p",
			expectedParams: map[string]any{"p0": 1, "p1": 2},
		},
		"all shortest paths": {
			builder: v2.New().WithAllShortestPaths().Where(
				v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Return(
				v2.Path(),
			),
			expectedCypher: "match p = allShortestPaths((s)-[r:MemberOf*]->(e)) where id(s) = $p0 and id(e) = $p1 return p",
			expectedParams: map[string]any{"p0": 1, "p1": 2},
		},
		"recursive traversal": {
			builder: v2.New().WithTraversalDepth(v2.DepthRange(1, 2)).Where(
				v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
				v2.Start().ID().Equals(1),
			).Return(
				v2.Path(),
				v2.End().ID(),
			),
			expectedCypher: "match p = (s)-[r:MemberOf*1..2]->(e) where id(s) = $p0 return p, id(e)",
			expectedParams: map[string]any{"p0": 1},
		},
		"create node": {
			builder: v2.New().Create(
				v2.NodePattern(graph.Kinds{graph.StringKind("User")}, v2.NamedParameter("props", map[string]any{"name": "u"})),
			).Return(
				v2.Node().ID(),
			),
			expectedCypher: "create (n:User $p0) return id(n)",
			expectedParams: map[string]any{"p0": map[string]any{"name": "u"}},
		},
		"update node": {
			builder: v2.New().Where(
				v2.Node().ID().Equals(1),
			).Update(
				v2.SetProperty(v2.Node().Property("name"), "updated"),
			),
			expectedCypher: "match (n) where id(n) = $p0 set n.name = $p1",
			expectedParams: map[string]any{"p0": 1, "p1": "updated"},
		},
		"delete relationship": {
			builder: v2.New().Where(
				v2.Relationship().ID().Equals(1),
			).Delete(
				v2.Relationship(),
			),
			expectedCypher: "match ()-[r]->() where id(r) = $p0 delete r",
			expectedParams: map[string]any{"p0": 1},
		},
		"delete node": {
			builder: v2.New().Where(
				v2.Node().ID().Equals(1),
			).Delete(
				v2.Node(),
			),
			expectedCypher: "match (n) where id(n) = $p0 detach delete n",
			expectedParams: map[string]any{"p0": 1},
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)

			queryBuilder := neo4j.NewQueryBuilder(preparedQuery.Query)
			require.NoError(t, queryBuilder.Prepare())

			rendered, err := queryBuilder.Render()
			require.NoError(t, err)
			require.Equal(t, testCase.expectedCypher, rendered)
			require.Equal(t, testCase.expectedParams, queryBuilder.Parameters)
		})
	}
}

func TestBackendParityPGTranslateTraversalDepth(t *testing.T) {
	edgeKind := graph.StringKind("MemberOf")
	mapper := testKindMapper(edgeKind)

	cases := map[string]struct {
		builder             v2.QueryBuilder
		expectedSQLContains []string
	}{
		"path": {
			builder: v2.New().WithTraversalDepth(v2.DepthRange(1, 2)).Where(
				v2.Relationship().Kind().Is(edgeKind),
				v2.Start().ID().Equals(1),
			).Return(
				v2.Path(),
			),
			expectedSQLContains: []string{
				"with recursive",
				"ordered_edges_to_path",
				"n0.id = @pi0::int8",
				"e0.kind_id = any (array [1]::int2[])",
				"depth < 2",
			},
		},
		"endpoints": {
			builder: v2.New().WithTraversalDepth(v2.DepthRange(1, 2)).Where(
				v2.Relationship().Kind().Is(edgeKind),
				v2.Start().ID().Equals(1),
			).Return(
				v2.Start().ID(),
				v2.End().ID(),
			),
			expectedSQLContains: []string{
				"with recursive",
				"n0.id = @pi0::int8",
				"e0.kind_id = any (array [1]::int2[])",
				"depth < 2",
				"select (s0.n0).id, (s0.n1).id from s0",
			},
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)

			translation, err := translate.Translate(context.Background(), preparedQuery.Query, mapper, preparedQuery.Parameters, translate.DefaultGraphID)
			require.NoError(t, err)

			sql, err := translate.Translated(translation)
			require.NoError(t, err)

			for _, expected := range testCase.expectedSQLContains {
				require.Contains(t, sql, expected)
			}
		})
	}
}

func TestBackendParityPGTranslate(t *testing.T) {
	userKind := graph.StringKind("User")
	edgeKind := graph.StringKind("MemberOf")
	mapper := testKindMapper(userKind, edgeKind)

	cases := map[string]struct {
		builder        v2.QueryBuilder
		expectedSQL    string
		expectedParams map[string]any
	}{
		"node read": {
			builder: v2.New().Where(
				v2.Node().Kinds().Has(userKind),
				v2.Node().Property("name").Contains("admin"),
			).Return(
				v2.Node().ID(),
				v2.Node().Kinds(),
			),
			expectedSQL:    "with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.kind_ids operator (pg_catalog.&&) array [1]::int2[] and cypher_contains((n0.properties ->> 'name'), (@pi0::text)::text)::bool)) select (s0.n0).id, (array(select _kind.name from generate_subscripts((s0.n0).kind_ids, 1) as _kind_idx, kind _kind where _kind.id = ((s0.n0).kind_ids)[_kind_idx] order by _kind_idx))::text[] from s0;",
			expectedParams: map[string]any{"pi0": "admin"},
		},
		"relationship read": {
			builder: v2.New().Where(
				v2.Relationship().Kind().Is(edgeKind),
				v2.Start().ID().Equals(1),
			).Return(
				v2.Start().ID(),
				v2.Relationship().ID(),
				v2.End().ID(),
			),
			expectedSQL:    "with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on (n0.id = @pi0::int8) and n0.id = e0.start_id join node n1 on n1.id = e0.end_id where e0.kind_id = any (array [2]::int2[])) select (s0.n0).id, (s0.e0).id, (s0.n1).id from s0;",
			expectedParams: map[string]any{"pi0": 1},
		},
		"update node": {
			builder: v2.New().Where(
				v2.Node().ID().Equals(1),
			).Update(
				v2.SetProperty(v2.Node().Property("name"), "updated"),
			),
			expectedSQL:    "with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.id = @pi0::int8)), s1 as (update node n1 set properties = n1.properties || jsonb_build_object('name', @pi1::text)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select 1;",
			expectedParams: map[string]any{"pi0": 1, "pi1": "updated"},
		},
		"delete relationship": {
			builder: v2.New().Where(
				v2.Relationship().ID().Equals(1),
			).Delete(
				v2.Relationship(),
			),
			expectedSQL:    "with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (e0.id = @pi0::int8)), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;",
			expectedParams: map[string]any{"pi0": 1},
		},
		"delete node": {
			builder: v2.New().Where(
				v2.Node().ID().Equals(1),
			).Delete(
				v2.Node(),
			),
			expectedSQL:    "with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.id = @pi0::int8)), s1 as (delete from node n1 using s0 where (s0.n0).id = n1.id) select 1;",
			expectedParams: map[string]any{"pi0": 1},
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)

			translation, err := translate.Translate(context.Background(), preparedQuery.Query, mapper, preparedQuery.Parameters, translate.DefaultGraphID)
			require.NoError(t, err)

			sql, err := translate.Translated(translation)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedSQL, sql)
			require.Equal(t, testCase.expectedParams, translation.Parameters)
		})
	}
}

func TestBackendParityPGTranslateShortestPaths(t *testing.T) {
	edgeKind := graph.StringKind("MemberOf")
	mapper := testKindMapper(edgeKind)

	cases := map[string]struct {
		builder         v2.QueryBuilder
		expectedHarness string
	}{
		"shortest path": {
			builder: v2.New().WithShortestPaths().Where(
				v2.Relationship().Kind().Is(edgeKind),
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Return(
				v2.Path(),
			),
			expectedHarness: "bidirectional_sp_harness",
		},
		"all shortest paths": {
			builder: v2.New().WithAllShortestPaths().Where(
				v2.Relationship().Kind().Is(edgeKind),
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Return(
				v2.Path(),
			),
			expectedHarness: "bidirectional_asp_harness",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)

			translation, err := translate.Translate(context.Background(), preparedQuery.Query, mapper, preparedQuery.Parameters, translate.DefaultGraphID)
			require.NoError(t, err)

			sql, err := translate.Translated(translation)
			require.NoError(t, err)
			require.Contains(t, sql, testCase.expectedHarness)
			require.Contains(t, sql, "ordered_edges_to_path")
			require.Contains(t, sql, "n0.id = 1")
			require.Contains(t, sql, "n1.id = 2")

			serializedHarnessQueryHasKindConstraint := false
			for _, parameterValue := range translation.Parameters {
				if serializedQuery, typeOK := parameterValue.(string); typeOK && strings.Contains(serializedQuery, "array [1]::int2[]") {
					serializedHarnessQueryHasKindConstraint = true
					break
				}
			}
			require.True(t, serializedHarnessQueryHasKindConstraint, "expected serialized shortest-path harness query to contain edge kind constraint: %#v", translation.Parameters)
		})
	}
}

func TestBackendParityPGCreate(t *testing.T) {
	edgeKind := graph.StringKind("MemberOf")
	mapper := testKindMapper(edgeKind)

	preparedQuery, err := v2.New().Where(
		v2.Start().ID().Equals(1),
		v2.End().ID().Equals(2),
	).Create(
		v2.RelationshipPattern(edgeKind, nil, graph.DirectionOutbound),
	).Return(
		v2.Relationship().ID(),
	).Build()
	require.NoError(t, err)

	translation, err := translate.Translate(context.Background(), preparedQuery.Query, mapper, preparedQuery.Parameters, translate.DefaultGraphID)
	require.NoError(t, err)

	sql, err := translate.Translated(translation)
	require.NoError(t, err)
	require.Contains(t, sql, "insert into edge")
	require.Contains(t, sql, "graph_id")
	require.Contains(t, sql, "kind_id")
}
