package v2_test

import (
	"context"
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
			expectedCypher: "match (s)-[r]->(e) where id(s) = $p0 return id(s), id(r), id(e)",
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
			expectedSQL:    "with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.kind_ids operator (pg_catalog.&&) array [1]::int2[] and (n0.properties ->> 'name') like '%' || @pi0::text || '%')) select (s0.n0).id, (s0.n0).kind_ids from s0;",
			expectedParams: map[string]any{"p0": "admin", "pi0": "admin"},
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
			expectedParams: map[string]any{"p0": 1, "pi0": 1},
		},
		"create relationship": {
			builder: v2.New().Where(
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Create(
				v2.RelationshipPattern(edgeKind, nil, graph.DirectionOutbound),
			).Return(
				v2.Relationship().ID(),
			),
			expectedSQL:    "with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.id = @pi0::int8)), s1 as (select s0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from s0, node n1 where (n1.id = @pi1::int8)) select e0.id from s1 where e0.kind_id = any (array [2]::int2[]);",
			expectedParams: map[string]any{"p0": 1, "p1": 2, "pi0": 1, "pi1": 2},
		},
		"update node": {
			builder: v2.New().Where(
				v2.Node().ID().Equals(1),
			).Update(
				v2.SetProperty(v2.Node().Property("name"), "updated"),
			),
			expectedSQL:    "with s0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0 where (n0.id = @pi0::int8)), s1 as (update node n1 set properties = n1.properties || jsonb_build_object('name', @pi1::text)::jsonb from s0 where (s0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0) select 1;",
			expectedParams: map[string]any{"p0": 1, "p1": "updated", "pi0": 1, "pi1": "updated"},
		},
		"delete relationship": {
			builder: v2.New().Where(
				v2.Relationship().ID().Equals(1),
			).Delete(
				v2.Relationship(),
			),
			expectedSQL:    "with s0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0, (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1 from edge e0 join node n0 on n0.id = e0.start_id join node n1 on n1.id = e0.end_id where (e0.id = @pi0::int8)), s1 as (delete from edge e1 using s0 where (s0.e0).id = e1.id) select 1;",
			expectedParams: map[string]any{"p0": 1, "pi0": 1},
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)

			translation, err := translate.Translate(context.Background(), preparedQuery.Query, mapper, preparedQuery.Parameters)
			require.NoError(t, err)

			sql, err := translate.Translated(translation)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedSQL, sql)
			require.Equal(t, testCase.expectedParams, translation.Parameters)
		})
	}
}
