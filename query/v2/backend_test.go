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
	cases := map[string]v2.QueryBuilder{
		"node read": v2.New().Where(
			v2.Node().Kinds().Has(graph.StringKind("User")),
			v2.Node().Property("name").Contains("admin"),
		).Return(
			v2.Node(),
		).OrderBy(
			v2.Node().Property("name"),
		),
		"relationship read": v2.New().Where(
			v2.Relationship().Kind().Is(graph.StringKind("MemberOf")),
			v2.Start().ID().Equals(1),
		).Return(
			v2.Start().ID(),
			v2.Relationship().ID(),
			v2.End().ID(),
		),
		"create node": v2.New().Create(
			v2.NodePattern(graph.Kinds{graph.StringKind("User")}, v2.NamedParameter("props", map[string]any{"name": "u"})),
		).Return(
			v2.Node().ID(),
		),
		"update node": v2.New().Where(
			v2.Node().ID().Equals(1),
		).Update(
			v2.SetProperty(v2.Node().Property("name"), "updated"),
		),
		"delete relationship": v2.New().Where(
			v2.Relationship().ID().Equals(1),
		).Delete(
			v2.Relationship(),
		),
	}

	for name, builder := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := builder.Build()
			require.NoError(t, err)

			queryBuilder := neo4j.NewQueryBuilder(preparedQuery.Query)
			require.NoError(t, queryBuilder.Prepare())

			rendered, err := queryBuilder.Render()
			require.NoError(t, err)
			require.NotEmpty(t, rendered)
			require.NotEmpty(t, queryBuilder.Parameters)
		})
	}
}

func TestBackendParityPGTranslate(t *testing.T) {
	userKind := graph.StringKind("User")
	edgeKind := graph.StringKind("MemberOf")
	mapper := testKindMapper(userKind, edgeKind)

	cases := map[string]v2.QueryBuilder{
		"node read": v2.New().Where(
			v2.Node().Kinds().Has(userKind),
			v2.Node().Property("name").Contains("admin"),
		).Return(
			v2.Node().ID(),
			v2.Node().Kinds(),
		),
		"relationship read": v2.New().Where(
			v2.Relationship().Kind().Is(edgeKind),
			v2.Start().ID().Equals(1),
		).Return(
			v2.Start().ID(),
			v2.Relationship().ID(),
			v2.End().ID(),
		),
		"create relationship": v2.New().Where(
			v2.Start().ID().Equals(1),
			v2.End().ID().Equals(2),
		).Create(
			v2.RelationshipPattern(edgeKind, nil, graph.DirectionOutbound),
		).Return(
			v2.Relationship().ID(),
		),
		"update node": v2.New().Where(
			v2.Node().ID().Equals(1),
		).Update(
			v2.SetProperty(v2.Node().Property("name"), "updated"),
		),
		"delete relationship": v2.New().Where(
			v2.Relationship().ID().Equals(1),
		).Delete(
			v2.Relationship(),
		),
	}

	for name, builder := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := builder.Build()
			require.NoError(t, err)

			translation, err := translate.Translate(context.Background(), preparedQuery.Query, mapper, preparedQuery.Parameters)
			require.NoError(t, err)

			sql, err := translate.Translated(translation)
			require.NoError(t, err)
			require.NotEmpty(t, sql)
		})
	}
}
