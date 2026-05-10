package v2_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	legacyquery "github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/query/neo4j"
	v2 "github.com/specterops/dawgs/query/v2"
	"github.com/stretchr/testify/require"
)

func renderNeo4jQuery(t *testing.T, regularQuery *cypher.RegularQuery, prepareAllShortestPaths bool) (string, map[string]any) {
	t.Helper()

	queryBuilder := neo4j.NewQueryBuilder(regularQuery)

	if prepareAllShortestPaths {
		require.NoError(t, queryBuilder.PrepareAllShortestPaths())
	} else {
		require.NoError(t, queryBuilder.Prepare())
	}

	rendered, err := queryBuilder.Render()
	require.NoError(t, err)

	return rendered, queryBuilder.Parameters
}

func assertLegacyNeo4jParity(t *testing.T, legacyQuery *cypher.RegularQuery, v2Builder v2.QueryBuilder, prepareLegacyAllShortestPaths bool) {
	t.Helper()

	preparedQuery, err := v2Builder.Build()
	require.NoError(t, err)

	legacyRendered, legacyParameters := renderNeo4jQuery(t, legacyQuery, prepareLegacyAllShortestPaths)
	v2Rendered, v2Parameters := renderNeo4jQuery(t, preparedQuery.Query, false)

	require.Equal(t, legacyRendered, v2Rendered)
	require.Equal(t, legacyParameters, v2Parameters)
}

func TestLegacyNeo4jParity(t *testing.T) {
	userKind := graph.StringKind("User")
	edgeKind := graph.StringKind("MemberOf")

	t.Run("node count by kind", func(t *testing.T) {
		assertLegacyNeo4jParity(t,
			legacyquery.SinglePartQuery(
				legacyquery.Where(
					legacyquery.KindIn(legacyquery.Node(), userKind),
				),
				legacyquery.Returning(
					legacyquery.Count(legacyquery.Node()),
				),
			),
			v2.New().Where(
				v2.Node().Kinds().Has(userKind),
			).Return(
				v2.Node().Count(),
			),
			false,
		)
	})

	t.Run("node read with pagination", func(t *testing.T) {
		assertLegacyNeo4jParity(t,
			legacyquery.SinglePartQuery(
				legacyquery.Where(
					legacyquery.And(
						legacyquery.StringContains(legacyquery.NodeProperty("name"), "admin"),
						legacyquery.IsNotNull(legacyquery.NodeProperty("enabled")),
					),
				),
				legacyquery.Returning(
					legacyquery.Node(),
					legacyquery.OrderBy(legacyquery.Order(legacyquery.NodeProperty("name"), legacyquery.Ascending())),
					legacyquery.Offset(0),
					legacyquery.Limit(0),
				),
			),
			v2.New().Where(
				v2.And(
					v2.Node().Property("name").Contains("admin"),
					v2.Node().Property("enabled").IsNotNull(),
				),
			).Return(
				v2.Node(),
			).OrderBy(
				v2.Asc(v2.Node().Property("name")),
			).Skip(0).Limit(0),
			false,
		)
	})

	t.Run("node read with or and adjacent predicate", func(t *testing.T) {
		assertLegacyNeo4jParity(t,
			legacyquery.SinglePartQuery(
				legacyquery.Where(
					legacyquery.And(
						legacyquery.Or(
							legacyquery.Equals(legacyquery.NodeProperty("name"), "alice"),
							legacyquery.Equals(legacyquery.NodeProperty("name"), "bob"),
						),
						legacyquery.IsNotNull(legacyquery.NodeProperty("enabled")),
					),
				),
				legacyquery.Returning(
					legacyquery.Node(),
				),
			),
			v2.New().Where(
				v2.Or(
					v2.Node().Property("name").Equals("alice"),
					v2.Node().Property("name").Equals("bob"),
				),
				v2.Node().Property("enabled").IsNotNull(),
			).Return(
				v2.Node(),
			),
			false,
		)
	})

	t.Run("relationship read", func(t *testing.T) {
		assertLegacyNeo4jParity(t,
			legacyquery.SinglePartQuery(
				legacyquery.Where(
					legacyquery.And(
						legacyquery.KindIn(legacyquery.Relationship(), edgeKind),
						legacyquery.Equals(legacyquery.StartID(), 1),
						legacyquery.Equals(legacyquery.EndID(), 2),
					),
				),
				legacyquery.Returning(
					legacyquery.StartID(),
					legacyquery.RelationshipID(),
					legacyquery.EndID(),
				),
			),
			v2.New().Where(
				v2.Relationship().Kind().Is(edgeKind),
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Return(
				v2.Start().ID(),
				v2.Relationship().ID(),
				v2.End().ID(),
			),
			false,
		)
	})

	t.Run("create relationship with matched endpoints", func(t *testing.T) {
		properties := map[string]any{"name": "rel"}

		assertLegacyNeo4jParity(t,
			legacyquery.SinglePartQuery(
				legacyquery.Where(
					legacyquery.And(
						legacyquery.Equals(legacyquery.StartID(), 1),
						legacyquery.Equals(legacyquery.EndID(), 2),
					),
				),
				legacyquery.Create(
					legacyquery.Start(),
					legacyquery.RelationshipPattern(edgeKind, legacyquery.Parameter(properties), graph.DirectionOutbound),
					legacyquery.End(),
				),
				legacyquery.Returning(
					legacyquery.RelationshipID(),
				),
			),
			v2.New().Where(
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Create(
				v2.Start(),
				v2.RelationshipPattern(edgeKind, v2.Parameter(properties), graph.DirectionOutbound),
				v2.End(),
			).Return(
				v2.Relationship().ID(),
			),
			false,
		)
	})

	t.Run("all shortest paths", func(t *testing.T) {
		assertLegacyNeo4jParity(t,
			legacyquery.SinglePartQuery(
				legacyquery.Where(
					legacyquery.And(
						legacyquery.KindIn(legacyquery.Relationship(), edgeKind),
						legacyquery.Equals(legacyquery.StartID(), 1),
						legacyquery.Equals(legacyquery.EndID(), 2),
					),
				),
				legacyquery.Returning(
					legacyquery.Path(),
				),
			),
			v2.New().WithAllShortestPaths().Where(
				v2.Relationship().Kind().Is(edgeKind),
				v2.Start().ID().Equals(1),
				v2.End().ID().Equals(2),
			).Return(
				v2.Path(),
			),
			true,
		)
	})
}
