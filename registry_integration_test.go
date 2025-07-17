//go:build manual_integration

package dawgs_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	//pg_v2 "github.com/specterops/dawgs/drivers/pg/v2"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/database/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	ctx := context.Background()

	graphDB, err := database.Open(ctx, neo4j.DriverName, database.Config{
		GraphQueryMemoryLimit: size.Gibibyte * 1,
		ConnectionString:      "neo4j://neo4j:neo4jj@localhost:7687",
	})

	//graphDB, err := v2.Open(ctx, pg_v2.DriverName, v2.Config{
	//	GraphQueryMemoryLimit: size.Gibibyte * 1,
	//	ConnectionString:      "postgresql://postgres:postgres@localhost:5432/bhe",
	//})

	require.NoError(t, err)

	require.NoError(t, graphDB.AssertSchema(ctx, database.NewSchema(
		"default",
		database.Graph{
			Name:  "default",
			Nodes: graph.Kinds{graph.StringKind("Node")},
			Edges: graph.Kinds{graph.StringKind("Edge")},
			NodeIndexes: []database.Index{{
				Name:  "node_label_name_index",
				Field: "name",
				Type:  database.IndexTypeTextSearch,
			}},
		})))

	preparedQuery, err := query.New().Return(query.Node()).Limit(10).Build()
	require.NoError(t, err)

	require.NoError(t, graphDB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		return driver.CreateNode(ctx, graph.PrepareNode(graph.AsProperties(map[string]any{
			"name": "THAT NODE",
		}), graph.StringKind("Node")))
	}))

	require.NoError(t, graphDB.Session(ctx, database.FetchNodes(preparedQuery, func(node *graph.Node) error {
		slog.Info(fmt.Sprintf("Got result from DB: %v", node))
		return nil
	})))

	require.NoError(t, graphDB.Transaction(ctx, database.FetchNodes(preparedQuery, func(node *graph.Node) error {
		slog.Info(fmt.Sprintf("Got result from DB: %v", node))
		return nil
	})))

	//require.NoError(t, graphDB.Transaction(ctx, func(ctx context.Context, driver v2.Driver) error {
	//	builder := v2.Query().Create(
	//		v2.Node().NodePattern(graph.Kinds{graph.StringKind("A")}, cypher.NewParameter("props", map[string]any{
	//			"name": "1234",
	//		})),
	//	)
	//
	//	if preparedQuery, err := builder.Build(); err != nil {
	//		return err
	//	} else {
	//		return driver.CypherQuery(ctx, preparedQuery.Query, preparedQuery.Parameters).Close(ctx)
	//	}
	//}))
}
