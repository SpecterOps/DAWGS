//go:build manual_integration

package dawgs_test

import (
	"context"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/database/pg"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	ctx := context.Background()

	graphDB, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte * 1,
		ConnectionString:      "postgresql://postgres:postgres@localhost:5432/bhe",
	})

	require.NoError(t, err)

	require.NoError(t, graphDB.AssertSchema(ctx, database.NewSchema(
		"default",
		database.Graph{
			Name:  "default",
			Nodes: graph.Kinds{graph.StringKind("Node")},
			Edges: graph.Kinds{graph.StringKind("Edge")},
			NodeIndexes: []database.Index{{
				Name:  "node_name_index",
				Field: "name",
				Type:  database.IndexTypeTextSearch,
			}},
		})))

	require.NoError(t, graphDB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		_, err := driver.CreateNode(ctx, graph.PrepareNode(graph.AsProperties(map[string]any{
			"name": "THAT NODE",
		}), graph.StringKind("Node")))

		return err
	}))

	require.NoError(t, graphDB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		myQuery := query.New().Where(
			query.Node().Property("a").Equals(1234),
		).OrderBy(
			query.Node().Property("my_order"),
		)

		if builtQuery, err := myQuery.Build(); err != nil {
			return err
		} else {
			var (
				node   graph.Node
				result = driver.Exec(ctx, builtQuery.Query, builtQuery.Parameters)
			)

			defer result.Close(ctx)

			if result.HasNext(ctx) {
				if err := result.Scan(&node); err != nil {
					return err
				} else {
					require.Equal(t, "THAT NODE", node.Properties.GetOrDefault("name", "").Any())
				}
			} else {
				t.Fatal("no node found")
			}
		}

		return nil
	}))
}
