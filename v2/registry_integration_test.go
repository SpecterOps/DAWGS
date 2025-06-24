//go:build manual_integration

package v2_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	neo4j_v2 "github.com/specterops/dawgs/drivers/neo4j/v2"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	v2 "github.com/specterops/dawgs/v2"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	ctx := context.Background()

	graphDB, err := v2.Open(ctx, neo4j_v2.DriverName, v2.Config{
		GraphQueryMemoryLimit: size.Gibibyte * 1,
		ConnectionString:      "neo4j://neo4j:neo4jj@localhost:7687",
	})

	require.NoError(t, err)

	cypherQuery, err := v2.Query().Return(v2.Node()).Limit(10).Build()
	require.NoError(t, err)

	require.NoError(t, graphDB.Session(ctx, v2.FetchNodes(cypherQuery, func(node *graph.Node) error {
		slog.Info(fmt.Sprintf("Got result from DB: %v", node))
		return nil
	})))

	require.NoError(t, graphDB.Transaction(ctx, v2.FetchNodes(cypherQuery, func(node *graph.Node) error {
		slog.Info(fmt.Sprintf("Got result from DB: %v", node))
		return nil
	})))
}
