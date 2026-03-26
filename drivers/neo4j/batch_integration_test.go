//go:build integration

package neo4j_test

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/assert"
)

const Neo4jConnectionStringEnv = "NEO4J_CONNECTION"

var (
	NodeKind1 = graph.StringKind("NodeKind1")
	NodeKind2 = graph.StringKind("NodeKind2")

	NameProperty = "name"
	HeatProperty = "heat"
	NewProperty  = "new"
)

func prepareNode(index int) *graph.Node {
	return graph.PrepareNode(
		graph.AsProperties(map[string]any{
			NameProperty: "Node " + strconv.Itoa(index),
			HeatProperty: 10 + index,
		}),
		NodeKind1,
	)
}

func TestBatchTransaction_NodeUpdate(t *testing.T) {
	const (
		numNodes = 1_000
	)

	var (
		neo4jConnectionStr = os.Getenv(Neo4jConnectionStringEnv)
		ctx, done          = context.WithCancel(context.Background())
	)

	defer done()

	if neo4jConnectionStr == "" {
		t.Fatalf("No Neo4j connection string specified. Test requires a valid Neo4j connection string present in the %s environment variable.", Neo4jConnectionStringEnv)
	}

	graphDB, err := dawgs.Open(ctx, neo4j.DriverName, dawgs.Config{
		ConnectionString:      neo4jConnectionStr,
		GraphQueryMemoryLimit: 0,
	})
	assert.NoError(t, err)

	// Regsiter a cleanup step to wipe the database
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()

		err := graphDB.WriteTransaction(cleanupCtx, func(tx graph.Transaction) error {
			return tx.Nodes().Filter(query.Kind(query.Node(), NodeKind1)).Delete()
		})

		if err != nil {
			slog.Error("Failed to cleanup after test.", slog.String("err", err.Error()))
		}
	})

	// Insert nodes to batch update afterward
	assert.NoError(t,
		graphDB.BatchOperation(ctx, func(batch graph.Batch) error {
			for idx := range numNodes {
				if err := batch.CreateNode(prepareNode(idx)); err != nil {
					return err
				}
			}

			return nil
		}),
	)

	// Update the nodes in batch
	assert.NoError(t,
		graphDB.BatchOperation(ctx, func(batch graph.Batch) error {
			// Fetch all of the nodes and ensure that they the number of nodes expected matches
			if nodes, err := ops.FetchNodes(batch.Nodes().Filter(query.Kind(query.Node(), NodeKind1))); err != nil {
				return err
			} else {
				assert.Equal(t, numNodes, len(nodes))

				for _, node := range nodes {
					node.AddKinds(NodeKind2)

					node.Properties.Set(NewProperty, true)
					node.Properties.Delete(HeatProperty)
				}

				return batch.UpdateNodes(nodes)
			}
		}),
	)

	// Assert the node update
	assert.NoError(t,
		graphDB.ReadTransaction(ctx, func(tx graph.Transaction) error {
			// Fetch all of the nodes and ensure that they have been updated
			if nodes, err := ops.FetchNodes(tx.Nodes().Filter(query.Kind(query.Node(), NodeKind1))); err != nil {
				return err
			} else {
				assert.Equal(t, numNodes, len(nodes))

				for _, node := range nodes {
					assert.True(t, node.Properties.Exists(HeatProperty))
					assert.Equal(t, true, node.Properties.Get(NewProperty).Any())
				}

				return nil
			}
		}),
	)
}
