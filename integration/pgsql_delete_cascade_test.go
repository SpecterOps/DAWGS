// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package integration

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

// TestPostgreSQLStatementLevelDeleteTriggerCascadesEdges verifies that the statement-level delete_node_edges trigger
// cascades edge deletion when multiple nodes are removed in a single delete statement. The batch delete path issues one
// `delete from node where id = any($1)` statement, so the trigger fires once with a transition table containing every
// deleted node; edges incident to any deleted node must be removed while edges between survivors remain.
func TestPostgreSQLStatementLevelDeleteTriggerCascadesEdges(t *testing.T) {
	connStr := os.Getenv("CONNECTION_STRING")
	if connStr == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}

	driver, err := DriverFromConnectionString(connStr)
	if err != nil {
		t.Fatalf("failed to detect driver: %v", err)
	}
	if driver != pg.DriverName {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	var (
		nodeKind   = graph.StringKind("CascadeDeleteNode")
		edgeKind   = graph.StringKind("CascadeDeleteEdge")
		db, ctx    = SetupDBWithKinds(t, CleanupGraph, graph.Kinds{nodeKind}, graph.Kinds{edgeKind})
		deletedIDs []graph.ID
	)

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var nodes [5]*graph.Node

		for i := range nodes {
			node, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			nodes[i] = node
		}

		// nodes[0..2] are deleted; nodes[3..4] survive.
		deletedIDs = []graph.ID{nodes[0].ID, nodes[1].ID, nodes[2].ID}

		edges := [][2]graph.ID{
			{nodes[0].ID, nodes[1].ID}, // both deleted -> removed
			{nodes[1].ID, nodes[3].ID}, // start deleted -> removed
			{nodes[4].ID, nodes[2].ID}, // end deleted -> removed
			{nodes[3].ID, nodes[4].ID}, // neither deleted -> survives
		}

		for _, edge := range edges {
			if _, err := tx.CreateRelationshipByIDs(edge[0], edge[1], edgeKind, graph.NewProperties()); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to create cascade-delete fixture: %v", err)
	}

	// Delete the targeted nodes in a single batch so the deletions flush as one `delete from node where id = any($1)`
	// statement, exercising the statement-level trigger with a multi-row transition table.
	if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
		for _, id := range deletedIDs {
			if err := batch.DeleteNode(id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to batch-delete nodes: %v", err)
	}

	if nodeCount := countByCypher(t, ctx, db, "MATCH (n:CascadeDeleteNode) RETURN count(n)"); nodeCount != 2 {
		t.Fatalf("surviving node count: got %d, want 2", nodeCount)
	}

	if edgeCount := countByCypher(t, ctx, db, "MATCH (:CascadeDeleteNode)-[r:CascadeDeleteEdge]->(:CascadeDeleteNode) RETURN count(r)"); edgeCount != 1 {
		t.Fatalf("surviving edge count: got %d, want 1", edgeCount)
	}
}

// countByCypher executes a Cypher query that returns a single count and returns it.
func countByCypher(t *testing.T, ctx context.Context, db graph.Database, cypher string) int64 {
	t.Helper()

	var count int64
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()

		if !result.Next() {
			if err := result.Error(); err != nil {
				return err
			}
			return errors.New("expected count row")
		}

		if err := result.Scan(&count); err != nil {
			return err
		}

		if result.Next() {
			return errors.New("expected one count row")
		}

		return result.Error()
	}); err != nil {
		t.Fatalf("count query failed: %v", err)
	}

	return count
}
