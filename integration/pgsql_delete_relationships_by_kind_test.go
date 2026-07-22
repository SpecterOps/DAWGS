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
	"os"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

// relationshipsByKindDeleter mirrors the capability the BloodHound delete path detects on the PostgreSQL driver.
type relationshipsByKindDeleter interface {
	DeleteRelationshipsByKinds(ctx context.Context, kinds graph.Kinds) error
}

// TestPostgreSQLDeleteRelationshipsByKinds verifies the server-side, set-based relationship delete: the listed kinds
// restrict the delete to relationships carrying one of those kinds, nodes are left intact, multiple kinds are unioned,
// undefined kinds are a safe no-op, and an empty request deletes nothing.
func TestPostgreSQLDeleteRelationshipsByKinds(t *testing.T) {
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
		nodeKind  = graph.StringKind("RelByKindNode")
		edgeKindA = graph.StringKind("RelByKindEdgeA")
		edgeKindB = graph.StringKind("RelByKindEdgeB")
		missing   = graph.StringKind("RelByKindMissing")
		db, ctx   = SetupDBWithKinds(t, CleanupGraph, graph.Kinds{nodeKind}, graph.Kinds{edgeKindA, edgeKindB})
	)

	deleter, hasCapability := graph.AsDriver[relationshipsByKindDeleter](db)
	if !hasCapability {
		t.Fatal("PostgreSQL driver does not implement DeleteRelationshipsByKinds")
	}

	// fixture creates three nodes joined by two edgeKindA edges and one edgeKindB edge. Nodes must survive every delete.
	createFixture := func() {
		if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			n0, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			n1, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			n2, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}

			if _, err := tx.CreateRelationshipByIDs(n0.ID, n1.ID, edgeKindA, graph.NewProperties()); err != nil {
				return err
			}
			if _, err := tx.CreateRelationshipByIDs(n1.ID, n2.ID, edgeKindA, graph.NewProperties()); err != nil {
				return err
			}
			_, err = tx.CreateRelationshipByIDs(n0.ID, n2.ID, edgeKindB, graph.NewProperties())
			return err
		}); err != nil {
			t.Fatalf("failed to create delete-relationships-by-kind fixture: %v", err)
		}
	}

	t.Run("deletes relationships of the given kind and leaves nodes", func(t *testing.T) {
		createFixture()

		if err := deleter.DeleteRelationshipsByKinds(ctx, graph.Kinds{edgeKindA}); err != nil {
			t.Fatalf("DeleteRelationshipsByKinds(edgeKindA) failed: %v", err)
		}

		if count := countByCypher(t, ctx, db, "MATCH ()-[r:RelByKindEdgeA]->() RETURN count(r)"); count != 0 {
			t.Fatalf("edgeKindA count: got %d, want 0", count)
		}
		if count := countByCypher(t, ctx, db, "MATCH ()-[r:RelByKindEdgeB]->() RETURN count(r)"); count != 1 {
			t.Fatalf("edgeKindB count: got %d, want 1", count)
		}
		if count := countByCypher(t, ctx, db, "MATCH (n:RelByKindNode) RETURN count(n)"); count != 3 {
			t.Fatalf("node count: got %d, want 3", count)
		}

		ClearGraph(t, db, ctx)
	})

	t.Run("multiple kinds delete every matching relationship", func(t *testing.T) {
		createFixture()

		if err := deleter.DeleteRelationshipsByKinds(ctx, graph.Kinds{edgeKindA, edgeKindB}); err != nil {
			t.Fatalf("DeleteRelationshipsByKinds(edgeKindA, edgeKindB) failed: %v", err)
		}

		if count := countByCypher(t, ctx, db, "MATCH ()-[r]->() RETURN count(r)"); count != 0 {
			t.Fatalf("edge count: got %d, want 0", count)
		}
		if count := countByCypher(t, ctx, db, "MATCH (n:RelByKindNode) RETURN count(n)"); count != 3 {
			t.Fatalf("node count: got %d, want 3", count)
		}

		ClearGraph(t, db, ctx)
	})

	t.Run("undefined and empty kinds are a safe no-op", func(t *testing.T) {
		createFixture()

		if err := deleter.DeleteRelationshipsByKinds(ctx, graph.Kinds{missing}); err != nil {
			t.Fatalf("DeleteRelationshipsByKinds(missing) failed: %v", err)
		}
		if err := deleter.DeleteRelationshipsByKinds(ctx, graph.Kinds{}); err != nil {
			t.Fatalf("DeleteRelationshipsByKinds(empty) failed: %v", err)
		}
		if err := deleter.DeleteRelationshipsByKinds(ctx, nil); err != nil {
			t.Fatalf("DeleteRelationshipsByKinds(nil) failed: %v", err)
		}

		if count := countByCypher(t, ctx, db, "MATCH ()-[r]->() RETURN count(r)"); count != 3 {
			t.Fatalf("edge count after no-op delete: got %d, want 3", count)
		}

		ClearGraph(t, db, ctx)
	})
}
