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

// nodesByKindDeleter mirrors the capability the BloodHound delete path detects on the PostgreSQL driver.
type nodesByKindDeleter interface {
	DeleteNodesByKinds(ctx context.Context, includeAny graph.Kinds, excludeAny graph.Kinds) error
}

// TestPostgreSQLDeleteNodesByKinds verifies the server-side, set-based node delete: includeAny restricts the delete to
// nodes carrying one of the listed kinds, excludeAny protects nodes carrying one of the listed kinds, undefined include
// kinds are a safe no-op while undefined exclude kinds fail closed, and deleting nodes cascades incident edges.
func TestPostgreSQLDeleteNodesByKinds(t *testing.T) {
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
		kindA    = graph.StringKind("DeleteByKindA")
		kindB    = graph.StringKind("DeleteByKindB")
		edgeKind = graph.StringKind("DeleteByKindEdge")
		missing  = graph.StringKind("DeleteByKindMissing")
		db, ctx  = SetupDBWithKinds(t, CleanupGraph, graph.Kinds{kindA, kindB}, graph.Kinds{edgeKind})
	)

	deleter, hasCapability := graph.AsDriver[nodesByKindDeleter](db)
	if !hasCapability {
		t.Fatal("PostgreSQL driver does not implement DeleteNodesByKinds")
	}

	// fixture creates two kindA nodes, two kindB nodes, and an A->B edge that must cascade when its start is deleted.
	createFixture := func() {
		if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			a0, err := tx.CreateNode(graph.NewProperties(), kindA)
			if err != nil {
				return err
			}
			if _, err := tx.CreateNode(graph.NewProperties(), kindA); err != nil {
				return err
			}

			b0, err := tx.CreateNode(graph.NewProperties(), kindB)
			if err != nil {
				return err
			}
			if _, err := tx.CreateNode(graph.NewProperties(), kindB); err != nil {
				return err
			}

			_, err = tx.CreateRelationshipByIDs(a0.ID, b0.ID, edgeKind, graph.NewProperties())
			return err
		}); err != nil {
			t.Fatalf("failed to create delete-by-kind fixture: %v", err)
		}
	}

	t.Run("includeAny deletes matching kinds and cascades edges", func(t *testing.T) {
		createFixture()

		if err := deleter.DeleteNodesByKinds(ctx, graph.Kinds{kindA}, nil); err != nil {
			t.Fatalf("DeleteNodesByKinds(include kindA) failed: %v", err)
		}

		if count := countByCypher(t, ctx, db, "MATCH (n:DeleteByKindA) RETURN count(n)"); count != 0 {
			t.Fatalf("kindA node count: got %d, want 0", count)
		}
		if count := countByCypher(t, ctx, db, "MATCH (n:DeleteByKindB) RETURN count(n)"); count != 2 {
			t.Fatalf("kindB node count: got %d, want 2", count)
		}
		if count := countByCypher(t, ctx, db, "MATCH ()-[r:DeleteByKindEdge]->() RETURN count(r)"); count != 0 {
			t.Fatalf("edge count after cascade: got %d, want 0", count)
		}

		cleanupAll(t, ctx, deleter)
	})

	t.Run("excludeAny protects matching kinds", func(t *testing.T) {
		createFixture()

		// Delete every node except those carrying kindB.
		if err := deleter.DeleteNodesByKinds(ctx, nil, graph.Kinds{kindB}); err != nil {
			t.Fatalf("DeleteNodesByKinds(exclude kindB) failed: %v", err)
		}

		if count := countByCypher(t, ctx, db, "MATCH (n:DeleteByKindA) RETURN count(n)"); count != 0 {
			t.Fatalf("kindA node count: got %d, want 0", count)
		}
		if count := countByCypher(t, ctx, db, "MATCH (n:DeleteByKindB) RETURN count(n)"); count != 2 {
			t.Fatalf("kindB node count: got %d, want 2", count)
		}

		cleanupAll(t, ctx, deleter)
	})

	t.Run("undefined include kinds are a safe no-op", func(t *testing.T) {
		createFixture()

		if err := deleter.DeleteNodesByKinds(ctx, graph.Kinds{missing}, nil); err != nil {
			t.Fatalf("DeleteNodesByKinds(include missing) failed: %v", err)
		}

		if count := countByCypher(t, ctx, db, "MATCH (n) RETURN count(n)"); count != 4 {
			t.Fatalf("node count after no-op delete: got %d, want 4", count)
		}

		cleanupAll(t, ctx, deleter)
	})

	t.Run("undefined exclude kinds fail closed and delete nothing", func(t *testing.T) {
		createFixture()

		// An unresolved exclusion would otherwise collapse to an unguarded delete; the driver must refuse instead.
		if err := deleter.DeleteNodesByKinds(ctx, nil, graph.Kinds{missing}); err == nil {
			t.Fatal("DeleteNodesByKinds(exclude missing) succeeded, want error")
		}

		if count := countByCypher(t, ctx, db, "MATCH (n) RETURN count(n)"); count != 4 {
			t.Fatalf("node count after failed delete: got %d, want 4", count)
		}

		cleanupAll(t, ctx, deleter)
	})
}

// cleanupAll removes every node between subtests so each starts from an empty graph.
func cleanupAll(t *testing.T, ctx context.Context, deleter nodesByKindDeleter) {
	t.Helper()

	if err := deleter.DeleteNodesByKinds(ctx, nil, nil); err != nil {
		t.Fatalf("failed to clean up nodes between subtests: %v", err)
	}
}
