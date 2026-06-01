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
	"errors"
	"os"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

func TestPostgreSQLCountStoreFastPathRequiresRelationshipEndpoints(t *testing.T) {
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
		nodeKind = graph.StringKind("CountFastPathNode")
		edgeKind = graph.StringKind("CountFastPathEdge")
		db, ctx  = SetupDBWithKinds(t, 0, graph.Kinds{nodeKind}, graph.Kinds{edgeKind})
	)

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		start, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}

		if _, err := tx.CreateRelationshipByIDs(start.ID, 0, edgeKind, graph.NewProperties()); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to create dangling relationship fixture: %v", err)
	}

	var count int64
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH ()-[r:CountFastPathEdge]->() RETURN count(r)", nil)
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
		t.Fatalf("query failed: %v", err)
	}

	if count != 0 {
		t.Fatalf("relationship count: got %d, want 0", count)
	}
}
