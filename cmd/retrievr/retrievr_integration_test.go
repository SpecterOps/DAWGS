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

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

func TestPostgreSQLDumpLoadRoundTrip(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING not set")
	}
	driverName, err := driverFromConnectionString(connection)
	if err != nil {
		t.Fatalf("infer driver: %v", err)
	}
	if driverName != pg.DriverName {
		t.Skipf("CONNECTION_STRING selects %s, not PostgreSQL", driverName)
	}

	ctx := context.Background()
	db, _, err := openDatabase(ctx, databaseConfig{Connection: connection})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close(ctx)

	graphName := fmt.Sprintf("retrievr_it_%d", time.Now().UTC().UnixNano())
	userKind := graph.StringKind("RetrievrUser")
	systemKind := graph.StringKind("RetrievrSystem")
	adminKind := graph.StringKind("RetrievrAdminTo")
	graphSchema := graph.Graph{
		Name:  graphName,
		Nodes: graph.Kinds{userKind, systemKind},
		Edges: graph.Kinds{adminKind},
	}
	if err := db.AssertSchema(ctx, graph.Schema{
		Graphs:       []graph.Graph{graphSchema},
		DefaultGraph: graphSchema,
	}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	t.Cleanup(func() {
		_ = db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.WithGraph(graph.Graph{Name: graphName}).Nodes().Delete()
		})
	})

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(graph.Graph{Name: graphName})
		alice, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "alice"}), userKind)
		if err != nil {
			return err
		}
		system, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "server"}), systemKind)
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(alice.ID, system.ID, adminKind, graph.AsProperties(map[string]any{"source": "test"}))
		return err
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	dumpDir := t.TempDir()
	dumpResult, err := Dump(ctx, db, driverName, []graphTarget{{Name: graphName}}, dumpOptions{
		OutputDir:   dumpDir,
		Scrub:       scrubNone,
		Compression: compressionGzip,
		ZstdLevel:   defaultZstdLevel,
		ShardSize:   1,
		BatchSize:   1,
	})
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	if dumpResult.NodeCount != 2 || dumpResult.EdgeCount != 1 {
		t.Fatalf("unexpected dump counts: nodes=%d edges=%d", dumpResult.NodeCount, dumpResult.EdgeCount)
	}

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.WithGraph(graph.Graph{Name: graphName}).Nodes().Delete()
	}); err != nil {
		t.Fatalf("clear graph before load: %v", err)
	}

	loadResult, err := Load(ctx, db, driverName, loadOptions{
		InputDir:  dumpDir,
		BatchSize: 1,
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loadResult.NodeCount != 2 || loadResult.EdgeCount != 1 {
		t.Fatalf("unexpected load counts: nodes=%d edges=%d", loadResult.NodeCount, loadResult.EdgeCount)
	}
}
