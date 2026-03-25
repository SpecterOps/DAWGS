// Copyright 2025 Specter Ops, Inc.
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

package opengraph

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

func setupTestDB(t *testing.T) (graph.Database, context.Context) {
	t.Helper()

	ctx := context.Background()
	connStr := os.Getenv("PG_CONNECTION_STRING")

	if connStr == "" {
		t.Skip("PG_CONNECTION_STRING not set")
	}

	pool, err := pg.NewPool(connStr)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	db, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pool,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "opengraph_test",
			Nodes: graph.Kinds{graph.StringKind("Person"), graph.StringKind("Place"), graph.StringKind("NodeKind1"), graph.StringKind("NodeKind2")},
			Edges: graph.Kinds{graph.StringKind("KNOWS"), graph.StringKind("LIVES_IN"), graph.StringKind("EdgeKind1"), graph.StringKind("EdgeKind2")},
		}},
		DefaultGraph: graph.Graph{Name: "opengraph_test"},
	}

	if err := db.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("Failed to assert schema: %v", err)
	}

	t.Cleanup(func() {
		// Clean up all nodes (cascades to edges)
		_ = db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Nodes().Delete()
		})
		db.Close(ctx)
	})

	return db, ctx
}

func TestLoad(t *testing.T) {
	db, ctx := setupTestDB(t)

	input := `{
		"graph": {
			"nodes": [
				{"id": "alice", "kinds": ["Person"], "properties": {"name": "Alice"}},
				{"id": "bob", "kinds": ["Person"], "properties": {"name": "Bob"}},
				{"id": "nyc", "kinds": ["Place"], "properties": {"name": "New York"}}
			],
			"edges": [
				{"start_id": "alice", "end_id": "bob", "kind": "KNOWS"},
				{"start_id": "alice", "end_id": "nyc", "kind": "LIVES_IN"}
			]
		}
	}`

	if _, err := Load(ctx, db, strings.NewReader(input)); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify nodes exist in DB
	var nodeCount int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var countErr error
		nodeCount, countErr = tx.Nodes().Count()
		return countErr
	})
	if err != nil {
		t.Fatalf("Failed to count nodes: %v", err)
	}
	if nodeCount != 3 {
		t.Fatalf("expected 3 nodes in DB, got %d", nodeCount)
	}

	// Verify edges exist in DB
	var edgeCount int64
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var countErr error
		edgeCount, countErr = tx.Relationships().Count()
		return countErr
	})
	if err != nil {
		t.Fatalf("Failed to count edges: %v", err)
	}
	if edgeCount != 2 {
		t.Fatalf("expected 2 edges in DB, got %d", edgeCount)
	}
}

func TestExport_EmptyDatabase(t *testing.T) {
	db, ctx := setupTestDB(t)

	var buf bytes.Buffer
	if err := Export(ctx, db, &buf); err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	var doc Document
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("Failed to decode exported JSON: %v", err)
	}

	if len(doc.Graph.Nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(doc.Graph.Nodes))
	}

	if len(doc.Graph.Edges) != 0 {
		t.Errorf("expected 0 edges, got %d", len(doc.Graph.Edges))
	}
}

func TestRoundTrip(t *testing.T) {
	db, ctx := setupTestDB(t)

	original := Document{
		Graph: Graph{
			Nodes: []Node{
				{ID: "a", Kinds: []string{"Person"}, Properties: map[string]any{"name": "Alice", "age": float64(30)}},
				{ID: "b", Kinds: []string{"Person"}, Properties: map[string]any{"name": "Bob", "age": float64(25)}},
				{ID: "c", Kinds: []string{"Place"}, Properties: map[string]any{"name": "Chicago"}},
			},
			Edges: []Edge{
				{StartID: "a", EndID: "b", Kind: "KNOWS", Properties: map[string]any{"since": float64(2020)}},
				{StartID: "b", EndID: "c", Kind: "LIVES_IN"},
			},
		},
	}

	// Load
	inputBytes, _ := json.Marshal(original)
	if _, err := Load(ctx, db, bytes.NewReader(inputBytes)); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Export
	var buf bytes.Buffer
	if err := Export(ctx, db, &buf); err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	// Decode exported
	var exported Document
	if err := json.Unmarshal(buf.Bytes(), &exported); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Validate exported document
	if err := Validate(exported); err != nil {
		t.Fatalf("Exported document is not valid: %v", err)
	}

	// Compare node count
	if len(exported.Graph.Nodes) != len(original.Graph.Nodes) {
		t.Fatalf("node count mismatch: got %d, want %d", len(exported.Graph.Nodes), len(original.Graph.Nodes))
	}

	// Compare edge count
	if len(exported.Graph.Edges) != len(original.Graph.Edges) {
		t.Fatalf("edge count mismatch: got %d, want %d", len(exported.Graph.Edges), len(original.Graph.Edges))
	}

	// Build maps for comparison (exported IDs will be database IDs, not original string IDs)
	// So we compare by properties instead
	exportedNodesByName := make(map[string]Node)
	for _, n := range exported.Graph.Nodes {
		if name, ok := n.Properties["name"].(string); ok {
			exportedNodesByName[name] = n
		}
	}

	for _, origNode := range original.Graph.Nodes {
		name := origNode.Properties["name"].(string)
		expNode, ok := exportedNodesByName[name]
		if !ok {
			t.Errorf("missing exported node with name %q", name)
			continue
		}

		// Compare kinds (sort for stable comparison)
		origKinds := make([]string, len(origNode.Kinds))
		copy(origKinds, origNode.Kinds)
		sort.Strings(origKinds)

		expKinds := make([]string, len(expNode.Kinds))
		copy(expKinds, expNode.Kinds)
		sort.Strings(expKinds)

		if strings.Join(origKinds, ",") != strings.Join(expKinds, ",") {
			t.Errorf("node %q kinds mismatch: got %v, want %v", name, expKinds, origKinds)
		}

		// Compare properties (excluding ID-like fields the DB may add)
		for key, origVal := range origNode.Properties {
			expVal, ok := expNode.Properties[key]
			if !ok {
				t.Errorf("node %q missing property %q", name, key)
				continue
			}

			// JSON round-trip means numbers become float64
			origJSON, _ := json.Marshal(origVal)
			expJSON, _ := json.Marshal(expVal)
			if string(origJSON) != string(expJSON) {
				t.Errorf("node %q property %q: got %v, want %v", name, key, expVal, origVal)
			}
		}
	}

	// Compare edges by resolving through node names
	exportedNodeIDToName := make(map[string]string)
	for _, n := range exported.Graph.Nodes {
		if name, ok := n.Properties["name"].(string); ok {
			exportedNodeIDToName[n.ID] = name
		}
	}

	type edgeKey struct {
		startName, endName, kind string
	}

	exportedEdges := make(map[edgeKey]Edge)
	for _, e := range exported.Graph.Edges {
		key := edgeKey{
			startName: exportedNodeIDToName[e.StartID],
			endName:   exportedNodeIDToName[e.EndID],
			kind:      e.Kind,
		}
		exportedEdges[key] = e
	}

	// Map original node IDs to names for comparison
	origIDToName := make(map[string]string)
	for _, n := range original.Graph.Nodes {
		origIDToName[n.ID] = n.Properties["name"].(string)
	}

	for _, origEdge := range original.Graph.Edges {
		key := edgeKey{
			startName: origIDToName[origEdge.StartID],
			endName:   origIDToName[origEdge.EndID],
			kind:      origEdge.Kind,
		}

		if _, ok := exportedEdges[key]; !ok {
			t.Errorf("missing exported edge (%s)-[%s]->(%s)", key.startName, key.kind, key.endName)
		}
	}
}
