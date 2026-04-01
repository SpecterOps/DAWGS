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

package integration

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"

	"github.com/specterops/dawgs/drivers/neo4j"
)

var (
	localDatasetFlag = flag.String("local-dataset", "", "name of a local dataset to test (e.g. local/phantom)")
)

// driverFromConnStr returns the dawgs driver name based on the connection string scheme.
func driverFromConnStr(connStr string) (string, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse connection string: %w", err)
	}

	switch u.Scheme {
	case "postgresql", "postgres":
		return pg.DriverName, nil
	case neo4j.DriverName, "neo4j+s", "neo4j+ssc":
		return neo4j.DriverName, nil
	default:
		return "", fmt.Errorf("unknown connection string scheme %q", u.Scheme)
	}
}

// SetupDB opens a database connection for the selected driver, asserts a schema
// derived from the given datasets, and registers cleanup. Returns the database
// and a background context.
func SetupDB(t *testing.T, datasets ...string) (graph.Database, context.Context) {
	t.Helper()

	ctx := context.Background()

	connStr := os.Getenv("CONNECTION_STRING")
	if connStr == "" {
		t.Fatal("CONNECTION_STRING env var is not set")
	}

	driver, err := driverFromConnStr(connStr)
	if err != nil {
		t.Fatalf("Failed to detect driver: %v", err)
	}

	cfg := dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      connStr,
	}

	if driver == pg.DriverName {
		pool, err := pg.NewPool(connStr)
		if err != nil {
			t.Fatalf("Failed to create PG pool: %v", err)
		}
		cfg.Pool = pool
	}

	db, err := dawgs.Open(ctx, driver, cfg)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	nodeKinds, edgeKinds := collectKinds(t, datasets)

	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "integration_test",
			Nodes: nodeKinds,
			Edges: edgeKinds,
		}},
		DefaultGraph: graph.Graph{Name: "integration_test"},
	}

	if err := db.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("Failed to assert schema: %v", err)
	}

	t.Cleanup(func() {
		_ = db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Nodes().Delete()
		})
		db.Close(ctx)
	})

	return db, ctx
}

// collectKinds parses the given datasets and returns the union of all node and edge kinds.
func collectKinds(t *testing.T, datasets []string) (graph.Kinds, graph.Kinds) {
	t.Helper()

	var nodeKinds, edgeKinds graph.Kinds

	for _, name := range datasets {
		f, err := os.Open(datasetPath(name))
		if err != nil {
			t.Fatalf("failed to open dataset %q for kind scanning: %v", name, err)
		}

		doc, err := opengraph.ParseDocument(f)
		f.Close()
		if err != nil {
			t.Fatalf("failed to parse dataset %q: %v", name, err)
		}

		nk, ek := doc.Graph.Kinds()
		nodeKinds = nodeKinds.Add(nk...)
		edgeKinds = edgeKinds.Add(ek...)
	}

	return nodeKinds, edgeKinds
}

// ClearGraph deletes all nodes (and cascading edges) from the database.
func ClearGraph(t *testing.T, db graph.Database, ctx context.Context) {
	t.Helper()

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	}); err != nil {
		t.Fatalf("failed to clear graph: %v", err)
	}
}

// datasetPath returns the filesystem path for a named dataset.
// Names may include subdirectories (e.g. "local/phantom").
func datasetPath(name string) string {
	return "testdata/" + name + ".json"
}

// LoadDataset loads a named JSON dataset from testdata/ and returns the ID mapping.
func LoadDataset(t *testing.T, db graph.Database, ctx context.Context, name string) opengraph.IDMap {
	t.Helper()

	f, err := os.Open(datasetPath(name))
	if err != nil {
		t.Fatalf("failed to open dataset %q: %v", name, err)
	}
	defer f.Close()

	idMap, err := opengraph.Load(ctx, db, f)
	if err != nil {
		t.Fatalf("failed to load dataset %q: %v", name, err)
	}

	return idMap
}

// QueryPaths runs a Cypher query and collects all returned paths.
func QueryPaths(t *testing.T, ctx context.Context, db graph.Database, cypher string) []graph.Path {
	t.Helper()

	var paths []graph.Path

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()

		for result.Next() {
			var p graph.Path
			if err := result.Scan(&p); err != nil {
				return fmt.Errorf("scan error: %w", err)
			}
			paths = append(paths, p)
		}

		return result.Error()
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	return paths
}

// QueryNodeIDs runs a Cypher query that returns nodes and collects their fixture IDs.
// Duplicate nodes are deduplicated.
func QueryNodeIDs(t *testing.T, ctx context.Context, db graph.Database, cypher string, idMap opengraph.IDMap) []string {
	t.Helper()

	rev := make(map[graph.ID]string, len(idMap))
	for fid, dbID := range idMap {
		rev[dbID] = fid
	}

	var ids []string
	seen := make(map[string]bool)

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()

		for result.Next() {
			var n graph.Node
			if err := result.Scan(&n); err != nil {
				return err
			}
			if fid, ok := rev[n.ID]; ok && !seen[fid] {
				ids = append(ids, fid)
				seen[fid] = true
			}
		}
		return result.Error()
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	return ids
}

// AssertIDSet checks that two sets of fixture node IDs match (order-independent).
func AssertIDSet(t *testing.T, got, expected []string) {
	t.Helper()

	sort.Strings(got)
	sort.Strings(expected)

	if len(got) != len(expected) {
		t.Fatalf("ID set length: got %d, want %d\n  got:  %v\n  want: %v", len(got), len(expected), got, expected)
	}

	for i := range got {
		if got[i] != expected[i] {
			t.Fatalf("ID set mismatch at index %d:\n  got:  %v\n  want: %v", i, got, expected)
		}
	}
}

// AssertPaths checks that the returned paths match the expected set of fixture node ID sequences.
// Each expected path is a slice of fixture node IDs, e.g. []string{"a", "b", "d"}.
// Pass nil for expected when no paths should be returned.
func AssertPaths(t *testing.T, paths []graph.Path, idMap opengraph.IDMap, expected [][]string) {
	t.Helper()

	rev := make(map[graph.ID]string, len(idMap))
	for fixtureID, dbID := range idMap {
		rev[dbID] = fixtureID
	}

	toSig := func(ids []string) string { return strings.Join(ids, ",") }

	got := make([]string, len(paths))
	for i, p := range paths {
		ids := make([]string, len(p.Nodes))
		for j, node := range p.Nodes {
			if fid, ok := rev[node.ID]; ok {
				ids[j] = fid
			} else {
				ids[j] = fmt.Sprintf("?(%d)", node.ID)
			}
		}
		got[i] = toSig(ids)
	}
	sort.Strings(got)

	want := make([]string, len(expected))
	for i, e := range expected {
		want[i] = toSig(e)
	}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("path count: got %d, want %d\n  got:  %v\n  want: %v", len(got), len(want), got, want)
	}

	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("path mismatch at index %d:\n  got:  %v\n  want: %v", i, got, want)
		}
	}
}
