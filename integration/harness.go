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
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"

	// Register drivers
	_ "github.com/specterops/dawgs/drivers/neo4j"
)

var (
	driverFlag  = flag.String("driver", "pg", "database driver to test against (pg, neo4j)")
	connStrFlag = flag.String("connection", "", "database connection string (overrides PG_CONNECTION_STRING env var)")
)

// SetupDB opens a database connection for the selected driver, asserts schema,
// and registers cleanup. Returns the database and a background context.
func SetupDB(t *testing.T) (graph.Database, context.Context) {
	t.Helper()

	ctx := context.Background()

	connStr := *connStrFlag
	if connStr == "" {
		connStr = os.Getenv("PG_CONNECTION_STRING")
	}
	if connStr == "" {
		t.Skip("no connection string: set -connection flag or PG_CONNECTION_STRING env var")
	}

	cfg := dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      connStr,
	}

	// PG needs a pool with composite type registration
	if *driverFlag == pg.DriverName {
		pool, err := pg.NewPool(connStr)
		if err != nil {
			t.Fatalf("Failed to create PG pool: %v", err)
		}
		cfg.Pool = pool
	}

	db, err := dawgs.Open(ctx, *driverFlag, cfg)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name: "integration_test",
			Nodes: graph.Kinds{
				graph.StringKind("NodeKind1"),
				graph.StringKind("NodeKind2"),
			},
			Edges: graph.Kinds{
				graph.StringKind("EdgeKind1"),
				graph.StringKind("EdgeKind2"),
			},
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

// LoadDataset loads a named JSON dataset from testdata/ and returns the ID mapping.
func LoadDataset(t *testing.T, db graph.Database, ctx context.Context, name string) opengraph.IDMap {
	t.Helper()

	f, err := os.Open("testdata/" + name + ".json")
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

// ASPQuery builds an allShortestPaths Cypher query using literal database IDs.
func ASPQuery(idMap opengraph.IDMap, start, end string) string {
	return fmt.Sprintf(
		"MATCH p = allShortestPaths((s)-[:EdgeKind1*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
		idMap[start], idMap[end],
	)
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
