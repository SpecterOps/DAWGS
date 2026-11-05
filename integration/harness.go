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
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

const ConnectionStringEnv = "CONNECTION_STRING"

var (
	localDatasetFlag   = flag.String("local-dataset", "", "name of a local dataset to test (e.g. local/phantom)")
	errFixtureRollback = errors.New("fixture rollback")
)

type CleanupMode int

const (
	CleanupGraph CleanupMode = iota
	CloseOnly
)

type Options struct {
	RequireDriver          string
	SkipIfNoConnection     bool
	SkipIfDriverMismatch   bool
	ConnectionStringEnvVar string
	GraphName              string
	GraphQueryMemoryLimit  size.Size
	Schema                 *graph.Schema
	ExtraNodeKinds         graph.Kinds
	ExtraEdgeKinds         graph.Kinds
	Datasets               []string
	DatasetPath            func(name string) string
	CleanupMode            CleanupMode
}

type Session struct {
	ConnectionString string
	Driver           string
	DB               graph.Database
	PGPool           *pgxpool.Pool
	Ctx              context.Context
	cleanupMode      CleanupMode
}

var (
	errConnectionStringMissing = errors.New("connection string is not set")
	errDriverMismatch          = errors.New("connection string driver mismatch")
)

// DriverFromConnStr returns the dawgs driver name based on the connection string scheme.
func DriverFromConnectionString(connStr string) (string, error) {
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

func OpenSession(opts Options) (*Session, error) {
	ctx := context.Background()
	connEnv := opts.ConnectionStringEnvVar
	if connEnv == "" {
		connEnv = ConnectionStringEnv
	}

	connStr := os.Getenv(connEnv)
	if connStr == "" {
		return nil, fmt.Errorf("%w: %s env var is not set", errConnectionStringMissing, connEnv)
	}

	driver, err := DriverFromConnectionString(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to detect driver: %w", err)
	}

	if opts.RequireDriver != "" && driver != opts.RequireDriver {
		return nil, fmt.Errorf("%w: %s is not a %s connection string", errDriverMismatch, connEnv, opts.RequireDriver)
	}

	cfg := dawgs.Config{
		ConnectionString:      connStr,
		GraphQueryMemoryLimit: opts.graphQueryMemoryLimit(),
	}

	session := &Session{
		ConnectionString: connStr,
		Driver:           driver,
		Ctx:              ctx,
		cleanupMode:      opts.CleanupMode,
	}

	if driver == pg.DriverName {
		poolCfg, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse pool configuration: %w", err)
		}
		pool, err := pg.NewPool(poolCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create PG pool: %w", err)
		}
		cfg.Pool = pool
		session.PGPool = pool
	}

	db, err := dawgs.Open(ctx, driver, cfg)
	if err != nil {
		if session.PGPool != nil {
			session.PGPool.Close()
		}
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	session.DB = db

	schema := opts.Schema
	if schema == nil {
		schema, err = buildSchema(opts)
		if err != nil {
			_ = db.Close(ctx)
			return nil, fmt.Errorf("failed to build schema: %w", err)
		}
	}
	if schema != nil {
		if err := db.AssertSchema(ctx, *schema); err != nil {
			_ = db.Close(ctx)
			return nil, fmt.Errorf("failed to assert schema: %w", err)
		}
	}

	return session, nil
}

func Open(t *testing.T, opts Options) *Session {
	t.Helper()

	session, err := OpenSession(opts)
	if err != nil {
		if errors.Is(err, errConnectionStringMissing) && opts.SkipIfNoConnection {
			t.Skipf("%v", err)
		}
		if errors.Is(err, errDriverMismatch) && opts.SkipIfDriverMismatch {
			t.Skipf("%v", err)
		}
		t.Fatalf("failed to open integration database: %v", err)
	}

	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Errorf("failed to clean up integration database: %v", err)
		}
	})

	return session
}

// ResetGraph removes all nodes and cascading edges from the session database.
func (s *Session) ResetGraph() error {
	return s.DB.WriteTransaction(s.Ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	})
}

// RegisterGraphCleanup resets the graph at the end of the supplied test scope.
// It is intended for subtests sharing a session opened by their parent test.
func (s *Session) RegisterGraphCleanup(t testing.TB) {
	t.Helper()
	t.Cleanup(func() {
		if err := s.ResetGraph(); err != nil {
			t.Errorf("failed to clear graph: %v", err)
		}
	})
}

// Close applies the configured cleanup mode and closes the database connection.
func (s *Session) Close() error {
	var cleanupErr error
	if s.cleanupMode != CloseOnly {
		cleanupErr = s.ResetGraph()
	}
	if err := s.DB.Close(s.Ctx); err != nil && cleanupErr == nil {
		cleanupErr = err
	}
	return cleanupErr
}

func (s *Session) ClearGraph(t *testing.T) {
	t.Helper()

	if err := s.ResetGraph(); err != nil {
		t.Fatalf("failed to clear graph: %v", err)
	}
}

func (s *Session) LoadDataset(t *testing.T, path string) opengraph.IDMap {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open dataset %q: %v", path, err)
	}
	defer f.Close()

	idMap, err := opengraph.Load(s.Ctx, s.DB, f)
	if err != nil {
		t.Fatalf("failed to load dataset %q: %v", path, err)
	}

	return idMap
}

func (s *Session) WithRollbackFixture(t *testing.T, fixture *opengraph.Graph, clearGraph bool, delegate func(tx graph.Transaction, idMap opengraph.IDMap) error) error {
	t.Helper()

	return s.withRollback(t, func(tx graph.Transaction) error {
		if clearGraph {
			if err := tx.Nodes().Delete(); err != nil {
				return fmt.Errorf("clearing graph before fixture: %w", err)
			}
		}

		idMap, err := opengraph.WriteGraphTx(tx, fixture)
		if err != nil {
			return fmt.Errorf("creating fixture: %w", err)
		}

		if delegate != nil {
			return delegate(tx, idMap)
		}

		return nil
	})
}

func (s *Session) WithRollback(t *testing.T, delegate func(tx graph.Transaction) error) error {
	t.Helper()
	return s.withRollback(t, delegate)
}

func (s *Session) withRollback(t *testing.T, delegate func(tx graph.Transaction) error) error {
	t.Helper()

	err := s.DB.WriteTransaction(s.Ctx, func(tx graph.Transaction) error {
		if err := delegate(tx); err != nil {
			return err
		}

		return errFixtureRollback
	})
	if errors.Is(err, errFixtureRollback) {
		return nil
	}

	return err
}

func buildSchema(opts Options) (*graph.Schema, error) {
	nodeKinds, edgeKinds, err := collectKinds(opts.Datasets, opts.datasetPath())
	if err != nil {
		return nil, err
	}
	nodeKinds = nodeKinds.Add(opts.ExtraNodeKinds...)
	edgeKinds = edgeKinds.Add(opts.ExtraEdgeKinds...)

	if len(nodeKinds) == 0 && len(edgeKinds) == 0 {
		return nil, nil
	}

	graphName := opts.GraphName
	if graphName == "" {
		graphName = "integration_test"
	}

	return &graph.Schema{
		Graphs: []graph.Graph{{
			Name:  graphName,
			Nodes: nodeKinds,
			Edges: edgeKinds,
		}},
		DefaultGraph: graph.Graph{Name: graphName},
	}, nil
}

// collectKinds parses the given datasets and returns the union of all node and edge kinds.
func collectKinds(datasets []string, datasetPath func(name string) string) (graph.Kinds, graph.Kinds, error) {
	var nodeKinds, edgeKinds graph.Kinds

	for _, name := range datasets {
		f, err := os.Open(datasetPath(name))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open dataset %q for kind scanning: %w", name, err)
		}

		doc, err := opengraph.ParseDocument(f)
		f.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse dataset %q: %w", name, err)
		}

		nk, ek := doc.Graph.Kinds()
		nodeKinds = nodeKinds.Add(nk...)
		edgeKinds = edgeKinds.Add(ek...)
	}

	return nodeKinds, edgeKinds, nil
}

func (s *Options) datasetPath() func(name string) string {
	if s.DatasetPath != nil {
		return s.DatasetPath
	}

	return func(name string) string {
		return "testdata/" + name + ".json"
	}
}

func (s Options) graphQueryMemoryLimit() size.Size {
	if s.GraphQueryMemoryLimit == 0 {
		return size.Gibibyte
	}

	return s.GraphQueryMemoryLimit
}

// SetupDB opens a database connection for the selected driver, asserts a schema
// derived from the given datasets, and registers cleanup. Returns the database
// and a background context.
func SetupDB(t *testing.T, cleanupMode CleanupMode, datasets ...string) (graph.Database, context.Context) {
	t.Helper()

	session := Open(t, Options{
		CleanupMode:    cleanupMode,
		Datasets:       datasets,
		ExtraNodeKinds: nil,
		ExtraEdgeKinds: nil,
		DatasetPath:    datasetPath,
	})

	return session.DB, session.Ctx
}

// SetupDBWithKinds opens a database connection like SetupDB, then extends the
// asserted schema with additional node and edge kinds.
func SetupDBWithKinds(t *testing.T, cleanupMode CleanupMode, extraNodeKinds, extraEdgeKinds graph.Kinds, datasets ...string) (graph.Database, context.Context) {
	t.Helper()

	session := Open(t, Options{
		CleanupMode:    cleanupMode,
		Datasets:       datasets,
		ExtraNodeKinds: extraNodeKinds,
		ExtraEdgeKinds: extraEdgeKinds,
		DatasetPath:    datasetPath,
	})

	return session.DB, session.Ctx
}

// SetupDBWithKindsNoGraphCleanup opens a database connection like SetupDBWithKinds
// but only closes the connection during cleanup. Use this for rollback-only tests
// that must not clear a shared database.
func SetupDBWithKindsNoGraphCleanup(t *testing.T, extraNodeKinds, extraEdgeKinds graph.Kinds, datasets ...string) (graph.Database, context.Context) {
	t.Helper()

	session := Open(t, Options{
		CleanupMode:    CloseOnly,
		Datasets:       datasets,
		ExtraNodeKinds: extraNodeKinds,
		ExtraEdgeKinds: extraEdgeKinds,
		DatasetPath:    datasetPath,
	})

	return session.DB, session.Ctx
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

	var (
		paths []graph.Path
		err   = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
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
	)

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

	var (
		ids  []string
		seen = make(map[string]bool)
		err  = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
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
	)

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

	var (
		toSig = func(ids []string) string { return strings.Join(ids, ",") }
		got   = make([]string, len(paths))
	)

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
