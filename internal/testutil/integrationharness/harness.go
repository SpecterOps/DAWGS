package integrationharness

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

const ConnectionStringEnv = "CONNECTION_STRING"

var (
	LocalDatasetFlag = flag.String("local-dataset", "", "name of a local dataset to test (e.g. local/phantom)")

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
}

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

func Open(t *testing.T, opts Options) *Session {
	t.Helper()

	ctx := context.Background()
	connEnv := opts.ConnectionStringEnvVar
	if connEnv == "" {
		connEnv = ConnectionStringEnv
	}

	connStr := os.Getenv(connEnv)
	if connStr == "" {
		if opts.SkipIfNoConnection {
			t.Skipf("%s env var is not set", connEnv)
		}
		t.Fatalf("%s env var is not set", connEnv)
	}

	driver, err := DriverFromConnectionString(connStr)
	if err != nil {
		t.Fatalf("failed to detect driver: %v", err)
	}

	if opts.RequireDriver != "" && driver != opts.RequireDriver {
		if opts.SkipIfDriverMismatch {
			t.Skipf("%s is not a %s connection string", connEnv, opts.RequireDriver)
		}
		t.Fatalf("%s is not a %s connection string", connEnv, opts.RequireDriver)
	}

	cfg := dawgs.Config{
		ConnectionString:      connStr,
		GraphQueryMemoryLimit: opts.graphQueryMemoryLimit(),
	}

	session := &Session{
		ConnectionString: connStr,
		Driver:           driver,
		Ctx:              ctx,
	}

	if driver == pg.DriverName {
		pool, err := pg.NewPool(drivers.DatabaseConfiguration{Connection: connStr})
		if err != nil {
			t.Fatalf("failed to create PG pool: %v", err)
		}
		cfg.Pool = pool
		session.PGPool = pool
	}

	db, err := dawgs.Open(ctx, driver, cfg)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	session.DB = db

	schema := opts.Schema
	if schema == nil {
		schema = buildSchema(t, opts)
	}
	if schema != nil {
		if err := db.AssertSchema(ctx, *schema); err != nil {
			t.Fatalf("failed to assert schema: %v", err)
		}
	}

	t.Cleanup(func() {
		if opts.CleanupMode != CloseOnly {
			_ = db.WriteTransaction(ctx, func(tx graph.Transaction) error {
				return tx.Nodes().Delete()
			})
		}
		db.Close(ctx)
	})

	return session
}

func (s *Session) ClearGraph(t *testing.T) {
	t.Helper()

	if err := s.DB.WriteTransaction(s.Ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	}); err != nil {
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

func buildSchema(t *testing.T, opts Options) *graph.Schema {
	t.Helper()

	nodeKinds, edgeKinds := collectKinds(t, opts.Datasets, opts.datasetPath())
	nodeKinds = nodeKinds.Add(opts.ExtraNodeKinds...)
	edgeKinds = edgeKinds.Add(opts.ExtraEdgeKinds...)

	if len(nodeKinds) == 0 && len(edgeKinds) == 0 {
		return nil
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
	}
}

func collectKinds(t *testing.T, datasets []string, datasetPath func(name string) string) (graph.Kinds, graph.Kinds) {
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
