package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

func TestDriverFromConnectionString(t *testing.T) {
	cases := map[string]string{
		"postgres://user:pass@example/db":   pg.DriverName,
		"postgresql://user:pass@example/db": pg.DriverName,
		"neo4j://user:pass@example":         neo4j.DriverName,
		"neo4j+s://user:pass@example":       neo4j.DriverName,
		"neo4j+ssc://user:pass@example":     neo4j.DriverName,
	}
	for connection, expected := range cases {
		actual, err := driverFromConnectionString(connection)
		if err != nil {
			t.Fatalf("driverFromConnectionString(%q): %v", connection, err)
		}
		if actual != expected {
			t.Fatalf("driverFromConnectionString(%q) = %q, want %q", connection, actual, expected)
		}
	}
	if _, err := driverFromConnectionString("mysql://example"); err == nil {
		t.Fatal("expected unsupported scheme error")
	}
}

func TestResolveGraphNamesPreservesOrderAndRejectsDuplicates(t *testing.T) {
	names, err := resolveGraphNames(context.Background(), nil, pg.DriverName, nil, false)
	if err != nil {
		t.Fatalf("resolve default graph: %v", err)
	}
	if !reflect.DeepEqual(names, []string{defaultGraphName}) {
		t.Fatalf("default graph names = %v", names)
	}

	names, err = resolveGraphNames(context.Background(), nil, pg.DriverName, []string{"b", "a"}, false)
	if err != nil {
		t.Fatalf("resolve explicit graphs: %v", err)
	}
	if !reflect.DeepEqual(names, []string{"b", "a"}) {
		t.Fatalf("explicit graph names = %v", names)
	}
	if _, err := resolveGraphNames(context.Background(), nil, pg.DriverName, []string{"a", "a"}, false); err == nil {
		t.Fatal("expected duplicate graph error")
	}
	if _, err := resolveGraphNames(context.Background(), nil, pg.DriverName, []string{"a"}, true); err == nil {
		t.Fatal("expected all-graphs and graph conflict")
	}
	if _, err := resolveGraphNames(context.Background(), nil, neo4j.DriverName, []string{"a", "b"}, false); err == nil {
		t.Fatal("expected neo4j multi-graph error")
	}

	names, err = resolveGraphNames(context.Background(), nil, neo4j.DriverName, nil, true)
	if err != nil {
		t.Fatalf("resolve neo4j all-graphs: %v", err)
	}
	if !reflect.DeepEqual(names, []string{defaultGraphName}) {
		t.Fatalf("neo4j all-graphs names = %v", names)
	}
}

func TestOpenDatabaseJoinsSetDefaultGraphAndCloseFailures(t *testing.T) {
	setFailure := errors.New("set default graph failed")
	closeFailure := errors.New("close failed")
	ctx, cancel := context.WithCancel(context.Background())
	database := &defaultGraphFailureDatabase{
		closingTestDatabase: closingTestDatabase{closeErr: closeFailure},
		setErr:              setFailure,
		cancel:              cancel,
	}

	_, _, err := openDatabaseWith(ctx, databaseConfig{
		Driver:     neo4j.DriverName,
		Connection: "neo4j://example",
		Graph:      "asset",
	}, databaseOpenOperations{
		open: func(context.Context, string, dawgs.Config) (graph.Database, error) {
			return database, nil
		},
	})
	if !errors.Is(err, setFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("open database error = %v, want joined set-default and close failures", err)
	}
	if len(database.closeContextErrors) != 1 || database.closeContextErrors[0] != nil {
		t.Fatalf("close context errors = %v, want one non-canceled cleanup context", database.closeContextErrors)
	}
}

type defaultGraphFailureDatabase struct {
	closingTestDatabase
	setErr error
	cancel context.CancelFunc
}

func (s *defaultGraphFailureDatabase) SetDefaultGraph(context.Context, graph.Graph) error {
	s.cancel()
	return s.setErr
}
