package main

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/retriever"
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
		t.Fatalf("expected unsupported scheme error")
	}
}

func TestResolveGraphTargets(t *testing.T) {
	targets, err := resolveGraphTargets(context.Background(), nil, pg.DriverName, nil, false)
	if err != nil {
		t.Fatalf("resolve default graph: %v", err)
	}

	if len(targets) != 1 || targets[0].Name != retriever.DefaultGraphName {
		t.Fatalf("unexpected default targets: %+v", targets)
	}

	targets, err = resolveGraphTargets(context.Background(), nil, pg.DriverName, []string{"a", "b"}, false)
	if err != nil {
		t.Fatalf("resolve explicit graphs: %v", err)
	}

	if len(targets) != 2 || targets[0].Name != "a" || targets[1].Name != "b" {
		t.Fatalf("unexpected explicit targets: %+v", targets)
	}

	if _, err := resolveGraphTargets(context.Background(), nil, pg.DriverName, []string{"a", "a"}, false); err == nil {
		t.Fatalf("expected duplicate graph error")
	}

	if _, err := resolveGraphTargets(context.Background(), nil, pg.DriverName, []string{"a"}, true); err == nil {
		t.Fatalf("expected all-graphs and graph conflict")
	}

	if _, err := resolveGraphTargets(context.Background(), nil, neo4j.DriverName, []string{"a", "b"}, false); err == nil {
		t.Fatalf("expected neo4j multi-graph error")
	}

	targets, err = resolveGraphTargets(context.Background(), nil, neo4j.DriverName, nil, true)
	if err != nil {
		t.Fatalf("resolve neo4j all-graphs: %v", err)
	}

	if len(targets) != 1 || targets[0].Name != retriever.DefaultGraphName {
		t.Fatalf("unexpected neo4j all-graphs target: %+v", targets)
	}
}

func TestGraphDirectoryName(t *testing.T) {
	if got := graphDirectoryName("default"); got != "default" {
		t.Fatalf("graphDirectoryName(default) = %q", got)
	}

	if got := graphDirectoryName("graph/name"); got != "graph%2Fname" {
		t.Fatalf("graphDirectoryName(graph/name) = %q", got)
	}

	if got := graphDirectoryName(""); got != retriever.DefaultGraphName {
		t.Fatalf("graphDirectoryName(empty) = %q", got)
	}
}
