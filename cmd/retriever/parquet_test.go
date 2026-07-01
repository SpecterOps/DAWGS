package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestParquetFilePath(t *testing.T) {
	nodePath, err := parquetFilePath("graph/name", phaseNodes)
	if err != nil {
		t.Fatalf("node parquet path: %v", err)
	}
	if nodePath != "graphs/graph%2Fname/nodes.parquet" {
		t.Fatalf("unexpected node parquet path %q", nodePath)
	}

	edgePath, err := parquetFilePath("default", phaseEdges)
	if err != nil {
		t.Fatalf("edge parquet path: %v", err)
	}
	if edgePath != "graphs/default/edges.parquet" {
		t.Fatalf("unexpected edge parquet path %q", edgePath)
	}

	if _, err := parquetFilePath("default", phase("bad")); err == nil {
		t.Fatalf("expected unsupported phase error")
	}
}

func TestParquetStatementsDecorateAnalyticsColumns(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "quote'path")
	value := newValidTestManifest(1)
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 2,
		EdgeCount: 1,
		Files: []fileManifest{
			{
				Phase: phaseNodes,
				Path:  "graphs/default/nodes.jsonl.zst",
				Count: 2,
			},
			{
				Phase: phaseEdges,
				Path:  "graphs/default/edges.jsonl.zst",
				Count: 1,
			},
		},
	}}

	statements, err := parquetStatements(outputDir, value)
	if err != nil {
		t.Fatalf("parquet statements: %v", err)
	}
	if len(statements) != 2 {
		t.Fatalf("statement count = %d, want 2", len(statements))
	}
	if statements[0].OutputPath != "graphs/default/nodes.parquet" {
		t.Fatalf("unexpected node parquet output path: %+v", statements[0])
	}
	if statements[1].OutputPath != "graphs/default/edges.parquet" {
		t.Fatalf("unexpected edge parquet output path: %+v", statements[1])
	}

	nodeSQL := statements[0].SQL
	for _, expected := range []string{
		"read_ndjson",
		"columns = {id: 'VARCHAR', kinds: 'VARCHAR[]', properties: 'JSON'}",
		"json_extract_string(properties, '$.objectid') AS objectid",
		"FORMAT parquet, COMPRESSION zstd",
		"quote''path",
	} {
		if !strings.Contains(nodeSQL, expected) {
			t.Fatalf("node SQL missing %q:\n%s", expected, nodeSQL)
		}
	}

	edgeSQL := statements[1].SQL
	for _, expected := range []string{
		"read_ndjson",
		"columns = {start_id: 'VARCHAR', end_id: 'VARCHAR', kind: 'VARCHAR', properties: 'JSON'}",
		"start_nodes.objectid || '_' || coalesce(e.kind, '') || '_' || end_nodes.objectid AS id",
		"start_nodes.objectid AS start_objectid",
		"end_nodes.objectid AS end_objectid",
		"FORMAT parquet, COMPRESSION zstd",
	} {
		if !strings.Contains(edgeSQL, expected) {
			t.Fatalf("edge SQL missing %q:\n%s", expected, edgeSQL)
		}
	}
}

func TestParquetStatementsCreateEmptyPhaseOutputs(t *testing.T) {
	value := newValidTestManifest(1)
	value.Graphs = []graphManifest{{
		Name:      "empty",
		NodeCount: 0,
		EdgeCount: 0,
	}}

	statements, err := parquetStatements(t.TempDir(), value)
	if err != nil {
		t.Fatalf("parquet statements: %v", err)
	}
	if len(statements) != 2 {
		t.Fatalf("statement count = %d, want 2", len(statements))
	}
	for _, statement := range statements {
		if !strings.Contains(statement.SQL, "WHERE false") {
			t.Fatalf("expected empty phase SQL for %s:\n%s", statement.OutputPath, statement.SQL)
		}
	}
}

func TestWriteParquetCollectionSynthetic(t *testing.T) {
	dir := t.TempDir()
	options := dumpOptions{
		OutputDir:   dir,
		Compression: compressionZstd,
		ZstdLevel:   defaultZstdLevel,
	}
	nodeEntry, err := writeNodeFragment(dir, "default", options, []fragmentNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"objectid": "node-1", "name": "alice"},
	}, {
		ID:         "2",
		Kinds:      []string{"Computer"},
		Properties: map[string]any{"objectid": "node-2", "name": "server"},
	}}, nil)
	if err != nil {
		t.Fatalf("write node JSONL: %v", err)
	}
	edgeEntry, err := writeEdgeFragment(dir, "default", options, []fragmentEdge{{
		StartID:    "1",
		EndID:      "2",
		Kind:       "AdminTo",
		Properties: map[string]any{"source": "test"},
	}}, nil)
	if err != nil {
		t.Fatalf("write edge JSONL: %v", err)
	}

	value := newValidTestManifest(1)
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 2,
		EdgeCount: 1,
		Files:     []fileManifest{nodeEntry, edgeEntry},
	}}

	if err := writeParquetCollection(context.Background(), dir, value); err != nil {
		t.Fatalf("write parquet collection: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	nodePath, err := parquetFilePath("default", phaseNodes)
	if err != nil {
		t.Fatalf("node parquet path: %v", err)
	}
	var (
		nodeCount int
		objectID  string
	)
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*), max(objectid) FROM read_parquet(%s)", sqlStringLiteral(absoluteCollectionPath(dir, nodePath)))).Scan(&nodeCount, &objectID); err != nil {
		t.Fatalf("query nodes parquet: %v", err)
	}
	if nodeCount != 2 || objectID != "node-2" {
		t.Fatalf("unexpected nodes parquet values: count=%d objectid=%q", nodeCount, objectID)
	}

	edgePath, err := parquetFilePath("default", phaseEdges)
	if err != nil {
		t.Fatalf("edge parquet path: %v", err)
	}
	var (
		edgeID        string
		startObjectID string
		endObjectID   string
	)
	if err := db.QueryRow(fmt.Sprintf("SELECT id, start_objectid, end_objectid FROM read_parquet(%s)", sqlStringLiteral(absoluteCollectionPath(dir, edgePath)))).Scan(&edgeID, &startObjectID, &endObjectID); err != nil {
		t.Fatalf("query edges parquet: %v", err)
	}
	if edgeID != "node-1_AdminTo_node-2" || startObjectID != "node-1" || endObjectID != "node-2" {
		t.Fatalf("unexpected edges parquet values: id=%q start=%q end=%q", edgeID, startObjectID, endObjectID)
	}
}
