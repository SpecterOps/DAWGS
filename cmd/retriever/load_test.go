package main

import (
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/graph"
)

func TestSchemaAssertionsFromManifest(t *testing.T) {
	value := newValidTestManifest(2)
	value.Graphs = []graphManifest{
		{
			Name: "a",
		},
		{
			Name: "b",
		},
	}
	value.Schema.Graphs = []graphSchemaMetadata{
		{
			Name:      "a",
			NodeKinds: []string{"User"},
			EdgeKinds: []string{"AdminTo"},
		},
		{
			Name:      "b",
			NodeKinds: []string{"Computer"},
			EdgeKinds: []string{"MemberOf"},
		},
	}

	assertions, err := schemaAssertionsFromManifest(value)
	if err != nil {
		t.Fatalf("schema assertions: %v", err)
	}
	if len(assertions) != 2 {
		t.Fatalf("assertion count = %d", len(assertions))
	}
	if assertions[0].Schema.DefaultGraph.Name != "a" || assertions[0].Schema.Graphs[0].Nodes[0].String() != "User" {
		t.Fatalf("unexpected first schema assertion: %+v", assertions[0])
	}

	value.Schema.Graphs = value.Schema.Graphs[:1]
	if _, err := schemaAssertionsFromManifest(value); err == nil {
		t.Fatalf("expected missing schema error")
	}
}

func TestResolveFragmentEdge(t *testing.T) {
	nodeMap := map[string]graph.ID{
		"1": 101,
		"2": 202,
	}
	item := fragmentEdge{
		StartID:    "1",
		EndID:      "2",
		Kind:       "AdminTo",
		Properties: map[string]any{"source": "test"},
	}

	resolved, err := resolveFragmentEdge(item, nodeMap)
	if err != nil {
		t.Fatalf("resolve edge: %v", err)
	}
	if resolved.StartID != 101 || resolved.EndID != 202 || resolved.Kind.String() != "AdminTo" {
		t.Fatalf("unexpected resolved edge: %+v", resolved)
	}
	if resolved.Properties.Get("source").Any() != "test" {
		t.Fatalf("unexpected resolved properties: %+v", resolved.Properties.Map)
	}

	item.StartID = "missing"
	if _, err := resolveFragmentEdge(item, nodeMap); err == nil {
		t.Fatalf("expected missing start node error")
	}
	item.StartID = "1"
	item.EndID = "missing"
	if _, err := resolveFragmentEdge(item, nodeMap); err == nil {
		t.Fatalf("expected missing end node error")
	}
}

func TestVerifyManifestFilesRejectsBadChecksum(t *testing.T) {
	dir := t.TempDir()
	writer, err := newCompressedJSONLinesWriter(filepath.Join(dir, "nodes.jsonl.gz"), compressionGzip, defaultZstdLevel)
	if err != nil {
		t.Fatalf("open JSONL writer: %v", err)
	}
	if err := writer.Encode(fragmentNode{
		ID: "1",
	}); err != nil {
		t.Fatalf("write JSONL node: %v", err)
	}
	entry, err := writer.Close()
	if err != nil {
		t.Fatalf("close JSONL writer: %v", err)
	}

	value := newValidTestManifest(1)
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 1,
		Files: []fileManifest{{
			Phase:           phaseNodes,
			Path:            "nodes.jsonl.gz",
			Count:           1,
			CompressedBytes: entry.CompressedBytes,
			SHA256:          "bad",
		}},
	}}

	if err := verifyManifestFiles(dir, value); err == nil {
		t.Fatalf("expected checksum verification failure")
	}
	value.Graphs[0].Files[0].SHA256 = entry.SHA256
	if err := verifyManifestFiles(dir, value); err != nil {
		t.Fatalf("verify manifest files: %v", err)
	}
}
