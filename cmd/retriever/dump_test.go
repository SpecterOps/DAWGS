package main

import (
	"path/filepath"
	"testing"
)

func TestPhaseFilePath(t *testing.T) {
	nodePath, err := phaseFilePath("graph/name", phaseNodes, compressionZstd)
	if err != nil {
		t.Fatalf("node phase path: %v", err)
	}
	if nodePath != "graphs/graph%2Fname/nodes.jsonl.zst" {
		t.Fatalf("unexpected node phase path %q", nodePath)
	}

	edgePath, err := phaseFilePath("default", phaseEdges, compressionGzip)
	if err != nil {
		t.Fatalf("edge phase path: %v", err)
	}
	if edgePath != "graphs/default/edges.jsonl.gz" {
		t.Fatalf("unexpected edge phase path %q", edgePath)
	}

	if _, err := phaseFilePath("default", phase("bad"), compressionGzip); err == nil {
		t.Fatalf("expected unsupported phase error")
	}
}

func TestWriteFragmentMetadata(t *testing.T) {
	options := dumpOptions{
		OutputDir:   t.TempDir(),
		Compression: compressionGzip,
		ZstdLevel:   defaultZstdLevel,
	}

	fileEntry, err := writeNodeFragment(options.OutputDir, "default", options, []fragmentNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"name": "alice"},
	}}, map[string]int{"pseudonymize": 1})
	if err != nil {
		t.Fatalf("write node fragment: %v", err)
	}
	if fileEntry.Phase != phaseNodes || fileEntry.Path != "graphs/default/nodes.jsonl.gz" || fileEntry.Count != 1 {
		t.Fatalf("unexpected node file manifest: %+v", fileEntry)
	}
	if fileEntry.ActionCounts["pseudonymize"] != 1 {
		t.Fatalf("missing action count: %+v", fileEntry.ActionCounts)
	}
	if _, err := readManifest(filepath.Join(options.OutputDir, "graphs")); err == nil {
		t.Fatalf("phase write should not create manifest")
	}

	edgeEntry, err := writeEdgeFragment(options.OutputDir, "default", options, []fragmentEdge{{
		StartID: "1",
		EndID:   "2",
		Kind:    "AdminTo",
	}}, nil)
	if err != nil {
		t.Fatalf("write edge fragment: %v", err)
	}
	if edgeEntry.Phase != phaseEdges || edgeEntry.Path != "graphs/default/edges.jsonl.gz" || edgeEntry.Count != 1 {
		t.Fatalf("unexpected edge file manifest: %+v", edgeEntry)
	}
}

func TestKindAndActionHelpers(t *testing.T) {
	kinds := map[string]struct{}{}
	addKindsToSet(kinds, []string{"User", "", "Computer", "User"})
	if got := stringsFromKindSet(kinds); len(got) != 2 || got[0] != "Computer" || got[1] != "User" {
		t.Fatalf("unexpected kinds: %v", got)
	}

	target := map[string]int{"preserve": 1}
	addActionCounts(target, map[string]int{"preserve": 2, "redact": 3})
	if target["preserve"] != 3 || target["redact"] != 3 {
		t.Fatalf("unexpected action counts: %+v", target)
	}

	clone := cloneActionCounts(target)
	clone["preserve"] = 100
	if target["preserve"] == 100 {
		t.Fatalf("expected clone to be independent")
	}

	total := fileTotal([]fileManifest{
		{
			Count: 2,
		},
		{
			Count: 3,
		},
	})
	if total != 5 {
		t.Fatalf("file total = %d", total)
	}
}
