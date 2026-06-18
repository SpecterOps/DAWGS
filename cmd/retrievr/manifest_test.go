package main

import (
	"path/filepath"
	"testing"
)

func TestManifestValidateRejectsUnsupportedFormat(t *testing.T) {
	value := newManifest("pg", compressionGzip, defaultZstdLevel, scrubMetadata{
		Mode:             scrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}, 0)
	value.Format = "future"

	if err := value.validate(); err == nil {
		t.Fatalf("expected unsupported format error")
	}
}

func TestManifestValidateRequiresNodeFilesBeforeEdgeFiles(t *testing.T) {
	value := newManifest("pg", compressionGzip, defaultZstdLevel, scrubMetadata{
		Mode:             scrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}, 1)
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 1,
		Files: []fileManifest{
			{Phase: phaseEdges, Path: "graphs/default/edges-000001.ogfrag.gz", Count: 1, SHA256: "abc"},
			{Phase: phaseNodes, Path: "graphs/default/nodes-000001.ogfrag.gz", Count: 1, SHA256: "def"},
		},
	}}

	if err := value.validate(); err == nil {
		t.Fatalf("expected phase ordering error")
	}
}

func TestWriteReadManifest(t *testing.T) {
	dir := t.TempDir()
	value := newManifest("pg", compressionGzip, defaultZstdLevel, scrubMetadata{
		Mode:             scrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}, 1)
	value.Schema.Graphs = []graphSchemaMetadata{{Name: "default", NodeKinds: []string{"User"}, EdgeKinds: []string{"AdminTo"}}}
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 0,
		EdgeCount: 0,
	}}

	if err := writeManifest(dir, value); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := readManifest(dir); err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if _, err := readManifest(filepath.Join(dir, "missing")); err == nil {
		t.Fatalf("expected missing manifest error")
	}
}
