package retriever

import (
	"path/filepath"
	"testing"
)

func TestFragmentPath(t *testing.T) {
	nodePath, err := fragmentPath("graph/name", PhaseNodes, 7, CompressionZstd)
	if err != nil {
		t.Fatalf("node fragment path: %v", err)
	}
	if nodePath != "graphs/graph%2Fname/nodes-000007.ogfrag.zst" {
		t.Fatalf("unexpected node fragment path %q", nodePath)
	}

	edgePath, err := fragmentPath("default", PhaseEdges, 3, CompressionGzip)
	if err != nil {
		t.Fatalf("edge fragment path: %v", err)
	}
	if edgePath != "graphs/default/edges-000003.ogfrag.gz" {
		t.Fatalf("unexpected edge fragment path %q", edgePath)
	}

	if _, err := fragmentPath("default", Phase("bad"), 1, CompressionGzip); err == nil {
		t.Fatalf("expected unsupported Phase error")
	}
	if _, err := fragmentPath("default", PhaseNodes, 0, CompressionGzip); err == nil {
		t.Fatalf("expected invalid shard number error")
	}
}

func TestWriteFragmentMetadata(t *testing.T) {
	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
	}

	fileEntry, err := writeNodeFragment(options.OutputDir, "default", 1, options, []FragmentNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"name": "alice"},
	}}, map[string]int{"pseudonymize": 1})
	if err != nil {
		t.Fatalf("write node fragment: %v", err)
	}
	if fileEntry.Phase != PhaseNodes || fileEntry.Path != "graphs/default/nodes-000001.ogfrag.gz" || fileEntry.Count != 1 {
		t.Fatalf("unexpected node file Manifest: %+v", fileEntry)
	}
	if fileEntry.ActionCounts["pseudonymize"] != 1 {
		t.Fatalf("missing action count: %+v", fileEntry.ActionCounts)
	}
	if _, err := readManifest(filepath.Join(options.OutputDir, "graphs")); err == nil {
		t.Fatalf("fragment write should not create Manifest")
	}

	edgeEntry, err := writeEdgeFragment(options.OutputDir, "default", 2, options, []FragmentEdge{{
		StartID: "1",
		EndID:   "2",
		Kind:    "AdminTo",
	}}, nil)
	if err != nil {
		t.Fatalf("write edge fragment: %v", err)
	}
	if edgeEntry.Phase != PhaseEdges || edgeEntry.Path != "graphs/default/edges-000002.ogfrag.gz" || edgeEntry.Count != 1 {
		t.Fatalf("unexpected edge file Manifest: %+v", edgeEntry)
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

	total := fileTotal([]FileManifest{
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
