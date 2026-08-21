package retriever

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestFragmentPath(t *testing.T) {
	nodePath, err := fragmentPath("graph/name", PhaseNodes, 7, CompressionZstd)
	if err != nil {
		t.Fatalf("node fragment path: %v", err)
	}
	if nodePath != "graphs/graph%2Fname/nodes-000007.jsonl.zst" {
		t.Fatalf("unexpected node fragment path %q", nodePath)
	}

	edgePath, err := fragmentPath("default", PhaseEdges, 3, CompressionGzip)
	if err != nil {
		t.Fatalf("edge fragment path: %v", err)
	}
	if edgePath != "graphs/default/edges-000003.jsonl.gz" {
		t.Fatalf("unexpected edge fragment path %q", edgePath)
	}

	nodeParquetPath, err := parquetFragmentPath("graph/name", PhaseNodes, 7)
	if err != nil {
		t.Fatalf("node Parquet fragment path: %v", err)
	}
	if nodeParquetPath != "graphs/graph%2Fname/nodes-000007.parquet" {
		t.Fatalf("unexpected node Parquet fragment path %q", nodeParquetPath)
	}

	edgeParquetPath, err := parquetFragmentPath("default", PhaseEdges, 3)
	if err != nil {
		t.Fatalf("edge Parquet fragment path: %v", err)
	}
	if edgeParquetPath != "graphs/default/edges-000003.parquet" {
		t.Fatalf("unexpected edge Parquet fragment path %q", edgeParquetPath)
	}

	if _, err := fragmentPath("default", Phase("bad"), 1, CompressionGzip); err == nil {
		t.Fatalf("expected unsupported Phase error")
	}
	if _, err := fragmentPath("default", PhaseNodes, 0, CompressionGzip); err == nil {
		t.Fatalf("expected invalid shard number error")
	}
	if _, err := parquetFragmentPath("default", Phase("bad"), 1); err == nil {
		t.Fatalf("expected unsupported Parquet Phase error")
	}
	if _, err := parquetFragmentPath("default", PhaseNodes, 0); err == nil {
		t.Fatalf("expected invalid Parquet shard number error")
	}
}

func TestWriteFragmentMetadata(t *testing.T) {
	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Compression: CompressionGzip,
		Parquet:     true,
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
	if fileEntry.Phase != PhaseNodes || fileEntry.Path != "graphs/default/nodes-000001.jsonl.gz" || fileEntry.Count != 1 {
		t.Fatalf("unexpected node file Manifest: %+v", fileEntry)
	}
	if fileEntry.ActionCounts["pseudonymize"] != 1 {
		t.Fatalf("missing action count: %+v", fileEntry.ActionCounts)
	}
	if _, err := readManifest(filepath.Join(options.OutputDir, "graphs")); err == nil {
		t.Fatalf("fragment write should not create Manifest")
	}
	nodeRows, err := parquet.ReadFile[parquetNodeRow](filepath.Join(options.OutputDir, "graphs/default/nodes-000001.parquet"))
	if err != nil {
		t.Fatalf("read node Parquet sidecar: %v", err)
	}
	if len(nodeRows) != 1 || nodeRows[0].ID != "1" || !reflect.DeepEqual(nodeRows[0].Kinds, []string{"User"}) || !reflect.DeepEqual(nodeRows[0].Properties, map[string]any{"name": "alice"}) {
		t.Fatalf("unexpected node Parquet rows: %#v", nodeRows)
	}

	edgeEntry, err := writeEdgeFragment(options.OutputDir, "default", 2, options, []FragmentEdge{{
		StartID:    "1",
		EndID:      "2",
		Kind:       "AdminTo",
		Properties: map[string]any{"active": true},
	}}, nil)
	if err != nil {
		t.Fatalf("write edge fragment: %v", err)
	}
	if edgeEntry.Phase != PhaseEdges || edgeEntry.Path != "graphs/default/edges-000002.jsonl.gz" || edgeEntry.Count != 1 {
		t.Fatalf("unexpected edge file Manifest: %+v", edgeEntry)
	}
	edgeRows, err := parquet.ReadFile[parquetEdgeRow](filepath.Join(options.OutputDir, "graphs/default/edges-000002.parquet"))
	if err != nil {
		t.Fatalf("read edge Parquet sidecar: %v", err)
	}
	if !reflect.DeepEqual(edgeRows, []parquetEdgeRow{{StartID: "1", EndID: "2", Kind: "AdminTo", Properties: map[string]any{"active": true}}}) {
		t.Fatalf("unexpected edge Parquet rows: %#v", edgeRows)
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
