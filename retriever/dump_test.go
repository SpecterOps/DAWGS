package retriever

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/specterops/dawgs/graph"
)

func TestDumpNestedScrubOutput(t *testing.T) {
	for _, mode := range []ScrubMode{ScrubNone, ScrubFull} {
		t.Run(string(mode), func(t *testing.T) {
			payload := map[string]any{
				"email":      "alice@example.com",
				"password":   "nested-secret",
				"created_at": "2026-01-01T00:00:00Z",
				"kind":       "User",
				"enabled":    true,
			}
			nodeProperties := map[string]any{"extra": payload}
			edgeProperties := map[string]any{"extra": []any{payload}}
			database := &checkpointTestDatabase{
				nodes: []*graph.Node{
					graph.NewNode(1, graph.AsProperties(nodeProperties), graph.StringKind("User")),
					graph.NewNode(2, graph.AsProperties(nodeProperties), graph.StringKind("User")),
				},
				relationships: []*graph.Relationship{
					graph.NewRelationship(3, 1, 2, graph.AsProperties(edgeProperties), graph.StringKind("Owns")),
				},
			}
			options := DefaultDumpOptions(t.TempDir())
			options.Scrub = mode
			options.Salt = "test-salt"
			options.Parquet = true
			options.BatchSize = 1
			options.ShardSize = 1
			result, err := Dump(context.Background(), database, "test", []GraphTarget{{Name: "default"}}, options)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := ReadManifest(options.OutputDir)
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyManifestFiles(options.OutputDir, manifest); err != nil {
				t.Fatal(err)
			}
			wantPayload := payload
			if mode == ScrubFull {
				if manifest.Scrub.RulesVersion != "retriever-scrub-v3" {
					t.Fatalf("scrub rules version = %q", manifest.Scrub.RulesVersion)
				}
				wantPayload = map[string]any{
					"email":      "user-144497604961@example.invalid",
					"password":   "[REDACTED]",
					"created_at": "2026-01-18T00:00:00Z",
					"kind":       "User",
					"enabled":    true,
				}
				for _, counts := range []map[string]int{manifest.Scrub.NodeActionCounts, manifest.Graphs[0].NodeActionCounts} {
					if !reflect.DeepEqual(counts, map[string]int{"scrub_nested": 2}) {
						t.Fatalf("node counts = %v", counts)
					}
				}
				for _, counts := range []map[string]int{manifest.Scrub.EdgeActionCounts, manifest.Graphs[0].EdgeActionCounts} {
					if !reflect.DeepEqual(counts, map[string]int{"scrub_nested": 1}) {
						t.Fatalf("edge counts = %v", counts)
					}
				}
			}
			var nodes []FragmentNode
			var edges []FragmentEdge
			for _, file := range manifest.Graphs[0].Files {
				if mode == ScrubFull && !reflect.DeepEqual(file.ActionCounts, map[string]int{"scrub_nested": 1}) {
					t.Fatalf("file counts = %v", file.ActionCounts)
				}
				partner := assertCheckpointParquetPartnerReadable(t, options.OutputDir, file)
				if file.Phase == PhaseNodes {
					_, err = readNodeFragmentFile(options.OutputDir, manifest.Compression, file, func(item FragmentNode) error {
						nodes = append(nodes, item)
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
					rows, readErr := parquet.ReadFile[parquetNodeRow](filepath.Join(options.OutputDir, partner))
					if readErr != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0].Properties, map[string]any{"extra": wantPayload}) {
						t.Fatalf("node Parquet output = %#v, error %v", rows, readErr)
					}
				} else {
					_, err = readEdgeFragmentFile(options.OutputDir, manifest.Compression, file, func(item FragmentEdge) error {
						edges = append(edges, item)
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
					rows, readErr := parquet.ReadFile[parquetEdgeRow](filepath.Join(options.OutputDir, partner))
					if readErr != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0].Properties, map[string]any{"extra": []any{wantPayload}}) {
						t.Fatalf("edge Parquet output = %#v, error %v", rows, readErr)
					}
				}
			}
			if !reflect.DeepEqual(nodes, []FragmentNode{
				{ID: "1", Kinds: []string{"User"}, Properties: map[string]any{"extra": wantPayload}},
				{ID: "2", Kinds: []string{"User"}, Properties: map[string]any{"extra": wantPayload}},
			}) || !reflect.DeepEqual(edges, []FragmentEdge{{StartID: "1", EndID: "2", Kind: "Owns", Properties: map[string]any{"extra": []any{wantPayload}}}}) {
				t.Fatalf("dump topology or JSONL properties changed: nodes %#v, edges %#v", nodes, edges)
			}
			if result.NodeCount != 2 || result.EdgeCount != 1 || payload["password"] != "nested-secret" {
				t.Fatalf("dump counts or source properties changed: %+v, %#v", result, payload)
			}
		})
	}
}

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
