package retriever

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestJSONLFragmentSinkOwnsWireFormatAndMetadata(t *testing.T) {
	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
	}
	spec := shardSpec{
		Graph:        "graph/name",
		Phase:        PhaseNodes,
		Number:       2,
		ExpectedRows: 1,
		ActionCounts: map[string]int{"preserve": 1},
	}

	metadata, err := writeFragment(context.Background(), newJSONLNodeSink(options), spec, []normalizedNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"name": "alice"},
	}})
	if err != nil {
		t.Fatalf("write JSONL fragment: %v", err)
	}
	if metadata.Phase != PhaseNodes || metadata.Path != "graphs/graph%2Fname/nodes-000002.jsonl.gz" || metadata.Count != 1 || !reflect.DeepEqual(metadata.ActionCounts, spec.ActionCounts) {
		t.Fatalf("JSONL metadata = %+v", metadata)
	}

	var records []FragmentNode
	if _, err := readCompressedJSONLines(filepath.Join(options.OutputDir, filepath.FromSlash(metadata.Path)), CompressionGzip, func(record FragmentNode) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatalf("read JSONL fragment: %v", err)
	}
	if !reflect.DeepEqual(records, []FragmentNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"name": "alice"},
	}}) {
		t.Fatalf("JSONL records = %#v", records)
	}
}

func TestJSONLEdgeSinkOmitsSourceRelationshipID(t *testing.T) {
	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
	}
	spec := shardSpec{Graph: "example", Phase: PhaseEdges, Number: 1, ExpectedRows: 1}
	metadata, err := writeFragment(context.Background(), newJSONLEdgeSink(options), spec, []normalizedEdge{{
		ID:      "source-edge-id",
		StartID: "1",
		EndID:   "2",
		Kind:    "MemberOf",
	}})
	if err != nil {
		t.Fatalf("write JSONL edge fragment: %v", err)
	}

	var records []FragmentEdge
	if _, err := readCompressedJSONLines(filepath.Join(options.OutputDir, filepath.FromSlash(metadata.Path)), CompressionGzip, func(record FragmentEdge) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatalf("read JSONL edge fragment: %v", err)
	}
	if !reflect.DeepEqual(records, []FragmentEdge{{StartID: "1", EndID: "2", Kind: "MemberOf"}}) {
		t.Fatalf("JSONL edge records = %#v", records)
	}
}

func TestJSONLFragmentSinkRejectsPhaseAndRowCountMismatch(t *testing.T) {
	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
	}
	sink := newJSONLNodeSink(options)
	if _, err := sink.Open(context.Background(), shardSpec{Graph: "example", Phase: PhaseEdges, Number: 1}); err == nil {
		t.Fatalf("expected phase mismatch")
	}

	spec := shardSpec{Graph: "example", Phase: PhaseNodes, Number: 1, ExpectedRows: 2}
	writer, err := sink.Open(context.Background(), spec)
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	if err := writer.WriteBatch(context.Background(), []normalizedNode{{ID: "1"}}); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	if _, err := writer.Prepare(context.Background()); err == nil {
		t.Fatalf("expected row count mismatch")
	}

	relativePath, err := jsonlFragmentPath(spec.Graph, spec.Phase, spec.Number, options.Compression)
	if err != nil {
		t.Fatalf("fragment path: %v", err)
	}
	absolutePath := filepath.Join(options.OutputDir, filepath.FromSlash(relativePath))
	if _, err := os.Stat(absolutePath); !os.IsNotExist(err) {
		t.Fatalf("final path exists after prepare failure: %v", err)
	}
	if _, err := os.Stat(absolutePath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("staged path exists after prepare failure: %v", err)
	}
}
