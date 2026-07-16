package retriever

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"
)

func TestJSONLAndParquetNodeFragmentsAreLogicallyEquivalent(t *testing.T) {
	outputDir := t.TempDir()
	workspace := newLocalCollectionWorkspace(outputDir, false)
	options := DumpOptions{Compression: CompressionGzip, ZstdLevel: DefaultZstdLevel}
	output := newShardSinkSet(
		newJSONLShardSink(newJSONLNodeSinkInWorkspace(options, workspace)),
		newParquetShardSink[normalizedNode](newParquetNodeSinkInWorkspace(workspace)),
	)
	id := shardID{Graph: "graph/name", Phase: PhaseNodes, Number: 1}
	records := []normalizedNode{
		{ID: "1", Kinds: []string{"Computer", "User"}, Properties: map[string]any{"active": true, "name": "alice", "scores": []any{1.0, 2.0}}},
		{ID: "2", Kinds: []string{"Group"}},
	}
	committed := writeTestShard(t, output, id, records)
	if committed.Parquet == nil {
		t.Fatalf("missing committed Parquet metadata")
	}
	assertParquetMetadata(t, outputDir, *committed.Parquet)

	var jsonlRows []FragmentNode
	jsonlPath := filepath.Join(outputDir, filepath.FromSlash(committed.JSONL.Path))
	if _, err := readCompressedJSONLines[FragmentNode](jsonlPath, options.Compression, func(row FragmentNode) error {
		jsonlRows = append(jsonlRows, row)
		return nil
	}); err != nil {
		t.Fatalf("read JSONL nodes: %v", err)
	}
	parquetRows, err := parquetgo.ReadFile[parquetNode](filepath.Join(outputDir, filepath.FromSlash(committed.Parquet.Path)))
	if err != nil {
		t.Fatalf("read Parquet nodes: %v", err)
	}
	if len(jsonlRows) != len(parquetRows) {
		t.Fatalf("JSONL rows = %d, Parquet rows = %d", len(jsonlRows), len(parquetRows))
	}
	for index, parquetRow := range parquetRows {
		jsonlRow := jsonlRows[index]
		if parquetRow.ID != jsonlRow.ID || !reflect.DeepEqual(parquetRow.Kinds, jsonlRow.Kinds) || !reflect.DeepEqual(decodeParquetProperties(t, parquetRow.Properties), jsonlRow.Properties) {
			t.Fatalf("node row %d differs: JSONL=%+v Parquet=%+v", index, jsonlRow, parquetRow)
		}
	}
}

func TestJSONLAndParquetEdgeFragmentsAreLogicallyEquivalentAndParquetRetainsID(t *testing.T) {
	outputDir := t.TempDir()
	workspace := newLocalCollectionWorkspace(outputDir, false)
	options := DumpOptions{Compression: CompressionGzip, ZstdLevel: DefaultZstdLevel}
	output := newShardSinkSet(
		newJSONLShardSink(newJSONLEdgeSinkInWorkspace(options, workspace)),
		newParquetShardSink[normalizedEdge](newParquetEdgeSinkInWorkspace(workspace)),
	)
	id := shardID{Graph: "graph/name", Phase: PhaseEdges, Number: 1}
	records := []normalizedEdge{
		{ID: "relationship-7", StartID: "1", EndID: "2", Kind: "MemberOf", Properties: map[string]any{"weight": 3.0}},
		{ID: "relationship-8", StartID: "2", EndID: "3", Kind: "Owns"},
	}
	committed := writeTestShard(t, output, id, records)
	if committed.Parquet == nil {
		t.Fatalf("missing committed Parquet metadata")
	}
	assertParquetMetadata(t, outputDir, *committed.Parquet)

	var jsonlRows []FragmentEdge
	jsonlPath := filepath.Join(outputDir, filepath.FromSlash(committed.JSONL.Path))
	if _, err := readCompressedJSONLines[FragmentEdge](jsonlPath, options.Compression, func(row FragmentEdge) error {
		jsonlRows = append(jsonlRows, row)
		return nil
	}); err != nil {
		t.Fatalf("read JSONL edges: %v", err)
	}
	parquetRows, err := parquetgo.ReadFile[parquetEdge](filepath.Join(outputDir, filepath.FromSlash(committed.Parquet.Path)))
	if err != nil {
		t.Fatalf("read Parquet edges: %v", err)
	}
	if len(jsonlRows) != len(parquetRows) {
		t.Fatalf("JSONL rows = %d, Parquet rows = %d", len(jsonlRows), len(parquetRows))
	}
	for index, parquetRow := range parquetRows {
		jsonlRow := jsonlRows[index]
		if parquetRow.ID != records[index].ID {
			t.Fatalf("Parquet edge ID = %q, want %q", parquetRow.ID, records[index].ID)
		}
		if parquetRow.StartID != jsonlRow.StartID || parquetRow.EndID != jsonlRow.EndID || parquetRow.Kind != jsonlRow.Kind || !reflect.DeepEqual(decodeParquetProperties(t, parquetRow.Properties), jsonlRow.Properties) {
			t.Fatalf("edge row %d differs: JSONL=%+v Parquet=%+v", index, jsonlRow, parquetRow)
		}
	}
}

func TestParquetPropertyConversionFailureLeavesNoArtifact(t *testing.T) {
	outputDir := t.TempDir()
	workspace := newLocalCollectionWorkspace(outputDir, false)
	sink := newParquetNodeSinkInWorkspace(workspace)
	id := shardID{Graph: "example", Phase: PhaseNodes, Number: 1}
	writer, err := sink.Open(context.Background(), id)
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	err = writer.WriteBatch(context.Background(), []normalizedNode{{ID: "1", Properties: map[string]any{"unsupported": func() {}}}})
	if err == nil {
		t.Fatalf("expected property conversion failure")
	}
	if err := writer.Abort(); err != nil {
		t.Fatalf("abort failed writer: %v", err)
	}
	relativePath, err := parquetFragmentPath(id.Graph, id.Phase, id.Number)
	if err != nil {
		t.Fatalf("fragment path: %v", err)
	}
	assertPathAbsent(t, filepath.Join(outputDir, filepath.FromSlash(relativePath)))
	assertPathAbsent(t, filepath.Join(outputDir, filepath.FromSlash(relativePath))+".tmp")
}

func writeTestShard[T any](t *testing.T, output shardOutput[T], id shardID, records []T) committedShard {
	t.Helper()
	writer, err := output.OpenShard(context.Background(), id)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	if err := writer.WriteBatch(context.Background(), records); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	committed, err := writer.Finish(context.Background(), shardSummary{ID: id, Rows: len(records)})
	if err != nil {
		t.Fatalf("finish shard: %v", err)
	}
	return committed
}

func decodeParquetProperties(t *testing.T, properties string) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal([]byte(properties), &value); err != nil {
		t.Fatalf("decode Parquet properties: %v", err)
	}
	return value
}

func assertParquetMetadata(t *testing.T, outputDir string, metadata parquetFragmentMetadata) {
	t.Helper()
	if metadata.Rows <= 0 || metadata.Bytes <= 0 || metadata.SHA256 == "" {
		t.Fatalf("invalid Parquet metadata: %+v", metadata)
	}
	if err := verifyChecksum(filepath.Join(outputDir, filepath.FromSlash(metadata.Path)), metadata.SHA256, metadata.Bytes); err != nil {
		t.Fatalf("verify Parquet metadata: %v", err)
	}
}
