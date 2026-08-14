package retriever

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestNodeFragmentWriter(t *testing.T) {
	t.Run("writes JSONL and Parquet when enabled", func(t *testing.T) {
		jsonlPath := filepath.Join(t.TempDir(), "nodes.jsonl")
		parquetPath := filepath.Join(t.TempDir(), "nodes.parquet")
		options := fragmentWriterTestOptions(t, true)
		first := FragmentNode{ID: "node-1", Kinds: []string{"Person", "Employee"}, Properties: representativeParquetProperties()}
		second := FragmentNode{ID: "node-2", Kinds: []string{"Device"}, Properties: map[string]any{"name": "Laptop"}}

		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, options)
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		if _, err := os.Stat(jsonlPath + ".tmp"); err != nil {
			t.Fatalf("stat JSONL staging file: %v", err)
		}
		assertAbsent(t, jsonlPath+".tmp.tmp")
		if err := writer.Write(first); err != nil {
			t.Fatalf("write first node: %v", err)
		}
		if writer.Count() != 1 {
			t.Fatalf("count after first node = %d, want 1", writer.Count())
		}
		if err := writer.Write(second); err != nil {
			t.Fatalf("write second node: %v", err)
		}
		if writer.Count() != 2 {
			t.Fatalf("count after second node = %d, want 2", writer.Count())
		}
		entry, err := writer.Close()
		if err != nil {
			t.Fatalf("close node writer: %v", err)
		}
		if entry.Count != 2 {
			t.Fatalf("manifest count = %d, want 2", entry.Count)
		}

		var jsonlRows []FragmentNode
		readJSONL(t, jsonlPath, &jsonlRows)
		if !reflect.DeepEqual(jsonlRows, []FragmentNode{first, second}) {
			t.Fatalf("JSONL rows = %#v, want %#v", jsonlRows, []FragmentNode{first, second})
		}

		parquetRows, err := parquet.ReadFile[parquetNodeRow](parquetPath)
		if err != nil {
			t.Fatalf("read node Parquet file: %v", err)
		}
		if len(parquetRows) != 2 {
			t.Fatalf("read %d node Parquet rows, want 2", len(parquetRows))
		}
		if parquetRows[0].ID != first.ID || !reflect.DeepEqual(parquetRows[0].Kinds, first.Kinds) || !reflect.DeepEqual(parquetRows[0].Properties, first.Properties) {
			t.Fatalf("first Parquet row = %#v, want node %#v", parquetRows[0], first)
		}
		if parquetRows[1].ID != second.ID || !reflect.DeepEqual(parquetRows[1].Kinds, second.Kinds) || !reflect.DeepEqual(parquetRows[1].Properties, second.Properties) {
			t.Fatalf("second Parquet row = %#v, want node %#v", parquetRows[1], second)
		}
	})

	t.Run("writes only JSONL when disabled", func(t *testing.T) {
		jsonlPath := filepath.Join(t.TempDir(), "nodes.jsonl")
		parquetPath := filepath.Join(t.TempDir(), "nodes.parquet")
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, false))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		if err := writer.Write(FragmentNode{ID: "node-1"}); err != nil {
			t.Fatalf("write node: %v", err)
		}
		if _, err := writer.Close(); err != nil {
			t.Fatalf("close node writer: %v", err)
		}

		var rows []FragmentNode
		readJSONL(t, jsonlPath, &rows)
		if !reflect.DeepEqual(rows, []FragmentNode{{ID: "node-1"}}) {
			t.Fatalf("JSONL rows = %#v, want node-1", rows)
		}
		assertAbsent(t, parquetPath)
		assertAbsent(t, parquetPath+".tmp")
	})

	t.Run("abort removes both outputs", func(t *testing.T) {
		jsonlPath := filepath.Join(t.TempDir(), "nodes.jsonl")
		parquetPath := filepath.Join(t.TempDir(), "nodes.parquet")
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, true))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		if err := writer.Write(FragmentNode{ID: "node-1"}); err != nil {
			t.Fatalf("write node: %v", err)
		}
		writer.Abort()

		assertAbsent(t, jsonlPath)
		assertAbsent(t, jsonlPath+".tmp")
		assertAbsent(t, parquetPath)
		assertAbsent(t, parquetPath+".tmp")
	})

	t.Run("Parquet write failure removes staging and final outputs", func(t *testing.T) {
		jsonlPath := filepath.Join(t.TempDir(), "nodes.jsonl")
		parquetPath := filepath.Join(t.TempDir(), "nodes.parquet")
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, true))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		writer.parquet.write = func(FragmentNode) error { return errors.New("injected parquet write failure") }
		if err := writer.Write(FragmentNode{ID: "node-1"}); err == nil {
			t.Fatal("expected Parquet write failure")
		}
		if writer.Count() != 0 {
			t.Fatalf("count after failed node = %d, want 0", writer.Count())
		}

		assertAbsent(t, jsonlPath)
		assertAbsent(t, jsonlPath+".tmp")
		assertAbsent(t, parquetPath)
		assertAbsent(t, parquetPath+".tmp")
	})

	t.Run("unsupported Parquet variant returns an error and removes every output", func(t *testing.T) {
		outputDir := t.TempDir()
		jsonlPath := filepath.Join(outputDir, "nodes.jsonl")
		parquetPath := filepath.Join(outputDir, "nodes.parquet")
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, true))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}

		err = writer.Write(FragmentNode{
			ID:         "node-1",
			Properties: map[string]any{"overflow": uint64(math.MaxUint64)},
		})
		if err == nil {
			t.Fatal("expected unsupported Parquet VARIANT value to return an error")
		}
		if writer.Count() != 0 {
			t.Fatalf("count after unsupported node = %d, want 0", writer.Count())
		}

		for _, path := range []string{
			jsonlPath,
			jsonlPath + ".tmp",
			parquetPath,
			parquetPath + ".tmp",
		} {
			assertAbsent(t, path)
		}
	})

	t.Run("abort panic does not escape or block output cleanup", func(t *testing.T) {
		writeErr := errors.New("injected parquet write failure")
		outputDir := t.TempDir()
		jsonlPath := filepath.Join(outputDir, "nodes.jsonl")
		parquetPath := filepath.Join(outputDir, "nodes.parquet")
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, true))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		originalAbort := writer.parquet.abort
		writer.parquet.write = func(FragmentNode) error { return writeErr }
		writer.parquet.abort = func() {
			originalAbort()
			panic("injected Parquet abort panic")
		}
		if err := writer.Write(FragmentNode{ID: "node-1"}); !errors.Is(err, writeErr) {
			t.Fatalf("write error = %v, want %v", err, writeErr)
		}

		for _, path := range []string{jsonlPath, jsonlPath + ".tmp", parquetPath, parquetPath + ".tmp"} {
			assertAbsent(t, path)
		}
	})

	t.Run("JSONL close failure removes Parquet staging", func(t *testing.T) {
		outputDir := t.TempDir()
		jsonlPath := filepath.Join(outputDir, "nodes.jsonl")
		parquetPath := filepath.Join(outputDir, "nodes.parquet")
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, true))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		if err := writer.Write(FragmentNode{ID: "node-1"}); err != nil {
			t.Fatalf("write node: %v", err)
		}
		if err := writer.jsonl.file.Close(); err != nil {
			t.Fatalf("inject JSONL close failure: %v", err)
		}
		if _, err := writer.Close(); err == nil {
			t.Fatal("expected JSONL close failure")
		}

		for _, path := range []string{jsonlPath, jsonlPath + ".tmp", parquetPath, parquetPath + ".tmp"} {
			assertAbsent(t, path)
		}
	})

	t.Run("Parquet close failure removes JSONL staging", func(t *testing.T) {
		outputDir := t.TempDir()
		jsonlPath := filepath.Join(outputDir, "nodes.jsonl")
		parquetPath := filepath.Join(outputDir, "nodes.parquet")
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, true))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		if err := writer.Write(FragmentNode{ID: "node-1"}); err != nil {
			t.Fatalf("write node: %v", err)
		}
		originalClose := writer.parquet.close
		writer.parquet.close = func() error {
			return errors.Join(originalClose(), errors.New("injected Parquet close failure"))
		}
		if _, err := writer.Close(); err == nil {
			t.Fatal("expected Parquet close failure")
		}

		for _, path := range []string{jsonlPath, jsonlPath + ".tmp", parquetPath, parquetPath + ".tmp"} {
			assertAbsent(t, path)
		}
	})

	t.Run("second rename failure removes published JSONL and Parquet staging", func(t *testing.T) {
		outputDir := t.TempDir()
		jsonlPath := filepath.Join(outputDir, "nodes.jsonl")
		parquetPath := filepath.Join(outputDir, "nodes.parquet")
		if err := os.Mkdir(parquetPath, 0o700); err != nil {
			t.Fatalf("create Parquet rename blocker: %v", err)
		}
		writer, err := newNodeFragmentWriter(jsonlPath, parquetPath, fragmentWriterTestOptions(t, true))
		if err != nil {
			t.Fatalf("create node writer: %v", err)
		}
		if err := writer.Write(FragmentNode{ID: "node-1"}); err != nil {
			t.Fatalf("write node: %v", err)
		}
		if _, err := writer.Close(); err == nil {
			t.Fatal("expected Parquet publish rename failure")
		}

		for _, path := range []string{jsonlPath, jsonlPath + ".tmp", parquetPath + ".tmp"} {
			assertAbsent(t, path)
		}
		if info, err := os.Stat(parquetPath); err != nil || !info.IsDir() {
			t.Fatalf("Parquet rename blocker was changed: info=%v err=%v", info, err)
		}
	})
}

func TestEdgeFragmentWriter(t *testing.T) {
	jsonlPath := filepath.Join(t.TempDir(), "edges.jsonl")
	parquetPath := filepath.Join(t.TempDir(), "edges.parquet")
	options := fragmentWriterTestOptions(t, true)
	first := FragmentEdge{StartID: "node-1", EndID: "node-2", Kind: "MemberOf", Properties: representativeParquetProperties()}
	second := FragmentEdge{StartID: "node-2", EndID: "node-3", Kind: "AdminTo", Properties: map[string]any{"enabled": false}}

	writer, err := newEdgeFragmentWriter(jsonlPath, parquetPath, options)
	if err != nil {
		t.Fatalf("create edge writer: %v", err)
	}
	if err := writer.Write(first); err != nil {
		t.Fatalf("write first edge: %v", err)
	}
	if err := writer.Write(second); err != nil {
		t.Fatalf("write second edge: %v", err)
	}
	if writer.Count() != 2 {
		t.Fatalf("count after edges = %d, want 2", writer.Count())
	}
	entry, err := writer.Close()
	if err != nil {
		t.Fatalf("close edge writer: %v", err)
	}
	if entry.Count != 2 {
		t.Fatalf("manifest count = %d, want 2", entry.Count)
	}

	var jsonlRows []FragmentEdge
	readJSONL(t, jsonlPath, &jsonlRows)
	if !reflect.DeepEqual(jsonlRows, []FragmentEdge{first, second}) {
		t.Fatalf("JSONL rows = %#v, want %#v", jsonlRows, []FragmentEdge{first, second})
	}

	parquetRows, err := parquet.ReadFile[parquetEdgeRow](parquetPath)
	if err != nil {
		t.Fatalf("read edge Parquet file: %v", err)
	}
	if len(parquetRows) != 2 {
		t.Fatalf("read %d edge Parquet rows, want 2", len(parquetRows))
	}
	if parquetRows[0].StartID != first.StartID || parquetRows[0].EndID != first.EndID || parquetRows[0].Kind != first.Kind || !reflect.DeepEqual(parquetRows[0].Properties, first.Properties) {
		t.Fatalf("first Parquet row = %#v, want edge %#v", parquetRows[0], first)
	}
	if parquetRows[1].StartID != second.StartID || parquetRows[1].EndID != second.EndID || parquetRows[1].Kind != second.Kind || !reflect.DeepEqual(parquetRows[1].Properties, second.Properties) {
		t.Fatalf("second Parquet row = %#v, want edge %#v", parquetRows[1], second)
	}
}

func fragmentWriterTestOptions(t *testing.T, parquetEnabled bool) DumpOptions {
	t.Helper()
	options := DefaultDumpOptions(t.TempDir())
	options.Compression = CompressionNone
	options.Parquet = parquetEnabled
	return options
}

func readJSONL[T any](t *testing.T, path string, records *[]T) {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open JSONL file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var record T
		if err := decoder.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			t.Fatalf("decode JSONL record: %v", err)
		}
		*records = append(*records, record)
	}
}

func assertAbsent(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %q to be absent, got %v", path, err)
	}
}
