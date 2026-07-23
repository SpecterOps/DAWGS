package retriever

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestCollectionTarDeterministicAndSorted(t *testing.T) {
	dir := writeArchiveFixture(t)
	value, err := readManifest(dir)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	archivePaths, err := ArchivePathsFromManifest(value)
	if err != nil {
		t.Fatalf("archive paths from manifest: %v", err)
	}

	if len(archivePaths) != 3 {
		t.Fatalf("archive path count = %d, want 3: %v", len(archivePaths), archivePaths)
	}

	var first bytes.Buffer

	if err := writeCollectionTar(&first, dir); err != nil {
		t.Fatalf("write first tar: %v", err)
	}

	var second bytes.Buffer

	if err := writeCollectionTar(&second, dir); err != nil {
		t.Fatalf("write second tar: %v", err)
	}

	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatalf("tar output is not deterministic")
	}

	names := tarEntryNames(t, first.Bytes())

	if !sort.StringsAreSorted(names) {
		t.Fatalf("tar paths are not sorted: %v", names)
	}

	if len(names) != 3 {
		t.Fatalf("tar entry count = %d, want 3: %v", len(names), names)
	}
}

func TestCollectionTarIgnoresFilesAbsentFromManifest(t *testing.T) {
	dir := writeArchiveFixture(t)
	orphanPath := filepath.Join(dir, "graphs", "secret-graph", "orphan.jsonl.gz")
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o600); err != nil {
		t.Fatalf("write orphan fragment: %v", err)
	}

	var buffer bytes.Buffer
	if err := writeCollectionTar(&buffer, dir); err != nil {
		t.Fatalf("write tar: %v", err)
	}

	for _, name := range tarEntryNames(t, buffer.Bytes()) {
		if name == "graphs/secret-graph/orphan.jsonl.gz" {
			t.Fatalf("archive included unmanifested file %q", name)
		}
	}
}

func TestCollectionTarStableMetadata(t *testing.T) {
	dir := writeArchiveFixture(t)
	var buffer bytes.Buffer
	if err := writeCollectionTar(&buffer, dir); err != nil {
		t.Fatalf("write tar: %v", err)
	}

	reader := tar.NewReader(bytes.NewReader(buffer.Bytes()))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar header: %v", err)
		}

		if header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" {
			t.Fatalf("tar header leaked host ownership: %+v", header)
		}

		if header.Mode != archiveFileMode {
			t.Fatalf("tar header mode for %s = %o, want %o", header.Name, header.Mode, archiveFileMode)
		}

		if !header.ModTime.Equal(archiveZeroTime) {
			t.Fatalf("tar header mtime for %s = %s, want %s", header.Name, header.ModTime, archiveZeroTime)
		}

		if header.Typeflag != tar.TypeReg {
			t.Fatalf("tar header type for %s = %d, want regular file", header.Name, header.Typeflag)
		}
	}
}

func TestCollectionTarRejectsUnsafeInputs(t *testing.T) {
	for name, archivePath := range map[string]string{
		"absolute":       "/tmp/fragment.gz",
		"parent":         "../fragment.gz",
		"nested parent":  "graphs/../fragment.gz",
		"back separator": `graphs\default\nodes-000001.jsonl.gz`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := writeManifestWithArchivePath(t, archivePath)
			var buffer bytes.Buffer
			if err := writeCollectionTar(&buffer, dir); err == nil {
				t.Fatalf("expected unsafe path %q to be rejected", archivePath)
			}
		})
	}
}

func TestCollectionTarRejectsSymlinks(t *testing.T) {
	dir := writeArchiveFixture(t)
	value, err := readManifest(dir)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	relativePath := value.Graphs[0].Files[0].Path
	absolutePath := filepath.Join(dir, filepath.FromSlash(relativePath))

	if err := os.Remove(absolutePath); err != nil {
		t.Fatalf("remove fragment: %v", err)
	}

	if err := os.Symlink("target", absolutePath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	var buffer bytes.Buffer
	if err := writeCollectionTar(&buffer, dir); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestUnpackTarRejectsUnsafeEntries(t *testing.T) {
	for name, build := range map[string]func(*testing.T) []byte{
		"path traversal": func(t *testing.T) []byte {
			return buildTarPayload(t, &tar.Header{
				Typeflag: tar.TypeReg,
				Name:     "../evil",
				Mode:     0o600,
				Size:     int64(len("bad")),
			}, []byte("bad"))
		},
		"symlink": func(t *testing.T) []byte {
			return buildTarPayload(t, &tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     "Manifest.json",
				Linkname: "target",
			}, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := unpackTar(bytes.NewReader(build(t)), filepath.Join(t.TempDir(), "out"), false)
			if err == nil {
				t.Fatalf("expected unsafe tar entry error")
			}
		})
	}
}

func TestUnpackTarForceSemantics(t *testing.T) {
	dir := writeArchiveFixture(t)
	var buffer bytes.Buffer
	if err := writeCollectionTar(&buffer, dir); err != nil {
		t.Fatalf("write tar: %v", err)
	}

	outputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(outputDir, "old"), []byte("old"), 0o600); err != nil {
		t.Fatalf("write old file: %v", err)
	}

	if err := unpackTar(bytes.NewReader(buffer.Bytes()), outputDir, false); err == nil {
		t.Fatalf("expected non-empty output directory error")
	}

	if err := unpackTar(bytes.NewReader(buffer.Bytes()), outputDir, true); err != nil {
		t.Fatalf("unpack with force: %v", err)
	}

	if _, err := os.Stat(filepath.Join(outputDir, "old")); !os.IsNotExist(err) {
		t.Fatalf("expected force unpack to remove old file, got %v", err)
	}

	if _, err := readManifest(outputDir); err != nil {
		t.Fatalf("read unpacked Manifest: %v", err)
	}
}

func writeArchiveFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	options := DumpOptions{
		OutputDir:   dir,
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
	}
	nodeSummary := shardSummary{
		ID:   shardID{Graph: "secret-graph", Phase: PhaseNodes, Number: 1},
		Rows: 1,
	}
	nodeMetadata := writeTestJSONLShard(t, newJSONLNodeSink(options), nodeSummary, []normalizedNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"name": "alice"},
	}})
	nodeEntry := newJSONLFileManifest(nodeSummary, nodeMetadata)

	edgeSummary := shardSummary{
		ID:   shardID{Graph: "secret-graph", Phase: PhaseEdges, Number: 1},
		Rows: 1,
	}
	edgeMetadata := writeTestJSONLShard(t, newJSONLEdgeSink(options), edgeSummary, []normalizedEdge{{
		StartID: "1",
		EndID:   "1",
		Kind:    "MemberOf",
	}})
	edgeEntry := newJSONLFileManifest(edgeSummary, edgeMetadata)

	value := newValidTestManifest(1)
	value.Schema.Graphs = []GraphSchemaMetadata{{
		Name:      "secret-graph",
		NodeKinds: []string{"User"},
		EdgeKinds: []string{"MemberOf"},
	}}
	value.Graphs = []GraphManifest{{
		Name:      "secret-graph",
		NodeCount: 1,
		EdgeCount: 1,
		Files:     []FileManifest{nodeEntry, edgeEntry},
	}}
	if err := writeManifest(dir, value); err != nil {
		t.Fatalf("write Manifest: %v", err)
	}

	return dir
}

func writeManifestWithArchivePath(t *testing.T, archivePath string) string {
	t.Helper()
	dir := t.TempDir()
	value := newValidTestManifest(1)
	value.Graphs = []GraphManifest{{
		Name:      "default",
		NodeCount: 0,
		EdgeCount: 0,
		Files: []FileManifest{{
			Phase:           PhaseNodes,
			Path:            archivePath,
			Count:           0,
			CompressedBytes: 0,
			SHA256:          "abc",
		}},
	}}
	if err := writeManifest(dir, value); err != nil {
		t.Fatalf("write Manifest: %v", err)
	}

	return dir
}

func tarEntryNames(t *testing.T, payload []byte) []string {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(payload))
	var names []string
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar entry: %v", err)
		}

		names = append(names, header.Name)
	}

	return names
}

func buildTarPayload(t *testing.T, header *tar.Header, payload []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	if err := writer.WriteHeader(header); err != nil {
		t.Fatalf("write tar header: %v", err)
	}

	if len(payload) > 0 {
		if _, err := writer.Write(payload); err != nil {
			t.Fatalf("write tar payload: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	return buffer.Bytes()
}
