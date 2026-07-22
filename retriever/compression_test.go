package retriever

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCompressedJSONLinesRoundTrip(t *testing.T) {
	for _, codec := range []CompressionCodec{CompressionNone, CompressionGzip, CompressionZstd} {
		t.Run(string(codec), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fragment")
			records := []FragmentNode{
				{
					ID:         "1",
					Kinds:      []string{"User"},
					Properties: map[string]any{"name": "alice"},
				},
				{
					ID:    "2",
					Kinds: []string{"Computer"},
				},
			}

			entry, err := writeCompressedJSONLines(path, codec, DefaultZstdLevel, records)
			if err != nil {
				t.Fatalf("write compressed JSONL: %v", err)
			}

			if entry.Count != len(records) || entry.SHA256 == "" {
				t.Fatalf("unexpected fragment metadata: %+v", entry)
			}

			if entry.CompressedBytes <= 0 || entry.UncompressedBytes <= 0 {
				t.Fatalf("expected positive byte counts: %+v", entry)
			}

			if err := verifyChecksum(path, entry.SHA256, entry.CompressedBytes); err != nil {
				t.Fatalf("verify checksum: %v", err)
			}

			var decoded []FragmentNode
			count, err := readCompressedJSONLines(path, codec, func(record FragmentNode) error {
				decoded = append(decoded, record)

				return nil
			})
			if err != nil {
				t.Fatalf("read compressed JSONL: %v", err)
			}

			if count != 2 || len(decoded) != 2 || decoded[0].ID != "1" || decoded[1].ID != "2" {
				t.Fatalf("unexpected decoded records: %+v", decoded)
			}
		})
	}
}

func TestVerifyChecksumFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fragment")
	entry, err := writeCompressedJSONLines(path, CompressionGzip, DefaultZstdLevel, []FragmentEdge{{
		StartID: "1",
		EndID:   "2",
		Kind:    "AdminTo",
	}})
	if err != nil {
		t.Fatalf("write compressed JSONL: %v", err)
	}

	if err := verifyChecksum(path, entry.SHA256, entry.CompressedBytes+1); err == nil {
		t.Fatalf("expected byte-count mismatch")
	} else {
		var mismatch ByteCountMismatchError

		if !errors.As(err, &mismatch) {
			t.Fatalf("expected ByteCountMismatchError, got %T: %v", err, err)
		}

		if mismatch.Path != path || mismatch.ExpectedBytes != entry.CompressedBytes+1 || mismatch.ActualBytes != entry.CompressedBytes {
			t.Fatalf("unexpected byte-count mismatch: %+v", mismatch)
		}
	}

	if err := verifyChecksum(path, "not-the-real-checksum", entry.CompressedBytes); err == nil {
		t.Fatalf("expected checksum mismatch")
	} else {
		var mismatch ChecksumMismatchError

		if !errors.As(err, &mismatch) {
			t.Fatalf("expected ChecksumMismatchError, got %T: %v", err, err)
		}

		if mismatch.Path != path || mismatch.ExpectedSHA256 != "not-the-real-checksum" || mismatch.ActualSHA256 == "" {
			t.Fatalf("unexpected checksum mismatch: %+v", mismatch)
		}
	}
}

func TestCompressionUnsupportedCodec(t *testing.T) {
	if err := validateCompression(CompressionCodec("zip")); err == nil {
		t.Fatalf("expected unsupported codec")
	}
	if _, err := compressionExtension(CompressionCodec("zip")); err == nil {
		t.Fatalf("expected unsupported extension codec")
	}

	if _, err := newCompressionWriter(&bytes.Buffer{}, CompressionCodec("zip"), DefaultZstdLevel); err == nil {
		t.Fatalf("expected unsupported writer codec")
	}

	if _, err := newDecompressionReader(strings.NewReader("bad"), CompressionCodec("zip")); err == nil {
		t.Fatalf("expected unsupported reader codec")
	}
}

func TestReadCompressedJSONLinesRejectsCorruptPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.gz")
	if err := os.WriteFile(path, []byte("not gzip"), 0o600); err != nil {
		t.Fatalf("write corrupt payload: %v", err)
	}

	if _, err := readCompressedJSONLines[FragmentNode](path, CompressionGzip, nil); err == nil {
		t.Fatalf("expected corrupt gzip error")
	}
}

func TestEmptyJSONLinesFragmentRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.zst")
	entry, err := writeCompressedJSONLines(path, CompressionZstd, DefaultZstdLevel, []FragmentEdge(nil))
	if err != nil {
		t.Fatalf("write empty fragment: %v", err)
	}

	count, err := readCompressedJSONLines[FragmentEdge](path, CompressionZstd, nil)
	if err != nil {
		t.Fatalf("read empty fragment: %v", err)
	}

	if entry.Count != 0 || entry.UncompressedBytes != 0 || count != 0 {
		t.Fatalf("unexpected empty fragment metadata: entry=%+v count=%d", entry, count)
	}
}

func TestCompressedJSONLinesSize(t *testing.T) {
	records := []FragmentNode{{
		ID:    "1",
		Kinds: []string{"User"},
	}}
	expectedPayload := "{\"id\":\"1\",\"kinds\":[\"User\"]}\n"

	uncompressedBytes, compressedBytes, err := compressedJSONLinesSize(CompressionGzip, DefaultZstdLevel, records)
	if err != nil {
		t.Fatalf("compressed JSONL size: %v", err)
	}

	if uncompressedBytes != int64(len(expectedPayload)) {
		t.Fatalf("uncompressed bytes = %d, want %d", uncompressedBytes, len(expectedPayload))
	}

	if compressedBytes <= 0 {
		t.Fatalf("expected compressed bytes > 0")
	}
}

func TestReadCompressedJSONLinesValidatesPhysicalLines(t *testing.T) {
	tests := map[string]struct {
		payload string
		wantErr string
		want    int
	}{
		"blank line": {
			payload: "{\"id\":\"1\",\"kinds\":[]}\n\n",
			wantErr: "blank line",
		},
		"malformed record": {
			payload: "{\"id\":\"1\",\"kinds\":[]}\n{bad}\n",
			wantErr: "record 2",
		},
		"unknown field": {
			payload: "{\"phase\":\"nodes\",\"items\":[]}\n",
			wantErr: "unknown field",
		},
		"multiple values": {
			payload: "{\"id\":\"1\",\"kinds\":[]} {}\n",
			wantErr: "record 1",
		},
		"missing final newline": {
			payload: "{\"id\":\"1\",\"kinds\":[]}",
			want:    1,
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fragment.gz")
			writeCompressedPayload(t, path, CompressionGzip, testCase.payload)

			count, err := readCompressedJSONLines[FragmentNode](path, CompressionGzip, nil)
			if testCase.wantErr == "" {
				if err != nil {
					t.Fatalf("read JSONL: %v", err)
				}
				if count != testCase.want {
					t.Fatalf("record count = %d, want %d", count, testCase.want)
				}
			} else if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("expected error containing %q, got %v", testCase.wantErr, err)
			}
		})
	}
}

func TestReadCompressedJSONLinesSupportsLargeRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.gz")
	records := []FragmentNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"description": strings.Repeat("x", 1024*1024)},
	}}

	if _, err := writeCompressedJSONLines(path, CompressionGzip, DefaultZstdLevel, records); err != nil {
		t.Fatalf("write large record: %v", err)
	}

	count, err := readCompressedJSONLines[FragmentNode](path, CompressionGzip, nil)
	if err != nil {
		t.Fatalf("read large record: %v", err)
	}
	if count != 1 {
		t.Fatalf("record count = %d", count)
	}
}

func TestReadCompressedJSONLinesRejectsLineLargerThanBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "too-large.gz")
	payload := `{"id":"1","kinds":[],"properties":{"description":"` +
		strings.Repeat("x", maxJSONLLineBytes) + `"}}` + "\n"
	writeCompressedPayload(t, path, CompressionGzip, payload)

	count, err := readCompressedJSONLines[FragmentNode](path, CompressionGzip, nil)
	if err == nil {
		t.Fatal("expected oversized line to fail")
	}
	if count != 0 {
		t.Fatalf("record count = %d, want 0", count)
	}
	if !strings.Contains(err.Error(), "read JSONL record 1") || (!strings.Contains(err.Error(), "token too long") && !strings.Contains(err.Error(), "line exceeds")) {
		t.Fatalf("unexpected oversized-line error: %v", err)
	}
}

func TestReadCompressedJSONLinesAcceptsMaximumLineSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "maximum.jsonl")
	prefix := `{"id":"1","kinds":[],"properties":{"description":"`
	suffix := `"}}`
	payload := prefix + strings.Repeat("x", maxJSONLLineBytes-len(prefix)-len(suffix)) + suffix
	if len(payload) != maxJSONLLineBytes {
		t.Fatalf("test payload size = %d", len(payload))
	}
	writeCompressedPayload(t, path, CompressionNone, payload)

	count, err := readCompressedJSONLines[FragmentNode](path, CompressionNone, nil)
	if err != nil {
		t.Fatalf("read maximum JSONL line: %v", err)
	}
	if count != 1 {
		t.Fatalf("record count = %d", count)
	}
}

func TestZstdWriterCanBeReusedAcrossFragments(t *testing.T) {
	for index := 1; index <= 2; index++ {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("fragment-%d.zst", index))
		entry, err := writeCompressedJSONLines(path, CompressionZstd, DefaultZstdLevel, []FragmentNode{{ID: strconv.Itoa(index)}})
		if err != nil {
			t.Fatalf("write zstd fragment %d: %v", index, err)
		}
		if err := verifyChecksum(path, entry.SHA256, entry.CompressedBytes); err != nil {
			t.Fatalf("verify zstd fragment %d: %v", index, err)
		}
		count, err := readCompressedJSONLines[FragmentNode](path, CompressionZstd, nil)
		if err != nil || count != 1 {
			t.Fatalf("read zstd fragment %d: count=%d err=%v", index, count, err)
		}
	}
}

type alwaysFailWriter struct{}

func (alwaysFailWriter) Write([]byte) (int, error) {
	return 0, errors.New("injected writer failure")
}

func TestZstdWriterResetDoesNotReuseFailedState(t *testing.T) {
	const level = 7
	var first bytes.Buffer
	firstWriter, err := newCompressionWriter(&first, CompressionZstd, level)
	if err != nil {
		t.Fatalf("open first zstd writer: %v", err)
	}
	if _, err := firstWriter.Write([]byte("first fragment")); err != nil {
		t.Fatalf("write first fragment: %v", err)
	}
	if err := firstWriter.Close(); err != nil {
		t.Fatalf("close first fragment: %v", err)
	}

	failedWriter, err := newCompressionWriter(alwaysFailWriter{}, CompressionZstd, level)
	if err != nil {
		t.Fatalf("reset pooled zstd writer: %v", err)
	}
	_, writeErr := failedWriter.Write(bytes.Repeat([]byte("failure"), 64*1024))
	closeErr := failedWriter.Close()
	if writeErr == nil && closeErr == nil {
		t.Fatal("expected reset writer failure")
	}

	var final bytes.Buffer
	finalWriter, err := newCompressionWriter(&final, CompressionZstd, level)
	if err != nil {
		t.Fatalf("open zstd writer after failure: %v", err)
	}
	if _, err := finalWriter.Write([]byte("final fragment")); err != nil {
		t.Fatalf("write final fragment: %v", err)
	}
	if err := finalWriter.Close(); err != nil {
		t.Fatalf("close final fragment: %v", err)
	}
	reader, err := newDecompressionReader(bytes.NewReader(final.Bytes()), CompressionZstd)
	if err != nil {
		t.Fatalf("open final fragment: %v", err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read final fragment: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close final reader: %v", err)
	}
	if string(decoded) != "final fragment" {
		t.Fatalf("final decoded payload = %q", decoded)
	}
}

func TestZstdWriterCanBeReusedAfterFragmentAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aborted.zst")
	aborted, err := newCompressedJSONLinesWriter(path, CompressionZstd, DefaultZstdLevel)
	if err != nil {
		t.Fatalf("open aborted fragment: %v", err)
	}
	if err := aborted.Write(FragmentNode{ID: "aborted"}); err != nil {
		t.Fatalf("write aborted fragment: %v", err)
	}
	aborted.Abort()

	finalPath := filepath.Join(t.TempDir(), "final.zst")
	entry, err := writeCompressedJSONLines(finalPath, CompressionZstd, DefaultZstdLevel, []FragmentNode{{ID: "final"}})
	if err != nil {
		t.Fatalf("write after abort: %v", err)
	}
	if err := verifyChecksum(finalPath, entry.SHA256, entry.CompressedBytes); err != nil {
		t.Fatalf("verify after abort: %v", err)
	}
}

func TestCompressedJSONLinesWriterAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fragment.gz")
	writer, err := newCompressedJSONLinesWriter(path, CompressionGzip, DefaultZstdLevel)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	if err := writer.Write(FragmentNode{ID: "1"}); err != nil {
		t.Fatalf("write record: %v", err)
	}
	writer.Abort()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected final path to be absent, got %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected temporary path to be absent, got %v", err)
	}
	if err := writer.Write(FragmentNode{ID: "2"}); err == nil {
		t.Fatalf("expected write after abort to fail")
	}
}

func writeCompressedPayload(t *testing.T, path string, codec CompressionCodec, payload string) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open compressed payload: %v", err)
	}

	compressor, err := newCompressionWriter(file, codec, DefaultZstdLevel)
	if err != nil {
		_ = file.Close()
		t.Fatalf("open compressor: %v", err)
	}

	if _, err := compressor.Write([]byte(payload)); err != nil {
		_ = compressor.Close()
		_ = file.Close()
		t.Fatalf("write compressed payload: %v", err)
	}
	if err := compressor.Close(); err != nil {
		_ = file.Close()
		t.Fatalf("close compressor: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close compressed payload: %v", err)
	}
}
