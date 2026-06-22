package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompressedJSONRoundTrip(t *testing.T) {
	for _, codec := range []compressionCodec{compressionGzip, compressionZstd} {
		t.Run(string(codec), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fragment")
			entry, err := writeCompressedJSON(path, codec, defaultZstdLevel, nodeFragment{
				Phase: phaseNodes,
				Items: []fragmentNode{{
					ID:         "1",
					Kinds:      []string{"User"},
					Properties: map[string]any{"name": "alice"},
				}},
			})
			if err != nil {
				t.Fatalf("write compressed JSON: %v", err)
			}
			if entry.SHA256 == "" {
				t.Fatalf("expected checksum")
			}
			if entry.CompressedBytes <= 0 || entry.UncompressedBytes <= 0 {
				t.Fatalf("expected positive byte counts: %+v", entry)
			}
			if err := verifyChecksum(path, entry.SHA256, entry.CompressedBytes); err != nil {
				t.Fatalf("verify checksum: %v", err)
			}

			var decoded nodeFragment
			if err := readCompressedJSON(path, codec, &decoded); err != nil {
				t.Fatalf("read compressed JSON: %v", err)
			}
			if decoded.Phase != phaseNodes || len(decoded.Items) != 1 || decoded.Items[0].ID != "1" {
				t.Fatalf("unexpected decoded fragment: %+v", decoded)
			}
		})
	}
}

func TestVerifyChecksumFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fragment")
	entry, err := writeCompressedJSON(path, compressionGzip, defaultZstdLevel, edgeFragment{
		Phase: phaseEdges,
		Items: []fragmentEdge{{
			StartID: "1",
			EndID:   "2",
			Kind:    "AdminTo",
		}},
	})
	if err != nil {
		t.Fatalf("write compressed JSON: %v", err)
	}

	if err := verifyChecksum(path, entry.SHA256, entry.CompressedBytes+1); err == nil {
		t.Fatalf("expected byte-count mismatch")
	}
	if err := verifyChecksum(path, "not-the-real-checksum", entry.CompressedBytes); err == nil {
		t.Fatalf("expected checksum mismatch")
	}
}

func TestCompressionUnsupportedCodec(t *testing.T) {
	if err := validateCompression(compressionCodec("zip")); err == nil {
		t.Fatalf("expected unsupported codec")
	}
	if _, err := compressionExtension(compressionCodec("zip")); err == nil {
		t.Fatalf("expected unsupported extension codec")
	}
	if _, err := newCompressionWriter(&bytes.Buffer{}, compressionCodec("zip"), defaultZstdLevel); err == nil {
		t.Fatalf("expected unsupported writer codec")
	}
	if _, err := newDecompressionReader(strings.NewReader("bad"), compressionCodec("zip")); err == nil {
		t.Fatalf("expected unsupported reader codec")
	}
}

func TestReadCompressedJSONRejectsCorruptPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.gz")
	if err := os.WriteFile(path, []byte("not gzip"), 0o600); err != nil {
		t.Fatalf("write corrupt payload: %v", err)
	}

	var fragment nodeFragment
	if err := readCompressedJSON(path, compressionGzip, &fragment); err == nil {
		t.Fatalf("expected corrupt gzip error")
	}
}

func TestEmptyFragmentRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.zst")
	_, err := writeCompressedJSON(path, compressionZstd, defaultZstdLevel, edgeFragment{
		Phase: phaseEdges,
		Items: nil,
	})
	if err != nil {
		t.Fatalf("write empty fragment: %v", err)
	}

	var decoded edgeFragment
	if err := readCompressedJSON(path, compressionZstd, &decoded); err != nil {
		t.Fatalf("read empty fragment: %v", err)
	}
	if decoded.Phase != phaseEdges || len(decoded.Items) != 0 {
		t.Fatalf("unexpected empty fragment decode: %+v", decoded)
	}
}

func TestCompressedJSONSizeAndCompactEncoding(t *testing.T) {
	fragment := nodeFragment{
		Phase: phaseNodes,
		Items: []fragmentNode{{
			ID:    "1",
			Kinds: []string{"User"},
		}},
	}

	payload, err := encodeCompactJSON(fragment)
	if err != nil {
		t.Fatalf("encode compact JSON: %v", err)
	}
	if strings.Contains(string(payload), "\n") || strings.Contains(string(payload), "  ") {
		t.Fatalf("expected compact JSON, got %q", payload)
	}

	uncompressedBytes, compressedBytes, err := compressedJSONSize(compressionGzip, defaultZstdLevel, fragment)
	if err != nil {
		t.Fatalf("compressed JSON size: %v", err)
	}
	if uncompressedBytes != int64(len(payload)) {
		t.Fatalf("uncompressed bytes = %d, want %d", uncompressedBytes, len(payload))
	}
	if compressedBytes <= 0 {
		t.Fatalf("expected compressed bytes > 0")
	}
}
