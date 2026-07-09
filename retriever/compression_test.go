package retriever

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompressedJSONRoundTrip(t *testing.T) {
	for _, codec := range []CompressionCodec{CompressionGzip, CompressionZstd} {
		t.Run(string(codec), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fragment")
			entry, err := writeCompressedJSON(path, codec, DefaultZstdLevel, NodeFragment{
				Phase: PhaseNodes,
				Items: []FragmentNode{{
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

			var decoded NodeFragment
			if err := readCompressedJSON(path, codec, &decoded); err != nil {
				t.Fatalf("read compressed JSON: %v", err)
			}

			if decoded.Phase != PhaseNodes || len(decoded.Items) != 1 || decoded.Items[0].ID != "1" {
				t.Fatalf("unexpected decoded fragment: %+v", decoded)
			}
		})
	}
}

func TestVerifyChecksumFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fragment")
	entry, err := writeCompressedJSON(path, CompressionGzip, DefaultZstdLevel, EdgeFragment{
		Phase: PhaseEdges,
		Items: []FragmentEdge{{
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

func TestReadCompressedJSONRejectsCorruptPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.gz")
	if err := os.WriteFile(path, []byte("not gzip"), 0o600); err != nil {
		t.Fatalf("write corrupt payload: %v", err)
	}

	var fragment NodeFragment
	if err := readCompressedJSON(path, CompressionGzip, &fragment); err == nil {
		t.Fatalf("expected corrupt gzip error")
	}
}

func TestEmptyFragmentRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.zst")
	_, err := writeCompressedJSON(path, CompressionZstd, DefaultZstdLevel, EdgeFragment{
		Phase: PhaseEdges,
		Items: nil,
	})
	if err != nil {
		t.Fatalf("write empty fragment: %v", err)
	}

	var decoded EdgeFragment

	if err := readCompressedJSON(path, CompressionZstd, &decoded); err != nil {
		t.Fatalf("read empty fragment: %v", err)
	}

	if decoded.Phase != PhaseEdges || len(decoded.Items) != 0 {
		t.Fatalf("unexpected empty fragment decode: %+v", decoded)
	}
}

func TestCompressedJSONSizeAndCompactEncoding(t *testing.T) {
	fragment := NodeFragment{
		Phase: PhaseNodes,
		Items: []FragmentNode{{
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

	uncompressedBytes, compressedBytes, err := compressedJSONSize(CompressionGzip, DefaultZstdLevel, fragment)
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
