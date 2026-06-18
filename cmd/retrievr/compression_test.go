package main

import (
	"path/filepath"
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
		Items: []fragmentEdge{{StartID: "1", EndID: "2", Kind: "AdminTo"}},
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
