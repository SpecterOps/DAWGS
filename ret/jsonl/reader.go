package jsonl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/specterops/dawgs/ret/entity"
)

const (
	initialLineBuffer = 64 * 1024
	maxPhysicalLine   = 10 * 1024 * 1024
)

func ReadNodes(root string, artifact NodeArtifact) ([]entity.Node, error) {
	if artifact.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("unsupported JSONL artifact schema %q", artifact.SchemaVersion)
	}
	if err := (Config{Enabled: true, Codec: Codec(artifact.Codec), Level: artifact.Level}).Validate(); err != nil {
		return nil, fmt.Errorf("validate JSONL artifact codec: %w", err)
	}
	if artifact.Count < 0 || artifact.UncompressedBytes < 0 || artifact.StoredBytes < 0 {
		return nil, fmt.Errorf("JSONL artifact sizes and count must be non-negative")
	}
	path, err := artifactPath(root, artifact.Path)
	if err != nil {
		return nil, err
	}
	stored, err := readStoredSnapshot(path)
	if err != nil {
		return nil, err
	}
	if err := verifyStored(stored, artifact.StoredBytes, artifact.SHA256); err != nil {
		return nil, err
	}
	nodes, count, uncompressed, err := decodeNodes(stored, Codec(artifact.Codec))
	if err != nil {
		return nil, err
	}
	if err := verifyReadMetadata(count, uncompressed, artifact.Count, artifact.UncompressedBytes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func ReadRelationships(root string, artifact RelationshipArtifact) ([]entity.Relationship, error) {
	if artifact.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("unsupported JSONL artifact schema %q", artifact.SchemaVersion)
	}
	if err := (Config{Enabled: true, Codec: Codec(artifact.Codec), Level: artifact.Level}).Validate(); err != nil {
		return nil, fmt.Errorf("validate JSONL artifact codec: %w", err)
	}
	if artifact.Count < 0 || artifact.UncompressedBytes < 0 || artifact.StoredBytes < 0 {
		return nil, fmt.Errorf("JSONL artifact sizes and count must be non-negative")
	}
	path, err := artifactPath(root, artifact.Path)
	if err != nil {
		return nil, err
	}
	stored, err := readStoredSnapshot(path)
	if err != nil {
		return nil, err
	}
	if err := verifyStored(stored, artifact.StoredBytes, artifact.SHA256); err != nil {
		return nil, err
	}
	relationships, count, uncompressed, err := decodeRelationships(stored, Codec(artifact.Codec))
	if err != nil {
		return nil, err
	}
	if err := verifyReadMetadata(count, uncompressed, artifact.Count, artifact.UncompressedBytes); err != nil {
		return nil, err
	}
	return relationships, nil
}

func artifactPath(root, relativePath string) (string, error) {
	path, err := cleanRelativePath(relativePath)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, filepath.FromSlash(path)), nil
}

func cleanRelativePath(path string) (string, error) {
	if path == "" || strings.Contains(path, "\\") {
		return "", fmt.Errorf("jsonl artifact path must be a slash-separated relative file: %q", path)
	}
	clean := filepath.Clean(filepath.FromSlash(path))
	if filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("jsonl artifact path escapes collection: %q", path)
	}
	cleanSlash := filepath.ToSlash(clean)
	if cleanSlash != path {
		return "", fmt.Errorf("jsonl artifact path is not clean: %q", path)
	}
	return cleanSlash, nil
}

func readStoredSnapshot(path string) ([]byte, error) {
	stored, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read JSONL artifact: %w", err)
	}
	return stored, nil
}

func verifyStored(stored []byte, expectedBytes int64, expectedSHA256 string) error {
	hasher := sha256.New()
	if _, err := hasher.Write(stored); err != nil {
		return fmt.Errorf("hash JSONL artifact: %w", err)
	}
	if int64(len(stored)) != expectedBytes {
		return fmt.Errorf("JSONL stored size mismatch: got %d, want %d", len(stored), expectedBytes)
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != expectedSHA256 {
		return fmt.Errorf("JSONL stored SHA-256 mismatch: got %s, want %s", actual, expectedSHA256)
	}
	return nil
}

func verifyReadMetadata(count, uncompressed, expectedCount, expectedUncompressed int64) error {
	if count != expectedCount {
		return fmt.Errorf("JSONL record count mismatch: got %d, want %d", count, expectedCount)
	}
	if uncompressed != expectedUncompressed {
		return fmt.Errorf("JSONL uncompressed size mismatch: got %d, want %d", uncompressed, expectedUncompressed)
	}
	return nil
}

func decodeNodes(stored []byte, codec Codec) ([]entity.Node, int64, int64, error) {
	nodes := []entity.Node{}
	count, uncompressed, err := readRecords(stored, codec, func(line []byte, index int64) error {
		var record NodeRecord
		if err := decodeRecord(line, &record); err != nil {
			return fmt.Errorf("decode node record %d: %w", index, err)
		}
		if err := normalizeProperties(record.Properties); err != nil {
			return fmt.Errorf("normalize node record %d properties: %w", index, err)
		}
		value := record.entity()
		if err := value.Validate(); err != nil {
			return fmt.Errorf("validate node record %d: %w", index, err)
		}
		nodes = append(nodes, value)
		return nil
	})
	return nodes, count, uncompressed, err
}

func decodeRelationships(stored []byte, codec Codec) ([]entity.Relationship, int64, int64, error) {
	relationships := []entity.Relationship{}
	count, uncompressed, err := readRecords(stored, codec, func(line []byte, index int64) error {
		var record RelationshipRecord
		if err := decodeRecord(line, &record); err != nil {
			return fmt.Errorf("decode relationship record %d: %w", index, err)
		}
		if err := normalizeProperties(record.Properties); err != nil {
			return fmt.Errorf("normalize relationship record %d properties: %w", index, err)
		}
		value := record.entity()
		if err := value.Validate(); err != nil {
			return fmt.Errorf("validate relationship record %d: %w", index, err)
		}
		relationships = append(relationships, value)
		return nil
	})
	return relationships, count, uncompressed, err
}

func readRecords(stored []byte, codec Codec, handle func([]byte, int64) error) (int64, int64, error) {
	decompressor, err := newDecompressionReader(bytes.NewReader(stored), codec)
	if err != nil {
		return 0, 0, fmt.Errorf("open JSONL decompressor: %w", err)
	}
	counted := &countingReader{reader: decompressor}
	scanner := bufio.NewScanner(counted)
	scanner.Buffer(make([]byte, initialLineBuffer), maxPhysicalLine+1)

	var count int64
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) > maxPhysicalLine {
			_ = decompressor.Close()
			return count, counted.count, fmt.Errorf("read JSONL record %d: line exceeds %d bytes", count+1, maxPhysicalLine)
		}
		if len(bytes.TrimSpace(line)) == 0 {
			_ = decompressor.Close()
			return count, counted.count, fmt.Errorf("decode JSONL record %d: blank line", count+1)
		}
		if err := handle(line, count+1); err != nil {
			_ = decompressor.Close()
			return count, counted.count, err
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		_ = decompressor.Close()
		return count, counted.count, fmt.Errorf("read JSONL record %d: %w", count+1, err)
	}
	if err := decompressor.Close(); err != nil {
		return count, counted.count, fmt.Errorf("close JSONL decompressor: %w", err)
	}
	return count, counted.count, nil
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (s *countingReader) Read(value []byte) (int, error) {
	read, err := s.reader.Read(value)
	s.count += int64(read)
	return read, err
}

func decodeRecord(line []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func normalizeProperties(properties map[string]any) error {
	for key, value := range properties {
		normalized, err := normalizeJSONValue(value, "properties."+key)
		if err != nil {
			return err
		}
		properties[key] = normalized
	}
	return nil
}

func normalizeJSONValue(value any, path string) (any, error) {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed, nil
	case json.Number:
		literal := typed.String()
		if !strings.ContainsAny(literal, ".eE") {
			integer, err := strconv.ParseInt(literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("JSON integer at %s is outside the int64 domain: %q", path, literal)
			}
			return integer, nil
		}
		fractional, err := strconv.ParseFloat(literal, 64)
		if err != nil || math.IsNaN(fractional) || math.IsInf(fractional, 0) {
			return nil, fmt.Errorf("JSON number at %s is not a finite float64: %q", path, literal)
		}
		return fractional, nil
	case []any:
		for index, element := range typed {
			normalized, err := normalizeJSONValue(element, fmt.Sprintf("%s[%d]", path, index))
			if err != nil {
				return nil, err
			}
			typed[index] = normalized
		}
		return typed, nil
	case map[string]any:
		for key, element := range typed {
			normalized, err := normalizeJSONValue(element, path+"."+key)
			if err != nil {
				return nil, err
			}
			typed[key] = normalized
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("JSON value at %s has unsupported decoded type %T", path, value)
	}
}

func newDecompressionReader(reader io.Reader, codec Codec) (io.ReadCloser, error) {
	switch codec {
	case CodecNone:
		return io.NopCloser(reader), nil
	case CodecGzip:
		return gzip.NewReader(reader)
	case CodecZstd:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return decoder.IOReadCloser(), nil
	default:
		return nil, fmt.Errorf("unsupported JSONL codec %q", codec)
	}
}
