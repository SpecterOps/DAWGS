package jsonl

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/specterops/dawgs/ret/entity"
)

type countingWriter struct {
	writer io.Writer
	count  int64
}

func (s *countingWriter) Write(value []byte) (int, error) {
	written, err := s.writer.Write(value)
	s.count += int64(written)
	return written, err
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

type writeStats struct {
	sha256                                string
	count, uncompressedBytes, storedBytes int64
}

func WriteNodes(tempPath, finalRelativePath string, config Config, nodes []entity.Node) (NodeArtifact, error) {
	path, err := preflightWrite(tempPath, finalRelativePath, config)
	if err != nil {
		return NodeArtifact{}, err
	}

	records := make([]NodeRecord, len(nodes))
	for index, value := range nodes {
		if err := value.Validate(); err != nil {
			return NodeArtifact{}, fmt.Errorf("record %d: validate node: %w", index+1, err)
		}
		records[index] = nodeRecord(value)
	}

	stats, err := writeRecords(tempPath, config, records)
	if err != nil {
		return NodeArtifact{}, err
	}
	return NodeArtifact{
		SchemaVersion:     SchemaVersion,
		Path:              path,
		Codec:             string(config.Codec),
		SHA256:            stats.sha256,
		Level:             config.Level,
		Count:             stats.count,
		UncompressedBytes: stats.uncompressedBytes,
		StoredBytes:       stats.storedBytes,
	}, nil
}

func WriteRelationships(tempPath, finalRelativePath string, config Config, relationships []entity.Relationship) (RelationshipArtifact, error) {
	path, err := preflightWrite(tempPath, finalRelativePath, config)
	if err != nil {
		return RelationshipArtifact{}, err
	}

	records := make([]RelationshipRecord, len(relationships))
	for index, value := range relationships {
		if err := value.Validate(); err != nil {
			return RelationshipArtifact{}, fmt.Errorf("record %d: validate relationship: %w", index+1, err)
		}
		records[index] = relationshipRecord(value)
	}

	stats, err := writeRecords(tempPath, config, records)
	if err != nil {
		return RelationshipArtifact{}, err
	}
	return RelationshipArtifact{
		SchemaVersion:     SchemaVersion,
		Path:              path,
		Codec:             string(config.Codec),
		SHA256:            stats.sha256,
		Level:             config.Level,
		Count:             stats.count,
		UncompressedBytes: stats.uncompressedBytes,
		StoredBytes:       stats.storedBytes,
	}, nil
}

func preflightWrite(tempPath, finalRelativePath string, config Config) (string, error) {
	if !config.Enabled {
		return "", fmt.Errorf("JSONL output is disabled")
	}
	if err := config.Validate(); err != nil {
		return "", err
	}
	if !filepath.IsAbs(tempPath) {
		return "", fmt.Errorf("JSONL temporary path must be absolute: %q", tempPath)
	}
	path, err := cleanRelativePath(finalRelativePath)
	if err != nil {
		return "", err
	}
	return path, nil
}

func writeRecords[T any](tempPath string, config Config, records []T) (writeStats, error) {
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return writeStats{}, fmt.Errorf("open JSONL temporary file: %w", err)
	}

	hasher := sha256.New()
	stored := &countingWriter{writer: io.MultiWriter(file, hasher)}
	compressor, err := newCompressionWriter(stored, config)
	if err != nil {
		_ = file.Close()
		return writeStats{}, err
	}
	uncompressed := &countingWriter{writer: compressor}

	var writeErr error
	for index, record := range records {
		encoded, err := json.Marshal(record)
		if err != nil {
			writeErr = fmt.Errorf("encode JSONL record %d: %w", index+1, err)
			break
		}
		if _, err := uncompressed.Write(append(encoded, '\n')); err != nil {
			writeErr = fmt.Errorf("write JSONL record %d: %w", index+1, err)
			break
		}
	}
	compressionErr := compressor.Close()
	closeErr := file.Close()
	if writeErr != nil {
		return writeStats{}, writeErr
	}
	if compressionErr != nil {
		return writeStats{}, fmt.Errorf("finish JSONL compression: %w", compressionErr)
	}
	if closeErr != nil {
		return writeStats{}, fmt.Errorf("close JSONL temporary file: %w", closeErr)
	}
	return writeStats{
		sha256:            hex.EncodeToString(hasher.Sum(nil)),
		count:             int64(len(records)),
		uncompressedBytes: uncompressed.count,
		storedBytes:       stored.count,
	}, nil
}

func newCompressionWriter(writer io.Writer, config Config) (io.WriteCloser, error) {
	switch config.Codec {
	case CodecNone:
		return nopWriteCloser{Writer: writer}, nil
	case CodecGzip:
		return gzip.NewWriterLevel(writer, config.gzipLevel())
	case CodecZstd:
		return zstd.NewWriter(writer, zstd.WithEncoderLevel(config.zstdLevel()))
	default:
		return nil, fmt.Errorf("unsupported JSONL codec %q", config.Codec)
	}
}
