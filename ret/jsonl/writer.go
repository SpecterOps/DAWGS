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

func WriteNodes(tempPath, finalRelativePath string, config Config, nodes []entity.Node) (NodeArtifact, error) {
	metadata, err := write(tempPath, finalRelativePath, config, nodes, func(value entity.Node) (any, error) {
		if err := value.Validate(); err != nil {
			return nil, fmt.Errorf("validate node: %w", err)
		}
		return nodeRecord(value), nil
	})
	if err != nil {
		return NodeArtifact{}, err
	}
	return NodeArtifact{SchemaVersion: SchemaVersion, Path: metadata.path, Codec: string(config.Codec), SHA256: metadata.sha256, Level: config.Level, Count: metadata.count, UncompressedBytes: metadata.uncompressedBytes, StoredBytes: metadata.storedBytes}, nil
}

func WriteRelationships(tempPath, finalRelativePath string, config Config, relationships []entity.Relationship) (RelationshipArtifact, error) {
	metadata, err := write(tempPath, finalRelativePath, config, relationships, func(value entity.Relationship) (any, error) {
		if err := value.Validate(); err != nil {
			return nil, fmt.Errorf("validate relationship: %w", err)
		}
		return relationshipRecord(value), nil
	})
	if err != nil {
		return RelationshipArtifact{}, err
	}
	return RelationshipArtifact{SchemaVersion: SchemaVersion, Path: metadata.path, Codec: string(config.Codec), SHA256: metadata.sha256, Level: config.Level, Count: metadata.count, UncompressedBytes: metadata.uncompressedBytes, StoredBytes: metadata.storedBytes}, nil
}

func write[T any](tempPath, finalRelativePath string, config Config, values []T, record func(T) (any, error)) (artifactMetadata, error) {
	if !config.Enabled {
		return artifactMetadata{}, fmt.Errorf("JSONL output is disabled")
	}
	if err := config.Validate(); err != nil {
		return artifactMetadata{}, err
	}
	if !filepath.IsAbs(tempPath) {
		return artifactMetadata{}, fmt.Errorf("JSONL temporary path must be absolute: %q", tempPath)
	}
	path, err := cleanRelativePath(finalRelativePath)
	if err != nil {
		return artifactMetadata{}, err
	}

	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return artifactMetadata{}, fmt.Errorf("open JSONL temporary file: %w", err)
	}

	result, writeErr := writeFile(file, config, values, record)
	closeErr := file.Close()
	if writeErr != nil {
		return artifactMetadata{}, writeErr
	}
	if closeErr != nil {
		return artifactMetadata{}, fmt.Errorf("close JSONL temporary file: %w", closeErr)
	}
	result.path = path
	return result, nil
}

func writeFile[T any](file *os.File, config Config, values []T, record func(T) (any, error)) (artifactMetadata, error) {
	hasher := sha256.New()
	stored := &countingWriter{writer: io.MultiWriter(file, hasher)}
	compressor, err := newCompressionWriter(stored, config)
	if err != nil {
		return artifactMetadata{}, err
	}
	uncompressed := &countingWriter{writer: compressor}

	for index, value := range values {
		record, err := record(value)
		if err != nil {
			_ = compressor.Close()
			return artifactMetadata{}, fmt.Errorf("record %d: %w", index+1, err)
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			_ = compressor.Close()
			return artifactMetadata{}, fmt.Errorf("encode JSONL record %d: %w", index+1, err)
		}
		if _, err := uncompressed.Write(append(encoded, '\n')); err != nil {
			_ = compressor.Close()
			return artifactMetadata{}, fmt.Errorf("write JSONL record %d: %w", index+1, err)
		}
	}
	if err := compressor.Close(); err != nil {
		return artifactMetadata{}, fmt.Errorf("finish JSONL compression: %w", err)
	}
	return artifactMetadata{count: int64(len(values)), uncompressedBytes: uncompressed.count, storedBytes: stored.count, sha256: hex.EncodeToString(hasher.Sum(nil))}, nil
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
