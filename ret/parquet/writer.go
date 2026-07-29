package parquet

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
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

func WriteNodes(tempPath, finalRelativePath string, config Config, nodes []entity.Node) (NodeArtifact, error) {
	rows := make([]NodeRow, len(nodes))
	for index, value := range nodes {
		if err := value.Validate(); err != nil {
			return NodeArtifact{}, fmt.Errorf("validate Parquet node %d: %w", index+1, err)
		}
		if err := validateProperties(value.Properties); err != nil {
			return NodeArtifact{}, fmt.Errorf("validate Parquet node %d properties: %w", index+1, err)
		}
		rows[index] = nodeRow(value)
	}

	metadata, err := writeParquet(tempPath, finalRelativePath, config, rows)
	if err != nil {
		return NodeArtifact{}, err
	}
	return NodeArtifact{
		SchemaVersion: SchemaVersion,
		Path:          metadata.path,
		SHA256:        metadata.sha256,
		Count:         metadata.count,
		StoredBytes:   metadata.storedBytes,
	}, nil
}

func WriteRelationships(tempPath, finalRelativePath string, config Config, relationships []entity.Relationship) (RelationshipArtifact, error) {
	rows := make([]RelationshipRow, len(relationships))
	for index, value := range relationships {
		if value.SourceID == "" {
			return RelationshipArtifact{}, fmt.Errorf("validate Parquet relationship %d: relationship source ID is required", index+1)
		}
		if err := value.Validate(); err != nil {
			return RelationshipArtifact{}, fmt.Errorf("validate Parquet relationship %d: %w", index+1, err)
		}
		if err := validateProperties(value.Properties); err != nil {
			return RelationshipArtifact{}, fmt.Errorf("validate Parquet relationship %d properties: %w", index+1, err)
		}
		rows[index] = relationshipRow(value)
	}

	metadata, err := writeParquet(tempPath, finalRelativePath, config, rows)
	if err != nil {
		return RelationshipArtifact{}, err
	}
	return RelationshipArtifact{
		SchemaVersion: SchemaVersion,
		Path:          metadata.path,
		SHA256:        metadata.sha256,
		Count:         metadata.count,
		StoredBytes:   metadata.storedBytes,
	}, nil
}

func writeParquet[T any](tempPath, finalRelativePath string, config Config, rows []T) (artifactMetadata, error) {
	if !config.Enabled {
		return artifactMetadata{}, fmt.Errorf("Parquet output is disabled")
	}
	if err := config.Validate(); err != nil {
		return artifactMetadata{}, err
	}
	if !filepath.IsAbs(tempPath) {
		return artifactMetadata{}, fmt.Errorf("Parquet temporary path must be absolute: %q", tempPath)
	}
	path, err := cleanRelativePath(finalRelativePath)
	if err != nil {
		return artifactMetadata{}, err
	}

	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return artifactMetadata{}, fmt.Errorf("open Parquet temporary file: %w", err)
	}

	hasher := sha256.New()
	stored := &countingWriter{writer: io.MultiWriter(file, hasher)}
	writer := parquetgo.NewGenericWriter[T](
		stored,
		parquetgo.Compression(&zstd.Codec{}),
	)
	written, writeErr := writer.Write(rows)
	closeWriterErr := writer.Close()
	closeFileErr := file.Close()
	if writeErr != nil {
		return artifactMetadata{}, fmt.Errorf("write Parquet rows: %w", writeErr)
	}
	if written != len(rows) {
		return artifactMetadata{}, fmt.Errorf("write Parquet rows: wrote %d, want %d", written, len(rows))
	}
	if closeWriterErr != nil {
		return artifactMetadata{}, fmt.Errorf("finish Parquet file: %w", closeWriterErr)
	}
	if closeFileErr != nil {
		return artifactMetadata{}, fmt.Errorf("close Parquet temporary file: %w", closeFileErr)
	}

	return artifactMetadata{
		path:        path,
		sha256:      hex.EncodeToString(hasher.Sum(nil)),
		count:       int64(written),
		storedBytes: stored.count,
	}, nil
}
