package parquet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/specterops/dawgs/ret/entity"
)

// afterVerifiedSnapshotForTest lets same-package regression tests replace the
// source path after verification. It remains nil in production.
var afterVerifiedSnapshotForTest func()

func ReadNodes(root string, artifact NodeArtifact, visit func(entity.Node) error) error {
	stored, metadata, err := verifiedSnapshot(root, artifact.metadata())
	if err != nil {
		return err
	}
	nodes, err := decodeParquet(stored, metadata, func(row NodeRow) (entity.Node, error) {
		return row.entity()
	})
	if err != nil {
		return err
	}
	for index, node := range nodes {
		if visit != nil {
			if err := visit(node); err != nil {
				return fmt.Errorf("visit Parquet node %d: %w", index+1, err)
			}
		}
	}
	return nil
}

func ReadRelationships(root string, artifact RelationshipArtifact, visit func(entity.Relationship) error) error {
	stored, metadata, err := verifiedSnapshot(root, artifact.metadata())
	if err != nil {
		return err
	}
	relationships, err := decodeParquet(stored, metadata, func(row RelationshipRow) (entity.Relationship, error) {
		return row.entity()
	})
	if err != nil {
		return err
	}
	for index, relationship := range relationships {
		if visit != nil {
			if err := visit(relationship); err != nil {
				return fmt.Errorf("visit Parquet relationship %d: %w", index+1, err)
			}
		}
	}
	return nil
}

func verifiedSnapshot(root string, artifact artifactMetadata) ([]byte, artifactMetadata, error) {
	path, err := validateArtifact(artifact)
	if err != nil {
		return nil, artifactMetadata{}, err
	}
	stored, err := readStoredSnapshot(root, path)
	if err != nil {
		return nil, artifactMetadata{}, fmt.Errorf("read Parquet artifact: %w", err)
	}
	if int64(len(stored)) != artifact.storedBytes {
		return nil, artifactMetadata{}, fmt.Errorf("Parquet stored size mismatch: got %d, want %d", len(stored), artifact.storedBytes)
	}
	hash := sha256.Sum256(stored)
	actualSHA256 := hex.EncodeToString(hash[:])
	if actualSHA256 != artifact.sha256 {
		return nil, artifactMetadata{}, fmt.Errorf("Parquet stored SHA-256 mismatch: got %s, want %s", actualSHA256, artifact.sha256)
	}
	if afterVerifiedSnapshotForTest != nil {
		afterVerifiedSnapshotForTest()
	}
	return stored, artifact, nil
}

func validateArtifact(artifact artifactMetadata) (string, error) {
	if artifact.schemaVersion != SchemaVersion {
		return "", fmt.Errorf("unsupported Parquet artifact schema %q", artifact.schemaVersion)
	}
	if artifact.count < 0 || artifact.storedBytes < 0 {
		return "", fmt.Errorf("Parquet artifact size and count must be non-negative")
	}
	sha, err := hex.DecodeString(artifact.sha256)
	if err != nil || len(sha) != sha256.Size {
		return "", fmt.Errorf("Parquet artifact SHA-256 is invalid: %q", artifact.sha256)
	}
	path, err := cleanRelativePath(artifact.path)
	if err != nil {
		return "", err
	}
	return path, nil
}

func cleanRelativePath(path string) (string, error) {
	if path == "" || strings.Contains(path, "\\") {
		return "", fmt.Errorf("Parquet artifact path must be a slash-separated relative file: %q", path)
	}
	clean := filepath.Clean(filepath.FromSlash(path))
	if filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("Parquet artifact path escapes collection: %q", path)
	}
	cleanSlash := filepath.ToSlash(clean)
	if cleanSlash != path {
		return "", fmt.Errorf("Parquet artifact path is not clean: %q", path)
	}
	return cleanSlash, nil
}

// readStoredSnapshot is the best portable local-filesystem guard available
// here: it rejects symlinks and non-directory intermediate components before
// opening the final regular file, then checks that the opened file is the one
// inspected. These pathname checks cannot make concurrent directory-entry
// replacement atomic; the collection layer must also enforce safe storage.
func readStoredSnapshot(root, relativePath string) ([]byte, error) {
	if root == "" {
		root = "."
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect Parquet collection root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("Parquet collection root is not a non-symlink directory: %q", root)
	}

	components := strings.Split(filepath.FromSlash(relativePath), string(filepath.Separator))
	path := root
	for index, component := range components {
		path = filepath.Join(path, component)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("inspect Parquet artifact path component %q: %w", component, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("Parquet artifact path contains symlink component %q", component)
		}
		if index < len(components)-1 {
			if !info.IsDir() {
				return nil, fmt.Errorf("Parquet artifact path component %q is not a directory", component)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("Parquet artifact path is not a regular file: %q", relativePath)
		}

		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open Parquet artifact: %w", err)
		}
		openedInfo, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("inspect open Parquet artifact: %w", statErr)
		}
		if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
			_ = file.Close()
			return nil, fmt.Errorf("Parquet artifact changed while opening: %q", relativePath)
		}
		stored, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read open Parquet artifact: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close Parquet artifact: %w", closeErr)
		}
		return stored, nil
	}
	return nil, fmt.Errorf("Parquet artifact path is empty")
}

func decodeParquet[Row, Value any](stored []byte, artifact artifactMetadata, convert func(Row) (Value, error)) ([]Value, error) {
	input := bytes.NewReader(stored)
	file, err := parquetgo.OpenFile(input, int64(len(stored)))
	if err != nil {
		return nil, fmt.Errorf("open Parquet artifact: %w", err)
	}
	wantSchema := parquetgo.SchemaOf(new(Row))
	if !parquetgo.EqualNodes(file.Schema(), wantSchema) {
		return nil, fmt.Errorf("Parquet row schema does not match %s", wantSchema.Name())
	}
	if file.NumRows() != artifact.count {
		return nil, fmt.Errorf("Parquet row count mismatch: got %d, want %d", file.NumRows(), artifact.count)
	}

	reader := parquetgo.NewGenericReader[Row](file)
	rows := make([]Row, 256)
	values := make([]Value, 0)
	var count int64
	for {
		read, readErr := reader.Read(rows)
		for index := range read {
			count++
			value, err := convert(rows[index])
			if err != nil {
				_ = reader.Close()
				return nil, fmt.Errorf("validate Parquet row %d: %w", count, err)
			}
			values = append(values, value)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("read Parquet row %d: %w", count+1, readErr)
		}
	}
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("close Parquet reader: %w", err)
	}
	if count != artifact.count {
		return nil, fmt.Errorf("Parquet row count mismatch: got %d, want %d", count, artifact.count)
	}
	return values, nil
}
