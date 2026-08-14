package parquet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/specterops/dawgs/ret/entity"
)

type nodeFixtureArtifact struct {
	SchemaVersion, Path, SHA256 string
	Count, StoredBytes          int64
}

type relationshipFixtureArtifact struct {
	SchemaVersion, Path, SHA256 string
	Count, StoredBytes          int64
}

var afterVerifiedSnapshotForTest func()

func writeNodesFixture(temporary, relative string, config Config, values []entity.Node) (nodeFixtureArtifact, error) {
	path, err := fixturePath(temporary, relative)
	if err != nil {
		return nodeFixtureArtifact{}, err
	}
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nodeFixtureArtifact{}, err
	}
	writer, err := NewNodeWriter(file, config)
	if err != nil {
		return nodeFixtureArtifact{}, errors.Join(err, file.Close())
	}
	pushErr := writer.Push(values)
	closeWriterErr := writer.Close()
	artifact, resultErr := writer.Result()
	closeFileErr := file.Close()
	if err := errors.Join(pushErr, closeWriterErr, resultErr, closeFileErr); err != nil {
		return nodeFixtureArtifact{}, err
	}
	return nodeFixtureArtifact{artifact.SchemaVersion, path, artifact.SHA256, artifact.Count, artifact.StoredBytes}, nil
}

func writeRelationshipsFixture(temporary, relative string, config Config, values []entity.Relationship) (relationshipFixtureArtifact, error) {
	path, err := fixturePath(temporary, relative)
	if err != nil {
		return relationshipFixtureArtifact{}, err
	}
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return relationshipFixtureArtifact{}, err
	}
	writer, err := NewRelationshipWriter(file, config)
	if err != nil {
		return relationshipFixtureArtifact{}, errors.Join(err, file.Close())
	}
	pushErr := writer.Push(values)
	closeWriterErr := writer.Close()
	artifact, resultErr := writer.Result()
	closeFileErr := file.Close()
	if err := errors.Join(pushErr, closeWriterErr, resultErr, closeFileErr); err != nil {
		return relationshipFixtureArtifact{}, err
	}
	return relationshipFixtureArtifact{artifact.SchemaVersion, path, artifact.SHA256, artifact.Count, artifact.StoredBytes}, nil
}

func readNodesFixture(root string, fixture nodeFixtureArtifact, visit func(entity.Node) error) error {
	file, size, err := openFixture(root, fixture.Path)
	if err != nil {
		return err
	}
	reader, err := NewNodeReader(file, size, Artifact{fixture.SchemaVersion, fixture.SHA256, fixture.Count, fixture.StoredBytes})
	if err != nil {
		return errors.Join(err, file.Close())
	}
	if afterVerifiedSnapshotForTest != nil {
		afterVerifiedSnapshotForTest()
	}
	values, readErr := drainFixtureReader(&reader)
	resultErr := reader.Result()
	closeReaderErr := reader.Close()
	closeFileErr := file.Close()
	if err := errors.Join(readErr, resultErr, closeReaderErr, closeFileErr); err != nil {
		return err
	}
	for _, value := range values {
		if visit != nil {
			if err := visit(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func readRelationshipsFixture(root string, fixture relationshipFixtureArtifact, visit func(entity.Relationship) error) error {
	file, size, err := openFixture(root, fixture.Path)
	if err != nil {
		return err
	}
	reader, err := NewRelationshipReader(file, size, Artifact{fixture.SchemaVersion, fixture.SHA256, fixture.Count, fixture.StoredBytes})
	if err != nil {
		return errors.Join(err, file.Close())
	}
	values, readErr := drainFixtureReader(&reader)
	resultErr := reader.Result()
	closeReaderErr := reader.Close()
	closeFileErr := file.Close()
	if err := errors.Join(readErr, resultErr, closeReaderErr, closeFileErr); err != nil {
		return err
	}
	for _, value := range values {
		if visit != nil {
			if err := visit(value); err != nil {
				return err
			}
		}
	}
	return nil
}

type fixturePullReader[E any] interface {
	Pull(int) ([]E, error)
	Done() bool
}

func drainFixtureReader[E any](reader fixturePullReader[E]) ([]E, error) {
	var values []E
	for !reader.Done() {
		batch, err := reader.Pull(256)
		if err != nil {
			return nil, err
		}
		values = append(values, batch...)
	}
	return values, nil
}

func fixturePath(temporary, relative string) (string, error) {
	if !filepath.IsAbs(temporary) {
		return "", fmt.Errorf("Parquet temporary path must be absolute: %q", temporary)
	}
	if relative == "" || strings.Contains(relative, "\\") {
		return "", fmt.Errorf("Parquet artifact path must be a slash-separated relative file: %q", relative)
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.ToSlash(clean) != relative {
		return "", fmt.Errorf("Parquet artifact path escapes collection: %q", relative)
	}
	return relative, nil
}

func openFixture(root, relative string) (*os.File, int64, error) {
	if _, err := fixturePath(filepath.Join(root, "fixture.tmp"), relative); err != nil {
		return nil, 0, err
	}
	path := root
	for _, component := range strings.Split(filepath.FromSlash(relative), string(filepath.Separator)) {
		path = filepath.Join(path, component)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, 0, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, 0, fmt.Errorf("Parquet artifact path contains symlink component %q", component)
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, errors.Join(err, file.Close())
	}
	return file, info.Size(), nil
}
