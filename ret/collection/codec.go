package collection

import (
	"errors"
	"fmt"
	"os"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
)

const codecReadBatchSize = 256

func ReadJSONLNodes(root string, artifact JSONLArtifact, visit func(entity.Node) error) error {
	file, err := openArtifact(root, artifact.Path)
	if err != nil {
		return err
	}
	reader, err := jsonl.NewNodeReader(file, artifact.Artifact)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	readErr := visitReader(&reader, visit)
	resultErr := reader.Result()
	closeReaderErr := reader.Close()
	closeFileErr := file.Close()
	return errors.Join(readErr, resultErr, closeReaderErr, closeFileErr)
}

func ReadJSONLRelationships(root string, artifact JSONLArtifact, visit func(entity.Relationship) error) error {
	file, err := openArtifact(root, artifact.Path)
	if err != nil {
		return err
	}
	reader, err := jsonl.NewRelationshipReader(file, artifact.Artifact)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	readErr := visitReader(&reader, visit)
	resultErr := reader.Result()
	closeReaderErr := reader.Close()
	closeFileErr := file.Close()
	return errors.Join(readErr, resultErr, closeReaderErr, closeFileErr)
}

func ReadParquetNodes(root string, artifact ParquetArtifact, visit func(entity.Node) error) error {
	file, size, err := openRandomAccessArtifact(root, artifact.Path)
	if err != nil {
		return err
	}
	reader, err := parquet.NewNodeReader(file, size, artifact.Artifact)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	readErr := visitReader(&reader, visit)
	resultErr := reader.Result()
	closeReaderErr := reader.Close()
	closeFileErr := file.Close()
	return errors.Join(readErr, resultErr, closeReaderErr, closeFileErr)
}

func ReadParquetRelationships(root string, artifact ParquetArtifact, visit func(entity.Relationship) error) error {
	file, size, err := openRandomAccessArtifact(root, artifact.Path)
	if err != nil {
		return err
	}
	reader, err := parquet.NewRelationshipReader(file, size, artifact.Artifact)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	readErr := visitReader(&reader, visit)
	resultErr := reader.Result()
	closeReaderErr := reader.Close()
	closeFileErr := file.Close()
	return errors.Join(readErr, resultErr, closeReaderErr, closeFileErr)
}

type pullReader[E any] interface {
	Pull(int) ([]E, error)
	Done() bool
}

func visitReader[E any](reader pullReader[E], visit func(E) error) error {
	var index int64
	for !reader.Done() {
		batch, err := reader.Pull(codecReadBatchSize)
		if err != nil {
			return err
		}
		for _, value := range batch {
			index++
			if visit != nil {
				if err := visit(value); err != nil {
					return fmt.Errorf("visit record %d: %w", index, err)
				}
			}
		}
	}
	return nil
}

func openArtifact(root, relative string) (*os.File, error) {
	if root == "" {
		root = "."
	}
	if err := inspectNonSymlinkArtifact(root, relative); err != nil {
		return nil, err
	}
	path, err := SafeJoin(root, relative)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open artifact: %w", err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("reinspect artifact: %w", err), file.Close())
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect open artifact: %w", err), file.Close())
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return nil, errors.Join(fmt.Errorf("artifact changed while opening: %q", relative), file.Close())
	}
	return file, nil
}

func openRandomAccessArtifact(root, relative string) (*os.File, int64, error) {
	file, err := openArtifact(root, relative)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, errors.Join(fmt.Errorf("inspect open artifact: %w", err), file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, 0, errors.Join(fmt.Errorf("artifact is not a regular file: %q", relative), file.Close())
	}
	return file, info.Size(), nil
}
