package ret

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
)

type artifactWriter[E, A any] interface {
	Push([]E) error
	Close() error
	Result() (A, error)
}

func writeArtifactFile[E, A any](
	temporary string,
	values []E,
	newWriter func(io.Writer) (artifactWriter[E, A], error),
) (A, error) {
	var zero A
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return zero, fmt.Errorf("open temporary artifact: %w", err)
	}
	writer, err := newWriter(file)
	if err != nil {
		return zero, errors.Join(err, file.Close())
	}
	pushErr := writer.Push(values)
	closeWriterErr := writer.Close()
	var artifact A
	var resultErr error
	if pushErr == nil && closeWriterErr == nil {
		artifact, resultErr = writer.Result()
	}
	closeFileErr := file.Close()
	if err := errors.Join(pushErr, closeWriterErr, resultErr, closeFileErr); err != nil {
		return zero, err
	}
	return artifact, nil
}

func writeJSONLNodeFile(temporary, relative string, config jsonl.Config, values []entity.Node) (collection.JSONLArtifact, error) {
	artifact, err := writeArtifactFile(temporary, values, func(output io.Writer) (artifactWriter[entity.Node, jsonl.Artifact], error) {
		writer, err := jsonl.NewNodeWriter(output, config)
		return &writer, err
	})
	if err != nil {
		return collection.JSONLArtifact{}, err
	}
	return collection.JSONLArtifact{Path: relative, Artifact: artifact}, nil
}

func writeJSONLRelationshipFile(temporary, relative string, config jsonl.Config, values []entity.Relationship) (collection.JSONLArtifact, error) {
	artifact, err := writeArtifactFile(temporary, values, func(output io.Writer) (artifactWriter[entity.Relationship, jsonl.Artifact], error) {
		writer, err := jsonl.NewRelationshipWriter(output, config)
		return &writer, err
	})
	if err != nil {
		return collection.JSONLArtifact{}, err
	}
	return collection.JSONLArtifact{Path: relative, Artifact: artifact}, nil
}

func writeParquetNodeFile(temporary, relative string, config parquet.Config, values []entity.Node) (collection.ParquetArtifact, error) {
	artifact, err := writeArtifactFile(temporary, values, func(output io.Writer) (artifactWriter[entity.Node, parquet.Artifact], error) {
		writer, err := parquet.NewNodeWriter(output, config)
		return &writer, err
	})
	if err != nil {
		return collection.ParquetArtifact{}, err
	}
	return collection.ParquetArtifact{Path: relative, Artifact: artifact}, nil
}

func writeParquetRelationshipFile(temporary, relative string, config parquet.Config, values []entity.Relationship) (collection.ParquetArtifact, error) {
	artifact, err := writeArtifactFile(temporary, values, func(output io.Writer) (artifactWriter[entity.Relationship, parquet.Artifact], error) {
		writer, err := parquet.NewRelationshipWriter(output, config)
		return &writer, err
	})
	if err != nil {
		return collection.ParquetArtifact{}, err
	}
	return collection.ParquetArtifact{Path: relative, Artifact: artifact}, nil
}
