package retriever

import (
	"context"
	"errors"
	"fmt"
)

func collectErrors(errs ...error) error {
	return errors.Join(errs...)
}

func cleanupOnError(primary error, cleanups ...func() error) error {
	errs := make([]error, 1, len(cleanups)+1)
	errs[0] = primary
	for _, cleanup := range cleanups {
		errs = append(errs, cleanup())
	}
	return collectErrors(errs...)
}

type fragmentMetadata interface {
	rowCount() int
}

type fragmentSink[T any, M fragmentMetadata] interface {
	Open(context.Context, shardID) (fragmentWriter[T, M], error)
}

type fragmentWriter[T any, M fragmentMetadata] interface {
	WriteBatch(context.Context, []T) error
	Prepare(context.Context) (preparedFragment[M], error)
	Abort() error
}

type preparedFragment[M fragmentMetadata] interface {
	Metadata() M
	Commit(context.Context) error
	Abort() error
}

func writeFragment[T any, M fragmentMetadata](ctx context.Context, sink fragmentSink[T, M], summary shardSummary, records []T) (M, error) {
	var empty M

	writer, err := sink.Open(ctx, summary.ID)
	if err != nil {
		return empty, err
	}
	if err := writer.WriteBatch(ctx, records); err != nil {
		return empty, cleanupOnError(err, writer.Abort)
	}

	prepared, err := writer.Prepare(ctx)
	if err != nil {
		return empty, cleanupOnError(err, writer.Abort)
	}
	metadata := prepared.Metadata()
	if metadata.rowCount() != summary.Rows {
		err := fmt.Errorf("prepared shard %d has %d rows, expected %d", summary.ID.Number, metadata.rowCount(), summary.Rows)
		return empty, cleanupOnError(err, prepared.Abort)
	}
	if err := prepared.Commit(ctx); err != nil {
		return empty, cleanupOnError(err, prepared.Abort)
	}

	return metadata, nil
}
