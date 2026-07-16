package retriever

import (
	"context"
	"fmt"
)

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
		_ = writer.Abort()
		return empty, err
	}

	prepared, err := writer.Prepare(ctx)
	if err != nil {
		_ = writer.Abort()
		return empty, err
	}
	metadata := prepared.Metadata()
	if metadata.rowCount() != summary.Rows {
		_ = prepared.Abort()
		return empty, fmt.Errorf("prepared shard %d has %d rows, expected %d", summary.ID.Number, metadata.rowCount(), summary.Rows)
	}
	if err := prepared.Commit(ctx); err != nil {
		_ = prepared.Abort()
		return empty, err
	}

	return metadata, nil
}
