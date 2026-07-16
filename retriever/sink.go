package retriever

import "context"

type fragmentSink[T, M any] interface {
	Open(context.Context, shardSpec) (fragmentWriter[T, M], error)
}

type fragmentWriter[T, M any] interface {
	WriteBatch(context.Context, []T) error
	Prepare(context.Context) (preparedFragment[M], error)
	Abort() error
}

type preparedFragment[M any] interface {
	Metadata() M
	Commit(context.Context) error
	Abort() error
}

func writeFragment[T, M any](ctx context.Context, sink fragmentSink[T, M], spec shardSpec, records []T) (M, error) {
	var empty M

	writer, err := sink.Open(ctx, spec)
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
	if err := prepared.Commit(ctx); err != nil {
		_ = prepared.Abort()
		return empty, err
	}

	return metadata, nil
}
