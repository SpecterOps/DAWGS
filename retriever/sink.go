package retriever

import (
	"context"
	"errors"
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
