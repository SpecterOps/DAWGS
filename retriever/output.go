package retriever

import (
	"context"
	"errors"
	"fmt"
)

type committedShard struct {
	JSONL jsonlFragmentMetadata
}

type shardOutput[T any] interface {
	OpenShard(context.Context, shardID) (shardOutputWriter[T], error)
}

type shardOutputWriter[T any] interface {
	WriteBatch(context.Context, []T) error
	Finish(context.Context, shardSummary) (committedShard, error)
	Abort() error
}

type jsonlShardOutput[T any] struct {
	sink fragmentSink[T, jsonlFragmentMetadata]
}

func newJSONLShardOutput[T any](sink fragmentSink[T, jsonlFragmentMetadata]) jsonlShardOutput[T] {
	return jsonlShardOutput[T]{sink: sink}
}

func (s jsonlShardOutput[T]) OpenShard(ctx context.Context, id shardID) (shardOutputWriter[T], error) {
	writer, err := s.sink.Open(ctx, id)
	if err != nil {
		return nil, err
	}
	return &jsonlShardOutputWriter[T]{id: id, writer: writer}, nil
}

type jsonlShardOutputWriter[T any] struct {
	id     shardID
	writer fragmentWriter[T, jsonlFragmentMetadata]
}

func (s *jsonlShardOutputWriter[T]) WriteBatch(ctx context.Context, records []T) error {
	return s.writer.WriteBatch(ctx, records)
}

func (s *jsonlShardOutputWriter[T]) Finish(ctx context.Context, summary shardSummary) (committedShard, error) {
	if summary.ID != s.id {
		err := fmt.Errorf("finish shard %d while shard %d is active", summary.ID.Number, s.id.Number)
		return committedShard{}, errors.Join(err, s.writer.Abort())
	}

	prepared, err := s.writer.Prepare(ctx)
	if err != nil {
		return committedShard{}, errors.Join(err, s.writer.Abort())
	}
	metadata := prepared.Metadata()
	if metadata.rowCount() != summary.Rows {
		err := fmt.Errorf("prepared shard %d has %d rows, expected %d", summary.ID.Number, metadata.rowCount(), summary.Rows)
		return committedShard{}, errors.Join(err, prepared.Abort())
	}
	if err := prepared.Commit(ctx); err != nil {
		return committedShard{}, errors.Join(err, prepared.Abort())
	}

	return committedShard{JSONL: metadata}, nil
}

func (s *jsonlShardOutputWriter[T]) Abort() error {
	return s.writer.Abort()
}

type shardOutputReceiver[T any] struct {
	ctx    context.Context
	output shardOutput[T]
	accept func(shardSummary, committedShard) error
	writer shardOutputWriter[T]
}

func newShardOutputReceiver[T any](ctx context.Context, output shardOutput[T], accept func(shardSummary, committedShard) error) *shardOutputReceiver[T] {
	return &shardOutputReceiver[T]{ctx: ctx, output: output, accept: accept}
}

func (s *shardOutputReceiver[T]) BeginShard(id shardID) error {
	if s.writer != nil {
		return fmt.Errorf("begin shard %d while another shard is active", id.Number)
	}

	writer, err := s.output.OpenShard(s.ctx, id)
	if err != nil {
		return err
	}
	s.writer = writer
	return nil
}

func (s *shardOutputReceiver[T]) WriteBatch(records []T) error {
	if s.writer == nil {
		return fmt.Errorf("write shard batch without an active shard")
	}
	if err := s.writer.WriteBatch(s.ctx, records); err != nil {
		cleanupErr := s.writer.Abort()
		s.writer = nil
		return errors.Join(err, cleanupErr)
	}
	return nil
}

func (s *shardOutputReceiver[T]) FinishShard(summary shardSummary) error {
	if s.writer == nil {
		return fmt.Errorf("finish shard %d without an active shard", summary.ID.Number)
	}

	committed, err := s.writer.Finish(s.ctx, summary)
	s.writer = nil
	if err != nil {
		return err
	}
	return s.accept(summary, committed)
}

func (s *shardOutputReceiver[T]) Abort() error {
	if s.writer == nil {
		return nil
	}
	err := s.writer.Abort()
	s.writer = nil
	return err
}
