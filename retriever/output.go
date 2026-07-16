package retriever

import (
	"context"
	"fmt"
)

type committedShard struct {
	JSONL   jsonlFragmentMetadata
	Parquet *parquetFragmentMetadata
}

type shardOutput[T any] interface {
	OpenShard(context.Context, shardID) (shardOutputWriter[T], error)
}

type shardOutputWriter[T any] interface {
	WriteBatch(context.Context, []T) error
	Finish(context.Context, shardSummary) (committedShard, error)
	Abort() error
}

type shardSink[T any] interface {
	format() string
	open(context.Context, shardID) (shardSinkWriter[T], error)
}

type shardSinkWriter[T any] interface {
	writeBatch(context.Context, []T) error
	prepare(context.Context) (preparedShardSink, error)
	abort() error
}

type preparedShardSink interface {
	rowCount() int
	commit(context.Context) error
	abort() error
	addTo(*committedShard)
}

type typedShardSink[T any, M fragmentMetadata] struct {
	name    string
	sink    fragmentSink[T, M]
	collect func(*committedShard, M)
}

func newShardSink[T any, M fragmentMetadata](name string, sink fragmentSink[T, M], collect func(*committedShard, M)) shardSink[T] {
	return typedShardSink[T, M]{name: name, sink: sink, collect: collect}
}

func newJSONLShardSink[T any](sink fragmentSink[T, jsonlFragmentMetadata]) shardSink[T] {
	return newShardSink(jsonlFragmentFormat, sink, func(committed *committedShard, metadata jsonlFragmentMetadata) {
		committed.JSONL = metadata
	})
}

func newParquetShardSink[T any](sink fragmentSink[T, parquetFragmentMetadata]) shardSink[T] {
	return newShardSink(parquetFragmentFormat, sink, func(committed *committedShard, metadata parquetFragmentMetadata) {
		committed.Parquet = &metadata
	})
}

func (s typedShardSink[T, M]) format() string {
	return s.name
}

func (s typedShardSink[T, M]) open(ctx context.Context, id shardID) (shardSinkWriter[T], error) {
	writer, err := s.sink.Open(ctx, id)
	if err != nil {
		return nil, err
	}
	return typedShardSinkWriter[T, M]{writer: writer, collect: s.collect}, nil
}

type typedShardSinkWriter[T any, M fragmentMetadata] struct {
	writer  fragmentWriter[T, M]
	collect func(*committedShard, M)
}

func (s typedShardSinkWriter[T, M]) writeBatch(ctx context.Context, records []T) error {
	return s.writer.WriteBatch(ctx, records)
}

func (s typedShardSinkWriter[T, M]) prepare(ctx context.Context) (preparedShardSink, error) {
	prepared, err := s.writer.Prepare(ctx)
	if err != nil {
		return nil, err
	}
	return typedPreparedShardSink[M]{prepared: prepared, collect: s.collect}, nil
}

func (s typedShardSinkWriter[T, M]) abort() error {
	return s.writer.Abort()
}

type typedPreparedShardSink[M fragmentMetadata] struct {
	prepared preparedFragment[M]
	collect  func(*committedShard, M)
}

func (s typedPreparedShardSink[M]) rowCount() int {
	return s.prepared.Metadata().rowCount()
}

func (s typedPreparedShardSink[M]) commit(ctx context.Context) error {
	return s.prepared.Commit(ctx)
}

func (s typedPreparedShardSink[M]) abort() error {
	return s.prepared.Abort()
}

func (s typedPreparedShardSink[M]) addTo(committed *committedShard) {
	s.collect(committed, s.prepared.Metadata())
}

type shardSinkSet[T any] struct {
	sinks []shardSink[T]
}

func newShardSinkSet[T any](sinks ...shardSink[T]) shardSinkSet[T] {
	return shardSinkSet[T]{sinks: sinks}
}

func (s shardSinkSet[T]) OpenShard(ctx context.Context, id shardID) (shardOutputWriter[T], error) {
	if len(s.sinks) == 0 {
		return nil, fmt.Errorf("open shard %d without a sink", id.Number)
	}

	writer := &shardSinkSetWriter[T]{id: id, sinks: make([]activeShardSink[T], len(s.sinks))}
	for index, sink := range s.sinks {
		writer.sinks[index].format = sink.format()
	}
	errs := runSinkOperations(ctx, len(s.sinks), func(index int, operationCtx context.Context) error {
		opened, err := s.sinks[index].open(operationCtx, id)
		writer.sinks[index].writer = opened
		return writer.failure(index, "open", err)
	})
	if err := collectErrors(errs...); err != nil {
		return nil, cleanupOnError(err, writer.Abort)
	}
	return writer, nil
}

type activeShardSink[T any] struct {
	format    string
	writer    shardSinkWriter[T]
	prepared  preparedShardSink
	committed bool
}

type shardSinkOperationError struct {
	Format    string
	ID        shardID
	Operation string
	Err       error
}

func (s *shardSinkOperationError) Error() string {
	return fmt.Sprintf("%s sink graph %q phase %q shard %d operation %q: %v", s.Format, s.ID.Graph, s.ID.Phase, s.ID.Number, s.Operation, s.Err)
}

func (s *shardSinkOperationError) Unwrap() error {
	return s.Err
}

type shardSinkSetWriter[T any] struct {
	id     shardID
	sinks  []activeShardSink[T]
	closed bool
}

func (s *shardSinkSetWriter[T]) WriteBatch(ctx context.Context, records []T) error {
	if s.closed {
		return fmt.Errorf("write shard %d after output closed", s.id.Number)
	}
	errs := runSinkOperations(ctx, len(s.sinks), func(index int, operationCtx context.Context) error {
		return s.failure(index, "write", s.sinks[index].writer.writeBatch(operationCtx, records))
	})
	if err := collectErrors(errs...); err != nil {
		return cleanupOnError(err, s.Abort)
	}
	return nil
}

func (s *shardSinkSetWriter[T]) Finish(ctx context.Context, summary shardSummary) (committedShard, error) {
	if s.closed {
		return committedShard{}, fmt.Errorf("finish shard %d after output closed", s.id.Number)
	}
	if summary.ID != s.id {
		err := fmt.Errorf("finish shard %d while shard %d is active", summary.ID.Number, s.id.Number)
		return committedShard{}, cleanupOnError(s.allFailures("validate", err), s.Abort)
	}

	errs := runSinkOperations(ctx, len(s.sinks), func(index int, operationCtx context.Context) error {
		prepared, err := s.sinks[index].writer.prepare(operationCtx)
		s.sinks[index].prepared = prepared
		return s.failure(index, "prepare", err)
	})
	if err := collectErrors(errs...); err != nil {
		return committedShard{}, cleanupOnError(err, s.Abort)
	}

	if err := s.validate(summary); err != nil {
		return committedShard{}, cleanupOnError(err, s.Abort)
	}

	for index := range s.sinks {
		if err := s.sinks[index].prepared.commit(ctx); err != nil {
			return committedShard{}, cleanupOnError(s.failure(index, "commit", err), s.Abort)
		}
		s.sinks[index].committed = true
	}

	var committed committedShard
	for _, sink := range s.sinks {
		sink.prepared.addTo(&committed)
	}
	s.closed = true
	return committed, nil
}

func (s *shardSinkSetWriter[T]) validate(summary shardSummary) error {
	errs := make([]error, 0, len(s.sinks)*2)
	peerRows := s.sinks[0].prepared.rowCount()
	for index, sink := range s.sinks {
		rows := sink.prepared.rowCount()
		if rows != summary.Rows {
			err := fmt.Errorf("prepared shard %d has %d rows, expected %d", summary.ID.Number, rows, summary.Rows)
			errs = append(errs, s.failure(index, "validate", err))
		}
		if index > 0 && rows != peerRows {
			err := fmt.Errorf("prepared shard %d has %d rows, first sink has %d", summary.ID.Number, rows, peerRows)
			errs = append(errs, s.failure(index, "validate", err))
		}
	}
	return collectErrors(errs...)
}

func (s *shardSinkSetWriter[T]) Abort() error {
	if s.closed {
		return nil
	}
	s.closed = true

	errs := make([]error, 0, len(s.sinks))
	for index, sink := range s.sinks {
		if sink.committed || sink.writer == nil {
			continue
		}
		if sink.prepared != nil {
			errs = append(errs, s.failure(index, "abort", sink.prepared.abort()))
		} else {
			errs = append(errs, s.failure(index, "abort", sink.writer.abort()))
		}
	}
	return collectErrors(errs...)
}

func (s *shardSinkSetWriter[T]) failure(index int, operation string, err error) error {
	if err == nil {
		return nil
	}
	return &shardSinkOperationError{Format: s.sinks[index].format, ID: s.id, Operation: operation, Err: err}
}

func (s *shardSinkSetWriter[T]) allFailures(operation string, err error) error {
	errs := make([]error, len(s.sinks))
	for index := range s.sinks {
		errs[index] = s.failure(index, operation, err)
	}
	return collectErrors(errs...)
}

type sinkOperationResult struct {
	index int
	err   error
}

func runSinkOperations(ctx context.Context, count int, operation func(int, context.Context) error) []error {
	if count == 1 {
		return []error{operation(0, ctx)}
	}

	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan sinkOperationResult, count)
	for index := range count {
		go func() {
			results <- sinkOperationResult{index: index, err: operation(index, operationCtx)}
		}()
	}

	errs := make([]error, count)
	for range count {
		result := <-results
		errs[result.index] = result.err
		if result.err != nil {
			cancel()
		}
	}
	return errs
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
		writer := s.writer
		s.writer = nil
		return cleanupOnError(err, writer.Abort)
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
