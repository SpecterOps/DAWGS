package retriever

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type recordingJSONLSinkState[T any] struct {
	fragments []*recordingJSONLFragmentState[T]
	rowOffset int
	writeErr  error
}

type recordingJSONLFragmentState[T any] struct {
	id              shardID
	batches         [][]T
	prepared        bool
	committed       bool
	writerAborted   bool
	preparedAborted bool
}

type recordingJSONLFragmentSink[T any] struct {
	state *recordingJSONLSinkState[T]
}

func (s recordingJSONLFragmentSink[T]) Open(_ context.Context, id shardID) (fragmentWriter[T, jsonlFragmentMetadata], error) {
	fragment := &recordingJSONLFragmentState[T]{id: id}
	s.state.fragments = append(s.state.fragments, fragment)
	return &recordingJSONLFragmentWriter[T]{sink: s.state, fragment: fragment}, nil
}

type recordingJSONLFragmentWriter[T any] struct {
	sink     *recordingJSONLSinkState[T]
	fragment *recordingJSONLFragmentState[T]
}

func (s *recordingJSONLFragmentWriter[T]) WriteBatch(_ context.Context, records []T) error {
	if s.sink.writeErr != nil {
		return s.sink.writeErr
	}
	s.fragment.batches = append(s.fragment.batches, append([]T(nil), records...))
	return nil
}

func (s *recordingJSONLFragmentWriter[T]) Prepare(context.Context) (preparedFragment[jsonlFragmentMetadata], error) {
	s.fragment.prepared = true
	return &recordingPreparedJSONLFragment[T]{
		fragment: s.fragment,
		metadata: jsonlFragmentMetadata{
			Path: "fragment.jsonl.gz",
			Rows: countBatchRows(s.fragment.batches) + s.sink.rowOffset,
		},
	}, nil
}

func (s *recordingJSONLFragmentWriter[T]) Abort() error {
	s.fragment.writerAborted = true
	return nil
}

type recordingPreparedJSONLFragment[T any] struct {
	fragment *recordingJSONLFragmentState[T]
	metadata jsonlFragmentMetadata
}

func (s *recordingPreparedJSONLFragment[T]) Metadata() jsonlFragmentMetadata {
	return s.metadata
}

func (s *recordingPreparedJSONLFragment[T]) Commit(context.Context) error {
	s.fragment.committed = true
	return nil
}

func (s *recordingPreparedJSONLFragment[T]) Abort() error {
	s.fragment.preparedAborted = true
	return nil
}

func TestJSONLShardOutputStreamsLogicalShardSlices(t *testing.T) {
	state := &recordingJSONLSinkState[int]{}
	output := newShardSinkSet(newJSONLShardSink(recordingJSONLFragmentSink[int]{state: state}))
	var committed []shardSummary
	receiver := newShardOutputReceiver(context.Background(), output, func(summary shardSummary, result committedShard) error {
		if result.JSONL.Rows != summary.Rows {
			t.Fatalf("committed metadata = %+v, summary = %+v", result.JSONL, summary)
		}
		committed = append(committed, summary)
		return nil
	})
	sharder, err := newLogicalSharder[int]("example", PhaseNodes, 3)
	if err != nil {
		t.Fatalf("new sharder: %v", err)
	}

	first := transformedBatch[int]{Records: []int{1, 2}, ActionCounts: make([]map[string]int, 2)}
	if err := sharder.Add(first, receiver); err != nil {
		t.Fatalf("add first batch: %v", err)
	}
	if len(state.fragments) != 1 || !reflect.DeepEqual(state.fragments[0].batches, [][]int{{1, 2}}) || state.fragments[0].prepared {
		t.Fatalf("first batch was not streamed into the active writer: %+v", state.fragments)
	}

	second := transformedBatch[int]{Records: []int{3, 4}, ActionCounts: make([]map[string]int, 2)}
	if err := sharder.Add(second, receiver); err != nil {
		t.Fatalf("add second batch: %v", err)
	}
	if err := sharder.Flush(receiver); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if len(state.fragments) != 2 || len(committed) != 2 {
		t.Fatalf("fragments=%d committed=%d", len(state.fragments), len(committed))
	}
	if !reflect.DeepEqual(state.fragments[0].batches, [][]int{{1, 2}, {3}}) || !reflect.DeepEqual(state.fragments[1].batches, [][]int{{4}}) {
		t.Fatalf("fragment batches = %+v", state.fragments)
	}
	for index, fragment := range state.fragments {
		if fragment.id.Number != index+1 || !fragment.prepared || !fragment.committed || fragment.writerAborted || fragment.preparedAborted {
			t.Fatalf("fragment %d lifecycle = %+v", index, fragment)
		}
	}
}

func TestJSONLShardOutputAbortsPreparedRowCountMismatch(t *testing.T) {
	state := &recordingJSONLSinkState[int]{rowOffset: 1}
	output := newShardSinkSet(newJSONLShardSink(recordingJSONLFragmentSink[int]{state: state}))
	id := shardID{Graph: "example", Phase: PhaseNodes, Number: 1}
	writer, err := output.OpenShard(context.Background(), id)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	if err := writer.WriteBatch(context.Background(), []int{1}); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	_, err = writer.Finish(context.Background(), shardSummary{ID: id, Rows: 1})
	assertShardSinkOperation(t, err, jsonlFragmentFormat, id, "validate", nil)

	fragment := state.fragments[0]
	if !fragment.prepared || !fragment.preparedAborted || fragment.committed || fragment.writerAborted {
		t.Fatalf("mismatch lifecycle = %+v", fragment)
	}
}

func assertShardSinkOperation(t *testing.T, err error, format string, id shardID, operation string, cause error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s %s error", format, operation)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("%s %s error = %v, want cause %v", format, operation, err, cause)
	}

	if !containsShardSinkOperation(err, format, id, operation) {
		t.Fatalf("error lacks %s %s context for %+v: %v", format, operation, id, err)
	}
}

func containsShardSinkOperation(err error, format string, id shardID, operation string) bool {
	if err == nil {
		return false
	}
	if operationErr, ok := err.(*shardSinkOperationError); ok && operationErr.Format == format && operationErr.ID == id && operationErr.Operation == operation {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if containsShardSinkOperation(child, format, id, operation) {
				return true
			}
		}
		return false
	}
	return containsShardSinkOperation(errors.Unwrap(err), format, id, operation)
}

func TestShardOutputReceiverAbortsAfterWriteAndUpstreamFailures(t *testing.T) {
	t.Run("write failure", func(t *testing.T) {
		writeErr := errors.New("write failed")
		state := &recordingJSONLSinkState[int]{writeErr: writeErr}
		receiver := newShardOutputReceiver(context.Background(), newShardSinkSet(newJSONLShardSink(recordingJSONLFragmentSink[int]{state: state})), func(shardSummary, committedShard) error {
			return nil
		})
		if err := receiver.BeginShard(shardID{Graph: "example", Phase: PhaseNodes, Number: 1}); err != nil {
			t.Fatalf("begin shard: %v", err)
		}
		if err := receiver.WriteBatch([]int{1}); !errors.Is(err, writeErr) {
			t.Fatalf("write error = %v", err)
		}
		if !state.fragments[0].writerAborted {
			t.Fatalf("writer was not aborted after write failure")
		}
	})

	t.Run("upstream failure", func(t *testing.T) {
		state := &recordingJSONLSinkState[int]{}
		receiver := newShardOutputReceiver(context.Background(), newShardSinkSet(newJSONLShardSink(recordingJSONLFragmentSink[int]{state: state})), func(shardSummary, committedShard) error {
			return nil
		})
		if err := receiver.BeginShard(shardID{Graph: "example", Phase: PhaseNodes, Number: 1}); err != nil {
			t.Fatalf("begin shard: %v", err)
		}
		if err := receiver.WriteBatch([]int{1}); err != nil {
			t.Fatalf("write batch: %v", err)
		}
		if err := receiver.Abort(); err != nil {
			t.Fatalf("abort receiver: %v", err)
		}
		if !state.fragments[0].writerAborted {
			t.Fatalf("writer was not aborted after upstream failure")
		}
	})
}
