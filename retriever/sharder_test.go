package retriever

import (
	"errors"
	"reflect"
	"testing"
)

type recordedLogicalShard[T any] struct {
	id       shardID
	batches  [][]T
	summary  shardSummary
	finished bool
}

type recordingLogicalShardReceiver[T any] struct {
	shards []recordedLogicalShard[T]
}

func (s *recordingLogicalShardReceiver[T]) BeginShard(id shardID) error {
	s.shards = append(s.shards, recordedLogicalShard[T]{id: id})
	return nil
}

func (s *recordingLogicalShardReceiver[T]) WriteBatch(records []T) error {
	index := len(s.shards) - 1
	batch := append([]T(nil), records...)
	s.shards[index].batches = append(s.shards[index].batches, batch)
	return nil
}

func (s *recordingLogicalShardReceiver[T]) FinishShard(summary shardSummary) error {
	index := len(s.shards) - 1
	s.shards[index].summary = summary
	s.shards[index].finished = true
	return nil
}

func TestLogicalSharderStreamsExactBoundaries(t *testing.T) {
	testCases := []struct {
		name            string
		shardSize       int
		batches         [][]int
		expectedBatches [][][]int
	}{
		{name: "empty", shardSize: 2},
		{
			name:            "exact boundaries",
			shardSize:       2,
			batches:         [][]int{{1, 2}, {3, 4}},
			expectedBatches: [][][]int{{{1, 2}}, {{3, 4}}},
		},
		{
			name:            "partial final shard",
			shardSize:       3,
			batches:         [][]int{{1, 2}, {3, 4}},
			expectedBatches: [][][]int{{{1, 2}, {3}}, {{4}}},
		},
		{
			name:            "batch larger than shard",
			shardSize:       2,
			batches:         [][]int{{1, 2, 3, 4, 5}},
			expectedBatches: [][][]int{{{1, 2}}, {{3, 4}}, {{5}}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sharder, err := newLogicalSharder[int]("example", PhaseNodes, testCase.shardSize)
			if err != nil {
				t.Fatalf("new sharder: %v", err)
			}

			receiver := &recordingLogicalShardReceiver[int]{}
			for _, records := range testCase.batches {
				batch := transformedBatch[int]{
					Records:      records,
					ActionCounts: make([]map[string]int, len(records)),
				}
				if err := sharder.Add(batch, receiver); err != nil {
					t.Fatalf("add batch: %v", err)
				}
			}
			if err := sharder.Flush(receiver); err != nil {
				t.Fatalf("flush: %v", err)
			}

			if len(receiver.shards) != len(testCase.expectedBatches) {
				t.Fatalf("shard count = %d, want %d", len(receiver.shards), len(testCase.expectedBatches))
			}
			for index, shard := range receiver.shards {
				if shard.id != (shardID{Graph: "example", Phase: PhaseNodes, Number: index + 1}) {
					t.Fatalf("shard %d ID = %+v", index, shard.id)
				}
				if !shard.finished || shard.summary.ID != shard.id {
					t.Fatalf("shard %d lifecycle = %+v", index, shard)
				}
				if !reflect.DeepEqual(shard.batches, testCase.expectedBatches[index]) {
					t.Fatalf("shard %d batches = %v, want %v", index, shard.batches, testCase.expectedBatches[index])
				}
				if shard.summary.Rows != countBatchRows(shard.batches) {
					t.Fatalf("shard %d summary = %+v", index, shard.summary)
				}
			}
		})
	}
}

func TestLogicalSharderStreamsBeforeFlushWithoutRetainingRecords(t *testing.T) {
	sharder, err := newLogicalSharder[int]("example", PhaseNodes, 1_000_000)
	if err != nil {
		t.Fatalf("new sharder: %v", err)
	}

	receiver := &recordingLogicalShardReceiver[int]{}
	batch := transformedBatch[int]{Records: []int{1, 2}, ActionCounts: make([]map[string]int, 2)}
	if err := sharder.Add(batch, receiver); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	if len(receiver.shards) != 1 || !reflect.DeepEqual(receiver.shards[0].batches, [][]int{{1, 2}}) || receiver.shards[0].finished {
		t.Fatalf("streamed state before flush = %+v", receiver.shards)
	}

	batch.Records[0] = 99
	if err := sharder.Flush(receiver); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !reflect.DeepEqual(receiver.shards[0].batches, [][]int{{1, 2}}) || receiver.shards[0].summary.Rows != 2 {
		t.Fatalf("finished shard = %+v", receiver.shards[0])
	}
}

type addressRecordingLogicalShardReceiver struct {
	starts  []*int
	lengths []int
}

func (*addressRecordingLogicalShardReceiver) BeginShard(shardID) error {
	return nil
}

func (s *addressRecordingLogicalShardReceiver) WriteBatch(records []int) error {
	s.starts = append(s.starts, &records[0])
	s.lengths = append(s.lengths, len(records))
	return nil
}

func (*addressRecordingLogicalShardReceiver) FinishShard(shardSummary) error {
	return nil
}

func TestLogicalSharderStreamsSourceBatchSlicesWithoutCopying(t *testing.T) {
	sharder, err := newLogicalSharder[int]("example", PhaseNodes, 2)
	if err != nil {
		t.Fatalf("new sharder: %v", err)
	}

	batch := transformedBatch[int]{Records: []int{1, 2, 3, 4, 5}, ActionCounts: make([]map[string]int, 5)}
	receiver := &addressRecordingLogicalShardReceiver{}
	if err := sharder.Add(batch, receiver); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	if err := sharder.Flush(receiver); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if !reflect.DeepEqual(receiver.lengths, []int{2, 2, 1}) {
		t.Fatalf("batch lengths = %v", receiver.lengths)
	}
	for index, offset := range []int{0, 2, 4} {
		if receiver.starts[index] != &batch.Records[offset] {
			t.Fatalf("batch %d does not share the transformed batch backing slice", index)
		}
	}
}

func TestLogicalSharderSplitsActionCounts(t *testing.T) {
	sharder, err := newLogicalSharder[string]("example", PhaseEdges, 2)
	if err != nil {
		t.Fatalf("new sharder: %v", err)
	}

	batch := transformedBatch[string]{
		Records: []string{"a", "b", "c"},
		ActionCounts: []map[string]int{
			{"preserve": 1},
			{"redact": 1},
			{"preserve": 2},
		},
	}
	receiver := &recordingLogicalShardReceiver[string]{}
	if err := sharder.Add(batch, receiver); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	if err := sharder.Flush(receiver); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if len(receiver.shards) != 2 || !reflect.DeepEqual(receiver.shards[0].summary.ActionCounts, map[string]int{"preserve": 1, "redact": 1}) || !reflect.DeepEqual(receiver.shards[1].summary.ActionCounts, map[string]int{"preserve": 2}) {
		t.Fatalf("shard action counts = %+v", receiver.shards)
	}
}

type failingLogicalShardReceiver[T any] struct {
	point   string
	failure error
}

func (s failingLogicalShardReceiver[T]) BeginShard(shardID) error {
	if s.point == "begin" {
		return s.failure
	}
	return nil
}

func (s failingLogicalShardReceiver[T]) WriteBatch([]T) error {
	if s.point == "write" {
		return s.failure
	}
	return nil
}

func (s failingLogicalShardReceiver[T]) FinishShard(shardSummary) error {
	if s.point == "finish" {
		return s.failure
	}
	return nil
}

func TestLogicalSharderValidationAndDownstreamErrors(t *testing.T) {
	if _, err := newLogicalSharder[int]("example", PhaseNodes, 0); err == nil {
		t.Fatalf("expected invalid shard size error")
	}

	validBatch := transformedBatch[int]{Records: []int{1}, ActionCounts: make([]map[string]int, 1)}
	for _, testCase := range []struct {
		name      string
		point     string
		shardSize int
		flush     bool
	}{
		{name: "begin", point: "begin", shardSize: 1},
		{name: "write", point: "write", shardSize: 1},
		{name: "finish at boundary", point: "finish", shardSize: 1},
		{name: "finish on flush", point: "finish", shardSize: 2, flush: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			failure := errors.New("downstream failed")
			receiver := failingLogicalShardReceiver[int]{point: testCase.point, failure: failure}
			sharder, err := newLogicalSharder[int]("example", PhaseNodes, testCase.shardSize)
			if err != nil {
				t.Fatalf("new sharder: %v", err)
			}

			err = sharder.Add(validBatch, receiver)
			if testCase.flush && err == nil {
				err = sharder.Flush(receiver)
			}
			if !errors.Is(err, failure) {
				t.Fatalf("downstream error = %v", err)
			}
		})
	}

	sharder, err := newLogicalSharder[int]("example", PhaseNodes, 1)
	if err != nil {
		t.Fatalf("new sharder: %v", err)
	}
	invalidBatch := transformedBatch[int]{Records: []int{1}}
	if err := sharder.Add(invalidBatch, &recordingLogicalShardReceiver[int]{}); err == nil {
		t.Fatalf("expected mismatched transformed batch error")
	}
}

func countBatchRows[T any](batches [][]T) int {
	var rows int
	for _, batch := range batches {
		rows += len(batch)
	}
	return rows
}
