package retriever

import (
	"errors"
	"reflect"
	"testing"
)

func TestLogicalSharderBoundaries(t *testing.T) {
	testCases := []struct {
		name      string
		shardSize int
		batches   [][]int
		expected  [][]int
	}{
		{name: "empty", shardSize: 2},
		{name: "exact boundaries", shardSize: 2, batches: [][]int{{1, 2}, {3, 4}}, expected: [][]int{{1, 2}, {3, 4}}},
		{name: "partial final shard", shardSize: 3, batches: [][]int{{1, 2}, {3, 4}}, expected: [][]int{{1, 2, 3}, {4}}},
		{name: "batch larger than shard", shardSize: 2, batches: [][]int{{1, 2, 3, 4, 5}}, expected: [][]int{{1, 2}, {3, 4}, {5}}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sharder, err := newLogicalSharder[int]("example", PhaseNodes, testCase.shardSize)
			if err != nil {
				t.Fatalf("new sharder: %v", err)
			}

			var shards []logicalShard[int]
			emit := func(shard logicalShard[int]) error {
				shards = append(shards, shard)
				return nil
			}
			for _, records := range testCase.batches {
				batch := transformedBatch[int]{Records: make([]transformedRecord[int], len(records))}
				for index, record := range records {
					batch.Records[index].Record = record
				}
				if err := sharder.Add(batch, emit); err != nil {
					t.Fatalf("add batch: %v", err)
				}
			}
			if err := sharder.Flush(emit); err != nil {
				t.Fatalf("flush: %v", err)
			}

			var actual [][]int
			for index, shard := range shards {
				actual = append(actual, shard.Records)
				if shard.Summary.ID.Graph != "example" || shard.Summary.ID.Phase != PhaseNodes || shard.Summary.ID.Number != index+1 || shard.Summary.Rows != len(shard.Records) {
					t.Fatalf("shard %d summary = %+v", index, shard.Summary)
				}
			}
			if !reflect.DeepEqual(actual, testCase.expected) {
				t.Fatalf("shards = %v, want %v", actual, testCase.expected)
			}
		})
	}
}

func TestLogicalSharderSplitsActionCounts(t *testing.T) {
	sharder, err := newLogicalSharder[string]("example", PhaseEdges, 2)
	if err != nil {
		t.Fatalf("new sharder: %v", err)
	}

	batch := transformedBatch[string]{Records: []transformedRecord[string]{
		{Record: "a", ActionCounts: map[string]int{"preserve": 1}},
		{Record: "b", ActionCounts: map[string]int{"redact": 1}},
		{Record: "c", ActionCounts: map[string]int{"preserve": 2}},
	}}
	var shards []logicalShard[string]
	emit := func(shard logicalShard[string]) error {
		shards = append(shards, shard)
		return nil
	}
	if err := sharder.Add(batch, emit); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	if err := sharder.Flush(emit); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if len(shards) != 2 || !reflect.DeepEqual(shards[0].Summary.ActionCounts, map[string]int{"preserve": 1, "redact": 1}) || !reflect.DeepEqual(shards[1].Summary.ActionCounts, map[string]int{"preserve": 2}) {
		t.Fatalf("shard action counts = %+v", shards)
	}
}

func TestLogicalSharderValidationAndEmitError(t *testing.T) {
	if _, err := newLogicalSharder[int]("example", PhaseNodes, 0); err == nil {
		t.Fatalf("expected invalid shard size error")
	}

	emitErr := errors.New("emit failed")
	sharder, err := newLogicalSharder[int]("example", PhaseNodes, 1)
	if err != nil {
		t.Fatalf("new sharder: %v", err)
	}
	if err := sharder.Add(transformedBatch[int]{Records: []transformedRecord[int]{{Record: 1}}}, func(logicalShard[int]) error {
		return emitErr
	}); !errors.Is(err, emitErr) {
		t.Fatalf("emit error = %v", err)
	}
}
