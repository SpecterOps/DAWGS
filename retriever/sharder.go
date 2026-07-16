package retriever

import "fmt"

type shardID struct {
	Graph  string
	Phase  Phase
	Number int
}

type shardSummary struct {
	ID           shardID
	Rows         int
	ActionCounts map[string]int
}

type logicalShardReceiver[T any] interface {
	BeginShard(shardID) error
	WriteBatch([]T) error
	FinishShard(shardSummary) error
}

type logicalSharder[T any] struct {
	graph        string
	phase        Phase
	shardSize    int
	nextNumber   int
	active       bool
	rows         int
	actionCounts map[string]int
}

func newLogicalSharder[T any](graphName string, phase Phase, shardSize int) (*logicalSharder[T], error) {
	if shardSize <= 0 {
		return nil, fmt.Errorf("shard size must be > 0")
	}

	return &logicalSharder[T]{
		graph:        graphName,
		phase:        phase,
		shardSize:    shardSize,
		nextNumber:   1,
		actionCounts: map[string]int{},
	}, nil
}

func (s *logicalSharder[T]) Add(batch transformedBatch[T], receiver logicalShardReceiver[T]) error {
	if len(batch.Records) != len(batch.ActionCounts) {
		return fmt.Errorf("transformed batch has %d records and %d action-count entries", len(batch.Records), len(batch.ActionCounts))
	}

	for offset := 0; offset < len(batch.Records); {
		if !s.active {
			if err := receiver.BeginShard(s.id()); err != nil {
				return err
			}
			s.active = true
		}

		batchEnd := min(offset+s.shardSize-s.rows, len(batch.Records))
		if err := receiver.WriteBatch(batch.Records[offset:batchEnd]); err != nil {
			return err
		}
		for _, actionCounts := range batch.ActionCounts[offset:batchEnd] {
			addActionCounts(s.actionCounts, actionCounts)
		}
		s.rows += batchEnd - offset
		offset = batchEnd

		if s.rows == s.shardSize {
			if err := s.finish(receiver); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *logicalSharder[T]) Flush(receiver logicalShardReceiver[T]) error {
	if !s.active || s.rows == 0 {
		return nil
	}
	return s.finish(receiver)
}

func (s *logicalSharder[T]) id() shardID {
	return shardID{
		Graph:  s.graph,
		Phase:  s.phase,
		Number: s.nextNumber,
	}
}

func (s *logicalSharder[T]) finish(receiver logicalShardReceiver[T]) error {
	summary := shardSummary{
		ID:           s.id(),
		Rows:         s.rows,
		ActionCounts: cloneActionCounts(s.actionCounts),
	}
	if err := receiver.FinishShard(summary); err != nil {
		return err
	}

	s.nextNumber++
	s.active = false
	s.rows = 0
	s.actionCounts = map[string]int{}
	return nil
}
