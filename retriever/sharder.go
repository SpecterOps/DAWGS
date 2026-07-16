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

type logicalShard[T any] struct {
	Summary shardSummary
	Records []T
}

type logicalSharder[T any] struct {
	graph        string
	phase        Phase
	shardSize    int
	nextNumber   int
	records      []T
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
		records:      make([]T, 0, shardSize),
		actionCounts: map[string]int{},
	}, nil
}

func (s *logicalSharder[T]) Add(batch transformedBatch[T], emit func(logicalShard[T]) error) error {
	for _, transformed := range batch.Records {
		s.records = append(s.records, transformed.Record)
		addActionCounts(s.actionCounts, transformed.ActionCounts)

		if len(s.records) == s.shardSize {
			if err := s.emit(emit); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *logicalSharder[T]) Flush(emit func(logicalShard[T]) error) error {
	if len(s.records) == 0 {
		return nil
	}
	return s.emit(emit)
}

func (s *logicalSharder[T]) emit(emit func(logicalShard[T]) error) error {
	shard := logicalShard[T]{
		Summary: shardSummary{
			ID: shardID{
				Graph:  s.graph,
				Phase:  s.phase,
				Number: s.nextNumber,
			},
			Rows:         len(s.records),
			ActionCounts: cloneActionCounts(s.actionCounts),
		},
		Records: s.records,
	}
	if err := emit(shard); err != nil {
		return err
	}

	s.nextNumber++
	s.records = make([]T, 0, s.shardSize)
	s.actionCounts = map[string]int{}
	return nil
}
