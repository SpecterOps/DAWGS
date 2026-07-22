package retriever

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
)

type scanTestEntity struct {
	id graph.ID
}

type scanReadCall struct {
	afterID    graph.ID
	hasAfterID bool
	limit      int
}

func TestScanEntityBatchesVisitsStrictKeysetsIncrementally(t *testing.T) {
	entities := []scanTestEntity{{id: 1}, {id: 2}, {id: 3}, {id: 4}, {id: 5}}
	var (
		calls    []scanReadCall
		handled  []graph.ID
		progress []int64
		batches  []ScanBatchEvent
	)

	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Context:    context.Background(),
		Total:      int64(len(entities)),
		BatchSize:  2,
		EntityName: "node",
		Read: func(afterID graph.ID, hasAfterID bool, limit int, visit func(scanTestEntity) error) error {
			calls = append(calls, scanReadCall{afterID: afterID, hasAfterID: hasAfterID, limit: limit})
			visited := 0
			for _, entity := range entities {
				if (!hasAfterID || entity.id > afterID) && visited < limit {
					if err := visit(entity); err != nil {
						return err
					}
					visited++
				}
			}
			return nil
		},
		ID: func(entity scanTestEntity) graph.ID { return entity.id },
		Handle: func(entity scanTestEntity) error {
			handled = append(handled, entity.id)
			return nil
		},
		BatchComplete: func(event ScanBatchEvent) error {
			batches = append(batches, event)
			return nil
		},
		LogProgress: func(processed int64, _ time.Time, nextProgressAt int64) int64 {
			progress = append(progress, processed)
			return nextProgressAt
		},
	})
	if err != nil {
		t.Fatalf("scan entity batches: %v", err)
	}

	if processed != 5 || !reflect.DeepEqual(handled, []graph.ID{1, 2, 3, 4, 5}) {
		t.Fatalf("processed=%d handled=%v", processed, handled)
	}

	expectedCalls := []scanReadCall{{limit: 2}, {afterID: 2, hasAfterID: true, limit: 2}, {afterID: 4, hasAfterID: true, limit: 1}}
	if !reflect.DeepEqual(calls, expectedCalls) {
		t.Fatalf("read calls = %+v", calls)
	}

	if !reflect.DeepEqual(progress, []int64{2, 4, 5}) {
		t.Fatalf("progress = %v", progress)
	}

	if got := []int{batches[0].Count, batches[1].Count, batches[2].Count}; !reflect.DeepEqual(got, []int{2, 2, 1}) {
		t.Fatalf("batch counts = %v", got)
	}
}

func TestScanEntityBatchesResumesAfterCommittedCursor(t *testing.T) {
	entities := []scanTestEntity{{id: 1}, {id: 2}, {id: 3}, {id: 4}, {id: 5}}
	var handled []graph.ID

	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Context:          context.Background(),
		Total:            int64(len(entities)),
		BatchSize:        2,
		EntityName:       "node",
		StartAfterID:     2,
		HasStartAfterID:  true,
		AlreadyProcessed: 2,
		Read: func(afterID graph.ID, _ bool, limit int, visit func(scanTestEntity) error) error {
			for _, entity := range entities {
				if entity.id > afterID && limit > 0 {
					if err := visit(entity); err != nil {
						return err
					}
					limit--
				}
			}
			return nil
		},
		ID: func(entity scanTestEntity) graph.ID { return entity.id },
		Handle: func(entity scanTestEntity) error {
			handled = append(handled, entity.id)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("resume scan: %v", err)
	}
	if processed != 5 || !reflect.DeepEqual(handled, []graph.ID{3, 4, 5}) {
		t.Fatalf("processed=%d handled=%v", processed, handled)
	}

	_, err = scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:            3,
		BatchSize:        1,
		AlreadyProcessed: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "resume cursor") {
		t.Fatalf("expected missing resume cursor error, got %v", err)
	}
}

func TestScanEntityBatchesBoundaryAndValidationCases(t *testing.T) {
	if processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{Total: 0}); err != nil || processed != 0 {
		t.Fatalf("empty scan processed=%d err=%v", processed, err)
	}

	for name, options := range map[string]entityScanOptions[scanTestEntity]{
		"batch size": {Total: 1, BatchSize: 0},
		"reader":     {Total: 1, BatchSize: 1, ID: func(entity scanTestEntity) graph.ID { return entity.id }},
		"ID": {
			Total:     1,
			BatchSize: 1,
			Read: func(graph.ID, bool, int, func(scanTestEntity) error) error {
				return nil
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := scanEntityBatches(options); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:      3,
		BatchSize:  1,
		EntityName: "node",
		Read: func(afterID graph.ID, hasAfterID bool, _ int, visit func(scanTestEntity) error) error {
			next := graph.ID(1)
			if hasAfterID {
				next = afterID + 1
			}
			return visit(scanTestEntity{id: next})
		},
		ID: func(entity scanTestEntity) graph.ID { return entity.id },
	})
	if err != nil || processed != 3 {
		t.Fatalf("B=1 exact multiple processed=%d err=%v", processed, err)
	}
}

func TestScanEntityBatchesRejectsBackendInvariantViolations(t *testing.T) {
	testCases := map[string]struct {
		total     int64
		batchSize int
		read      entityCursorReader[scanTestEntity]
		wantErr   string
		wantCount int64
	}{
		"over limit": {
			total:     2,
			batchSize: 2,
			read: func(_ graph.ID, _ bool, _ int, visit func(scanTestEntity) error) error {
				for _, id := range []graph.ID{1, 2, 3} {
					if err := visit(scanTestEntity{id: id}); err != nil {
						return err
					}
				}
				return nil
			},
			wantErr:   "exceeded requested limit 2",
			wantCount: 2,
		},
		"duplicate ID": {
			total:     2,
			batchSize: 2,
			read: func(_ graph.ID, _ bool, _ int, visit func(scanTestEntity) error) error {
				if err := visit(scanTestEntity{id: 7}); err != nil {
					return err
				}
				return visit(scanTestEntity{id: 7})
			},
			wantErr:   "not strictly increasing",
			wantCount: 1,
		},
		"descending ID": {
			total:     2,
			batchSize: 2,
			read: func(_ graph.ID, _ bool, _ int, visit func(scanTestEntity) error) error {
				if err := visit(scanTestEntity{id: 8}); err != nil {
					return err
				}
				return visit(scanTestEntity{id: 7})
			},
			wantErr:   "ID 7 followed ID 8",
			wantCount: 1,
		},
		"short snapshot": {
			total:     3,
			batchSize: 2,
			read: func(afterID graph.ID, hasAfterID bool, _ int, visit func(scanTestEntity) error) error {
				if hasAfterID {
					return nil
				}
				return visit(scanTestEntity{id: afterID + 1})
			},
			wantErr:   "ended after 1 of 3",
			wantCount: 1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
				Total:      testCase.total,
				BatchSize:  testCase.batchSize,
				EntityName: "relationship",
				Read:       testCase.read,
				ID:         func(entity scanTestEntity) graph.ID { return entity.id },
			})
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("expected error containing %q, got %v", testCase.wantErr, err)
			}
			if processed != testCase.wantCount {
				t.Fatalf("processed=%d, want %d", processed, testCase.wantCount)
			}
		})
	}
}

func TestScanEntityBatchesPropagatesErrorsAndCancellation(t *testing.T) {
	readerErr := errors.New("reader failed")
	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:     1,
		BatchSize: 1,
		Read: func(graph.ID, bool, int, func(scanTestEntity) error) error {
			return readerErr
		},
		ID: func(entity scanTestEntity) graph.ID { return entity.id },
	})
	if !errors.Is(err, readerErr) || processed != 0 {
		t.Fatalf("reader error processed=%d err=%v", processed, err)
	}

	handlerErr := errors.New("handler failed")
	processed, err = scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:     1,
		BatchSize: 1,
		Read: func(_ graph.ID, _ bool, _ int, visit func(scanTestEntity) error) error {
			return visit(scanTestEntity{id: 1})
		},
		ID:     func(entity scanTestEntity) graph.ID { return entity.id },
		Handle: func(scanTestEntity) error { return handlerErr },
	})
	if !errors.Is(err, handlerErr) || processed != 0 {
		t.Fatalf("handler error processed=%d err=%v", processed, err)
	}

	batchErr := errors.New("batch failed")
	processed, err = scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:     1,
		BatchSize: 1,
		Read: func(_ graph.ID, _ bool, _ int, visit func(scanTestEntity) error) error {
			return visit(scanTestEntity{id: 1})
		},
		ID:            func(entity scanTestEntity) graph.ID { return entity.id },
		BatchComplete: func(ScanBatchEvent) error { return batchErr },
	})
	if !errors.Is(err, batchErr) || processed != 1 {
		t.Fatalf("batch error processed=%d err=%v", processed, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	processed, err = scanEntityBatches(entityScanOptions[scanTestEntity]{
		Context:   ctx,
		Total:     1,
		BatchSize: 1,
		Read: func(graph.ID, bool, int, func(scanTestEntity) error) error {
			t.Fatal("reader called after cancellation")
			return nil
		},
		ID: func(entity scanTestEntity) graph.ID { return entity.id },
	})
	if !errors.Is(err, context.Canceled) || processed != 0 {
		t.Fatalf("cancellation processed=%d err=%v", processed, err)
	}
}

type producingCursor[T any] struct {
	values chan T
	done   chan struct{}
	err    error
	once   sync.Once
}

func newProducingCursor[T any](buffer int) *producingCursor[T] {
	return &producingCursor[T]{values: make(chan T, buffer), done: make(chan struct{})}
}

func (s *producingCursor[T]) Error() error { return s.err }
func (s *producingCursor[T]) Chan() chan T { return s.values }
func (s *producingCursor[T]) Close()       { s.once.Do(func() { close(s.done) }) }

func TestVisitBoundedCursorCancelsProducerOnEarlyReturn(t *testing.T) {
	cursor := newProducingCursor[scanTestEntity](0)
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(cursor.values)
		for id := graph.ID(1); ; id++ {
			select {
			case <-cursor.done:
				return
			case cursor.values <- scanTestEntity{id: id}:
			}
		}
	}()

	handlerErr := errors.New("stop")
	count, err := visitBoundedCursor(context.Background(), cursor, "node", 10, func(scanTestEntity) error {
		return handlerErr
	})
	if !errors.Is(err, handlerErr) || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}

	select {
	case <-producerDone:
	case <-time.After(time.Second):
		t.Fatal("cursor producer did not stop after early callback return")
	}
}

func TestVisitBoundedCursorReportsLimitCursorErrorAndCancellation(t *testing.T) {
	t.Run("limit", func(t *testing.T) {
		cursor := newProducingCursor[scanTestEntity](3)
		cursor.values <- scanTestEntity{id: 1}
		cursor.values <- scanTestEntity{id: 2}
		close(cursor.values)

		count, err := visitBoundedCursor(context.Background(), cursor, "node", 1, nil)
		if err == nil || !strings.Contains(err.Error(), "exceeded requested limit 1") || count != 1 {
			t.Fatalf("count=%d err=%v", count, err)
		}
	})

	t.Run("cursor error", func(t *testing.T) {
		cursorErr := errors.New("cursor failed")
		cursor := newProducingCursor[scanTestEntity](0)
		cursor.err = cursorErr
		close(cursor.values)

		count, err := visitBoundedCursor(context.Background(), cursor, "node", 1, nil)
		if !errors.Is(err, cursorErr) || count != 0 {
			t.Fatalf("count=%d err=%v", count, err)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cursor := newProducingCursor[scanTestEntity](0)

		count, err := visitBoundedCursor(ctx, cursor, "node", 1, nil)
		if !errors.Is(err, context.Canceled) || count != 0 {
			t.Fatalf("count=%d err=%v", count, err)
		}
		select {
		case <-cursor.done:
		default:
			t.Fatal("cursor was not closed on cancellation")
		}
	})
}

func BenchmarkScanEntityBatchesFixedBatch(b *testing.B) {
	for _, total := range []int64{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("entities_%d", total), func(b *testing.B) {
			for range b.N {
				processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
					Total:     total,
					BatchSize: 100,
					Read: func(afterID graph.ID, _ bool, limit int, visit func(scanTestEntity) error) error {
						for offset := 1; offset <= limit; offset++ {
							if err := visit(scanTestEntity{id: afterID + graph.ID(offset)}); err != nil {
								return err
							}
						}
						return nil
					},
					ID: func(entity scanTestEntity) graph.ID { return entity.id },
				})
				if err != nil || processed != total {
					b.Fatalf("processed=%d err=%v", processed, err)
				}
			}
		})
	}
}
