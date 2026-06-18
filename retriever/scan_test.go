package retriever

import (
	"errors"
	"reflect"
	"strings"
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

func TestScanEntityBatchesUsesKeysetAndRemainingLimit(t *testing.T) {
	entities := []scanTestEntity{{id: 1}, {id: 2}, {id: 3}, {id: 4}}
	var (
		calls    []scanReadCall
		handled  []graph.ID
		progress []int64
	)

	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:      3,
		BatchSize:  2,
		EntityName: "node",
		Read: func(afterID graph.ID, hasAfterID bool, limit int) ([]scanTestEntity, error) {
			calls = append(calls, scanReadCall{
				afterID:    afterID,
				hasAfterID: hasAfterID,
				limit:      limit,
			})
			var batch []scanTestEntity
			for _, entity := range entities {
				if !hasAfterID || entity.id > afterID {
					batch = append(batch, entity)
				}
			}

			if len(batch) > limit {
				batch = batch[:limit]
			}

			return batch, nil
		},
		ID: func(entity scanTestEntity) graph.ID {
			return entity.id
		},
		Handle: func(batch []scanTestEntity) error {
			for _, entity := range batch {
				handled = append(handled, entity.id)
			}

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

	if processed != 3 {
		t.Fatalf("processed = %d", processed)
	}

	if !reflect.DeepEqual(handled, []graph.ID{1, 2, 3}) {
		t.Fatalf("handled IDs = %v", handled)
	}

	expectedCalls := []scanReadCall{
		{limit: 2},
		{afterID: 2, hasAfterID: true, limit: 1},
	}

	if !reflect.DeepEqual(calls, expectedCalls) {
		t.Fatalf("read calls = %+v", calls)
	}

	if !reflect.DeepEqual(progress, []int64{2, 3}) {
		t.Fatalf("progress = %v", progress)
	}
}

func TestScanEntityBatchesTruncatesOverfullFinalBatch(t *testing.T) {
	var handled []graph.ID
	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:      2,
		BatchSize:  10,
		EntityName: "node",
		Read: func(graph.ID, bool, int) ([]scanTestEntity, error) {
			return []scanTestEntity{{id: 1}, {id: 2}, {id: 3}}, nil
		},
		ID: func(entity scanTestEntity) graph.ID {
			return entity.id
		},
		Handle: func(batch []scanTestEntity) error {
			for _, entity := range batch {
				handled = append(handled, entity.id)
			}

			return nil
		},
	})

	if err != nil {
		t.Fatalf("scan entity batches: %v", err)
	}

	if processed != 2 || !reflect.DeepEqual(handled, []graph.ID{1, 2}) {
		t.Fatalf("processed=%d handled=%v", processed, handled)
	}
}

func TestScanEntityBatchesUsesProgressInterval(t *testing.T) {
	var firstNextProgressAt int64
	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:            10,
		BatchSize:        1,
		ProgressInterval: 3,
		EntityName:       "node",
		Read: func(afterID graph.ID, hasAfterID bool, _ int) ([]scanTestEntity, error) {
			if !hasAfterID {
				return []scanTestEntity{{id: 1}}, nil
			}

			return []scanTestEntity{{id: afterID + 1}}, nil
		},
		ID: func(entity scanTestEntity) graph.ID {
			return entity.id
		},
		LogProgress: func(_ int64, _ time.Time, nextProgressAt int64) int64 {
			if firstNextProgressAt == 0 {
				firstNextProgressAt = nextProgressAt
			}
			return nextProgressAt
		},
	})
	if err != nil {
		t.Fatalf("scan entity batches: %v", err)
	}

	if processed != 10 {
		t.Fatalf("processed = %d", processed)
	}

	if firstNextProgressAt != 3 {
		t.Fatalf("first next progress threshold = %d, want 3", firstNextProgressAt)
	}
}

func TestScanEntityBatchesReportsNonAdvancingCursor(t *testing.T) {
	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:      2,
		BatchSize:  1,
		EntityName: "relationship",
		Read: func(afterID graph.ID, hasAfterID bool, _ int) ([]scanTestEntity, error) {
			if hasAfterID {
				return []scanTestEntity{{id: afterID}}, nil
			}

			return []scanTestEntity{{id: 7}}, nil
		},
		ID: func(entity scanTestEntity) graph.ID {
			return entity.id
		},
	})
	if err == nil || !strings.Contains(err.Error(), "relationship keyset scan did not advance after ID 7") {
		t.Fatalf("expected non-advancing cursor error, got %v", err)
	}

	if processed != 1 {
		t.Fatalf("processed = %d", processed)
	}
}

func TestScanEntityBatchesValidationAndReaderErrors(t *testing.T) {
	if processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{Total: 0}); err != nil || processed != 0 {
		t.Fatalf("empty scan processed=%d err=%v", processed, err)
	}

	if _, err := scanEntityBatches(entityScanOptions[scanTestEntity]{Total: 1, BatchSize: 0}); err == nil {
		t.Fatalf("expected batch size validation error")
	}

	if _, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:     1,
		BatchSize: 1,
		Read: func(graph.ID, bool, int) ([]scanTestEntity, error) {
			return nil, nil
		},
	}); err == nil {
		t.Fatalf("expected ID accessor validation error")
	}

	readerErr := errors.New("reader failed")
	processed, err := scanEntityBatches(entityScanOptions[scanTestEntity]{
		Total:      1,
		BatchSize:  1,
		EntityName: "node",
		Read: func(graph.ID, bool, int) ([]scanTestEntity, error) {
			return nil, readerErr
		},
		ID: func(entity scanTestEntity) graph.ID {
			return entity.id
		},
	})
	if !errors.Is(err, readerErr) || processed != 0 {
		t.Fatalf("processed=%d err=%v", processed, err)
	}
}
