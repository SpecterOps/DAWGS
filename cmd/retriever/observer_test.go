package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs/ret/observe"
)

func TestCommandObserverTranslatesEveryConcreteEvent(t *testing.T) {
	cases := []struct {
		name  string
		event observe.Event
		want  string
	}{
		{name: "operation started", event: observe.OperationStarted{Operation: "dump"}, want: "operation started"},
		{name: "operation completed", event: observe.OperationCompleted{Operation: "dump", Duration: time.Second}, want: "operation completed"},
		{name: "graph started", event: observe.GraphStarted{Operation: "dump", Graph: "asset"}, want: "graph started"},
		{name: "graph completed", event: observe.GraphCompleted{Operation: "dump", Graph: "asset", Nodes: 2, Relationships: 1, Duration: time.Second}, want: "graph completed"},
		{name: "phase started", event: observe.PhaseStarted{Operation: "dump", Graph: "asset", Phase: "nodes", Total: 2}, want: "phase started"},
		{name: "phase progress", event: observe.PhaseProgress{Operation: "dump", Graph: "asset", Phase: "nodes", Completed: 2, Total: 2}, want: "phase progress"},
		{name: "phase completed", event: observe.PhaseCompleted{Operation: "dump", Graph: "asset", Phase: "nodes", Completed: 2, Duration: time.Second}, want: "phase completed"},
		{name: "shard committed", event: observe.ShardCommitted{Graph: "asset", EntityType: "nodes", Index: 1, Count: 2, JSONLPath: "nodes.jsonl"}, want: "shard committed"},
		{name: "artifact verified", event: observe.ArtifactVerified{Graph: "asset", EntityType: "nodes", Format: "jsonl", Path: "nodes.jsonl", Count: 2}, want: "artifact verified"},
		{name: "archive entry", event: observe.ArchiveEntryProcessed{Operation: "pack", Path: "manifest.json", Size: 100}, want: "archive entry processed"},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			observer := newCommandObserver(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})))
			observer.Observe(context.Background(), test.event)
			if !strings.Contains(output.String(), test.want) {
				t.Fatalf("log output %q does not contain %q", output.String(), test.want)
			}
		})
	}
}

func TestCommandObserverLogsOperationFailureAsError(t *testing.T) {
	var output bytes.Buffer
	observer := newCommandObserver(slog.New(slog.NewTextHandler(&output, nil)))
	observer.Observe(context.Background(), observe.OperationCompleted{
		Operation: "load",
		Duration:  time.Second,
		Err:       errors.New("failed"),
	})

	if got := output.String(); !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "error=failed") {
		t.Fatalf("failure log = %q", got)
	}
}

func TestCommandObserverWritesStructuredEventAttributes(t *testing.T) {
	cases := []struct {
		name  string
		event observe.Event
		want  map[string]any
	}{
		{
			name:  "operation",
			event: observe.OperationStarted{Operation: "dump"},
			want:  map[string]any{"operation": "dump"},
		},
		{
			name: "graph",
			event: observe.GraphCompleted{
				Operation:     "load",
				Graph:         "asset",
				Nodes:         4,
				Relationships: 3,
			},
			want: map[string]any{
				"operation":     "load",
				"graph":         "asset",
				"nodes":         float64(4),
				"relationships": float64(3),
			},
		},
		{
			name: "shard",
			event: observe.ShardCommitted{
				Graph:        "asset",
				EntityType:   "nodes",
				Index:        2,
				Count:        5,
				JSONLPath:    "nodes.jsonl",
				JSONLBytes:   10,
				ParquetPath:  "nodes.parquet",
				ParquetBytes: 20,
			},
			want: map[string]any{
				"graph":         "asset",
				"entity_type":   "nodes",
				"shard":         float64(2),
				"count":         float64(5),
				"jsonl_path":    "nodes.jsonl",
				"jsonl_bytes":   float64(10),
				"parquet_path":  "nodes.parquet",
				"parquet_bytes": float64(20),
			},
		},
		{
			name: "artifact",
			event: observe.ArtifactVerified{
				Graph:      "asset",
				EntityType: "relationships",
				Format:     "parquet",
				Path:       "relationships.parquet",
				Count:      6,
				Bytes:      30,
			},
			want: map[string]any{
				"graph":       "asset",
				"entity_type": "relationships",
				"format":      "parquet",
				"path":        "relationships.parquet",
				"count":       float64(6),
				"bytes":       float64(30),
			},
		},
		{
			name:  "archive",
			event: observe.ArchiveEntryProcessed{Operation: "unpack", Path: "manifest.json", Size: 40},
			want:  map[string]any{"operation": "unpack", "path": "manifest.json", "bytes": float64(40)},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			observer := newCommandObserver(slog.New(slog.NewJSONHandler(&output, nil)))
			observer.Observe(context.Background(), test.event)
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatalf("decode structured log %q: %v", output.String(), err)
			}
			for key, expected := range test.want {
				if actual := record[key]; actual != expected {
					t.Fatalf("structured attribute %q = %#v, want %#v; record=%v", key, actual, expected, record)
				}
			}
		})
	}
}

func TestCommandObserverSamplesProgressAndAddsRuntimeTelemetry(t *testing.T) {
	var output bytes.Buffer
	observer := newCommandObserver(slog.New(slog.NewTextHandler(&output, nil)))
	observer.progressInterval = 100
	started := time.Unix(100, 0)
	observer.now = func() time.Time { return started }
	observer.Observe(context.Background(), observe.PhaseStarted{
		Operation: "dump",
		Graph:     "asset",
		Phase:     "nodes",
		Total:     250,
	})

	output.Reset()
	observer.now = func() time.Time { return started.Add(2 * time.Second) }
	observer.Observe(context.Background(), observe.PhaseProgress{
		Operation: "dump",
		Graph:     "asset",
		Phase:     "nodes",
		Completed: 50,
		Total:     250,
	})
	if output.Len() != 0 {
		t.Fatalf("unsampled progress was logged: %q", output.String())
	}

	observer.now = func() time.Time { return started.Add(4 * time.Second) }
	observer.Observe(context.Background(), observe.PhaseProgress{
		Operation: "dump",
		Graph:     "asset",
		Phase:     "nodes",
		Completed: 150,
		Total:     250,
	})
	got := output.String()
	for _, value := range []string{
		"phase progress",
		"entities_per_second=37.5",
		"heap_alloc_bytes=",
		"heap_inuse_bytes=",
		"sys_bytes=",
		"gc_count=",
		"rss_bytes=",
	} {
		if !strings.Contains(got, value) {
			t.Fatalf("progress log %q does not contain %q", got, value)
		}
	}
}

func TestCommandObserverUsesFirstProgressAsResumedBaseline(t *testing.T) {
	var output bytes.Buffer
	observer := newCommandObserver(slog.New(slog.NewTextHandler(&output, nil)))
	observer.progressInterval = 100
	started := time.Unix(100, 0)
	observer.now = func() time.Time { return started }
	observer.Observe(context.Background(), observe.PhaseStarted{
		Operation: "dump",
		Graph:     "asset",
		Phase:     "nodes",
		Completed: 500,
		Total:     1_000,
	})

	output.Reset()
	observer.now = func() time.Time { return started.Add(2 * time.Second) }
	observer.Observe(context.Background(), observe.PhaseProgress{
		Operation: "dump",
		Graph:     "asset",
		Phase:     "nodes",
		Completed: 600,
		Total:     1_000,
	})
	if got := output.String(); !strings.Contains(got, "entities_per_second=50") {
		t.Fatalf("resumed rate log = %q, want 100/2 entities per second", got)
	}
}

func TestCommandObserverReportsFreshOneBatchProgress(t *testing.T) {
	var output bytes.Buffer
	observer := newCommandObserver(slog.New(slog.NewTextHandler(&output, nil)))
	observer.progressInterval = 100
	started := time.Unix(100, 0)
	observer.now = func() time.Time { return started }
	observer.Observe(context.Background(), observe.PhaseStarted{
		Operation: "load",
		Graph:     "asset",
		Phase:     "nodes",
		Completed: 0,
		Total:     1,
	})

	output.Reset()
	observer.now = func() time.Time { return started.Add(time.Second) }
	observer.Observe(context.Background(), observe.PhaseProgress{
		Operation: "load",
		Graph:     "asset",
		Phase:     "nodes",
		Completed: 1,
		Total:     1,
	})
	if got := output.String(); !strings.Contains(got, "phase progress") ||
		!strings.Contains(got, "entities_per_second=1") ||
		!strings.Contains(got, "heap_alloc_bytes=") {
		t.Fatalf("fresh one-batch progress log = %q, want progress, rate, and telemetry", got)
	}
}

func TestCommandObserverCompletionRemovesPhaseState(t *testing.T) {
	observer := newCommandObserver(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	event := observe.PhaseStarted{Operation: "load", Graph: "asset", Phase: "nodes", Total: 10}
	observer.Observe(context.Background(), event)
	if len(observer.phases) != 1 {
		t.Fatalf("phase state count = %d, want 1", len(observer.phases))
	}
	observer.Observe(context.Background(), observe.PhaseCompleted{
		Operation: event.Operation,
		Graph:     event.Graph,
		Phase:     event.Phase,
		Completed: 10,
	})
	if len(observer.phases) != 0 {
		t.Fatalf("phase state count after completion = %d, want 0", len(observer.phases))
	}
}

func TestCommandObserverHandlesConcurrentPhasesAndCanceledContexts(t *testing.T) {
	var output bytes.Buffer
	observer := newCommandObserver(slog.New(slog.NewTextHandler(&output, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	const phases = 32
	var wait sync.WaitGroup
	wait.Add(phases)
	for index := range phases {
		go func() {
			defer wait.Done()
			graphName := fmt.Sprintf("graph-%d", index)
			observer.Observe(ctx, observe.PhaseStarted{
				Operation: "verify_database",
				Graph:     graphName,
				Phase:     "nodes",
				Total:     1,
			})
			observer.Observe(ctx, observe.PhaseProgress{
				Operation: "verify_database",
				Graph:     graphName,
				Phase:     "nodes",
				Completed: 1,
				Total:     1,
			})
			observer.Observe(ctx, observe.PhaseCompleted{
				Operation: "verify_database",
				Graph:     graphName,
				Phase:     "nodes",
				Completed: 1,
			})
		}()
	}
	wait.Wait()

	if len(observer.phases) != 0 {
		t.Fatalf("phase state count = %d, want 0", len(observer.phases))
	}
	if !strings.Contains(output.String(), "operation=verify_database") {
		t.Fatalf("canceled-context events were not structured: %q", output.String())
	}
}
