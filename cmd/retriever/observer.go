package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/specterops/dawgs/ret/observe"
)

const commandProgressInterval int64 = 250_000

type commandObserver struct {
	logger           *slog.Logger
	now              func() time.Time
	progressInterval int64

	mu     sync.Mutex
	phases map[commandPhaseKey]commandPhaseState
}

type commandPhaseKey struct {
	operation string
	graph     string
	phase     string
}

type commandPhaseState struct {
	started  time.Time
	baseline int64
	next     int64
}

type commandRuntimeTelemetry struct {
	heapAlloc uint64
	heapInuse uint64
	sys       uint64
	numGC     uint32
	rss       uint64
}

func newCommandObserver(logger *slog.Logger) *commandObserver {
	if logger == nil {
		logger = slog.Default()
	}
	return &commandObserver{
		logger:           logger,
		now:              time.Now,
		progressInterval: commandProgressInterval,
		phases:           map[commandPhaseKey]commandPhaseState{},
	}
}

func (s *commandObserver) Observe(ctx context.Context, event observe.Event) {
	switch value := event.(type) {
	case observe.OperationStarted:
		s.logger.InfoContext(ctx, "retriever operation started",
			slog.String("operation", value.Operation))
	case observe.OperationCompleted:
		attributes := []any{
			slog.String("operation", value.Operation),
			slog.Duration("duration", value.Duration),
		}
		if value.Err != nil {
			attributes = append(attributes, slog.Any("error", value.Err))
			s.logger.ErrorContext(ctx, "retriever operation completed", attributes...)
		} else {
			s.logger.InfoContext(ctx, "retriever operation completed", attributes...)
		}
	case observe.GraphStarted:
		s.logger.InfoContext(ctx, "retriever graph started",
			slog.String("operation", value.Operation),
			slog.String("graph", value.Graph))
	case observe.GraphCompleted:
		s.logger.InfoContext(ctx, "retriever graph completed",
			slog.String("operation", value.Operation),
			slog.String("graph", value.Graph),
			slog.Int64("nodes", value.Nodes),
			slog.Int64("relationships", value.Relationships),
			slog.Duration("duration", value.Duration))
	case observe.PhaseStarted:
		s.phaseStarted(ctx, value)
	case observe.PhaseProgress:
		s.phaseProgress(ctx, value)
	case observe.PhaseCompleted:
		s.phaseCompleted(ctx, value)
	case observe.ShardCommitted:
		s.logger.InfoContext(ctx, "retriever shard committed",
			slog.String("graph", value.Graph),
			slog.String("entity_type", value.EntityType),
			slog.Int("shard", value.Index),
			slog.Int64("count", value.Count),
			slog.String("jsonl_path", value.JSONLPath),
			slog.Int64("jsonl_bytes", value.JSONLBytes),
			slog.String("parquet_path", value.ParquetPath),
			slog.Int64("parquet_bytes", value.ParquetBytes))
	case observe.ArtifactVerified:
		s.logger.InfoContext(ctx, "retriever artifact verified",
			slog.String("graph", value.Graph),
			slog.String("entity_type", value.EntityType),
			slog.String("format", value.Format),
			slog.String("path", value.Path),
			slog.Int64("count", value.Count),
			slog.Int64("bytes", value.Bytes))
	case observe.ArchiveEntryProcessed:
		s.logger.InfoContext(ctx, "retriever archive entry processed",
			slog.String("operation", value.Operation),
			slog.String("path", value.Path),
			slog.Int64("bytes", value.Size))
	default:
		s.logger.DebugContext(ctx, "retriever event",
			slog.String("type", fmt.Sprintf("%T", event)))
	}
}

func (s *commandObserver) phaseStarted(ctx context.Context, event observe.PhaseStarted) {
	key := commandPhaseKey{operation: event.Operation, graph: event.Graph, phase: event.Phase}
	interval := s.normalizedProgressInterval()
	next := event.Completed + interval
	if event.Total > event.Completed && event.Total-event.Completed < interval {
		next = event.Total
	}
	s.mu.Lock()
	s.phases[key] = commandPhaseState{
		started:  s.now(),
		baseline: event.Completed,
		next:     next,
	}
	s.mu.Unlock()

	s.logger.InfoContext(ctx, "retriever phase started",
		slog.String("operation", event.Operation),
		slog.String("graph", event.Graph),
		slog.String("phase", event.Phase),
		slog.Int64("completed", event.Completed),
		slog.Int64("total", event.Total))
}

func (s *commandObserver) phaseProgress(ctx context.Context, event observe.PhaseProgress) {
	key := commandPhaseKey{operation: event.Operation, graph: event.Graph, phase: event.Phase}
	now := s.now()
	interval := s.normalizedProgressInterval()

	s.mu.Lock()
	state, found := s.phases[key]
	if !found {
		state = commandPhaseState{
			started: now,
			next:    interval,
		}
	}
	report := event.Completed >= state.next || event.Completed >= event.Total
	if report {
		for state.next <= event.Completed {
			state.next += interval
		}
		s.phases[key] = state
	}
	s.mu.Unlock()
	if !report {
		return
	}

	elapsed := now.Sub(state.started)
	telemetry := sampleCommandRuntimeTelemetry()
	s.logger.InfoContext(ctx, "retriever phase progress",
		slog.String("operation", event.Operation),
		slog.String("graph", event.Graph),
		slog.String("phase", event.Phase),
		slog.Int64("completed", event.Completed),
		slog.Int64("total", event.Total),
		slog.Duration("elapsed", elapsed),
		slog.Float64("entities_per_second", commandPerSecond(event.Completed-state.baseline, elapsed)),
		slog.Uint64("heap_alloc_bytes", telemetry.heapAlloc),
		slog.Uint64("heap_inuse_bytes", telemetry.heapInuse),
		slog.Uint64("sys_bytes", telemetry.sys),
		slog.Uint64("gc_count", uint64(telemetry.numGC)),
		slog.Uint64("rss_bytes", telemetry.rss))
}

func (s *commandObserver) phaseCompleted(ctx context.Context, event observe.PhaseCompleted) {
	key := commandPhaseKey{operation: event.Operation, graph: event.Graph, phase: event.Phase}
	s.mu.Lock()
	delete(s.phases, key)
	s.mu.Unlock()

	s.logger.InfoContext(ctx, "retriever phase completed",
		slog.String("operation", event.Operation),
		slog.String("graph", event.Graph),
		slog.String("phase", event.Phase),
		slog.Int64("completed", event.Completed),
		slog.Duration("duration", event.Duration))
}

func (s *commandObserver) normalizedProgressInterval() int64 {
	if s.progressInterval <= 0 {
		return commandProgressInterval
	}
	return s.progressInterval
}

func sampleCommandRuntimeTelemetry() commandRuntimeTelemetry {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return commandRuntimeTelemetry{
		heapAlloc: stats.HeapAlloc,
		heapInuse: stats.HeapInuse,
		sys:       stats.Sys,
		numGC:     stats.NumGC,
		rss:       commandCurrentRSS(),
	}
}

func commandCurrentRSS() uint64 {
	contents, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(contents))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
