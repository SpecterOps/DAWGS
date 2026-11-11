package retriever

import (
	"log/slog"
	"time"
)

const retrieverProgressEntityInterval int64 = DefaultProgressInterval

// retrieverInitialProgressAtInterval selects the first intermediate entity checkpoint or disables it for small workloads.
func retrieverInitialProgressAtInterval(planned int64, interval int64) int64 {
	effectiveInterval := normalizedProgressInterval(interval)
	if planned <= effectiveInterval {
		return 0
	}

	return effectiveInterval
}

func retrieverBatchLimit(remaining int64, batchSize int) int {
	if remaining <= 0 {
		return 0
	}

	if int64(batchSize) > remaining {
		return int(remaining)
	}

	return batchSize
}

func logRetrieverEntityProgressInterval(message string, graphName string, phaseName Phase, processed int64, planned int64, startedAt time.Time, nextProgressAt int64, progress ProgressFunc, interval int64) int64 {
	if nextProgressAt == 0 || processed < nextProgressAt || processed >= planned {
		return nextProgressAt
	}

	elapsed := time.Since(startedAt)
	telemetry := sampleRuntimeTelemetry()
	slog.Info(message,
		slog.String("graph", graphName),
		slog.String("phase", string(phaseName)),
		slog.Int64("processed", processed),
		slog.Int64("planned_count", planned),
		slog.Duration("wall_elapsed", elapsed),
		slog.Float64("entities_per_second", perSecond(processed, elapsed)),
		slog.Uint64("heap_alloc_bytes", telemetry.heapAlloc),
		slog.Uint64("heap_inuse_bytes", telemetry.heapInuse),
		slog.Uint64("sys_bytes", telemetry.sys),
		slog.Uint64("gc_count", uint64(telemetry.numGC)),
		slog.Uint64("rss_bytes", telemetry.rss),
	)
	progress.emit(ProgressEvent{
		Message:           message,
		Graph:             graphName,
		Phase:             phaseName,
		Processed:         processed,
		Planned:           planned,
		Elapsed:           elapsed,
		EntitiesPerSecond: perSecond(processed, elapsed),
		HeapAlloc:         telemetry.heapAlloc,
		HeapInuse:         telemetry.heapInuse,
		Sys:               telemetry.sys,
		NumGC:             telemetry.numGC,
		RSS:               telemetry.rss,
	})

	return retrieverNextProgressAtInterval(processed, planned, nextProgressAt, interval)
}

// retrieverNextProgressAtInterval advances the entity checkpoint beyond completed work and disables it at the final boundary.
func retrieverNextProgressAtInterval(processed int64, planned int64, nextProgressAt int64, interval int64) int64 {
	nextCheckpoint := nextProgressAt

	effectiveInterval := normalizedProgressInterval(interval)
	if nextCheckpoint <= processed {
		nextCheckpoint += ((processed-nextCheckpoint)/effectiveInterval + 1) * effectiveInterval
	}

	if nextCheckpoint >= planned {
		return 0
	}

	return nextCheckpoint
}

func normalizedProgressInterval(interval int64) int64 {
	if interval <= 0 {
		return retrieverProgressEntityInterval
	}

	return interval
}

func perSecond(count int64, elapsed time.Duration) float64 {
	if count == 0 || elapsed <= 0 {
		return 0
	}

	return float64(count) / elapsed.Seconds()
}
