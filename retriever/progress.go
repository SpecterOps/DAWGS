package retriever

import (
	"log/slog"
	"time"
)

const retrieverProgressEntityInterval int64 = DefaultProgressInterval

func retrieverInitialProgressAt(planned int64) int64 {
	return retrieverInitialProgressAtInterval(planned, retrieverProgressEntityInterval)
}

func retrieverInitialProgressAtInterval(planned int64, interval int64) int64 {
	interval = normalizedProgressInterval(interval)
	if planned <= interval {
		return 0
	}

	return interval
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

func logRetrieverEntityProgress(message string, graphName string, phaseName Phase, processed int64, planned int64, startedAt time.Time, nextProgressAt int64, progress ProgressFunc) int64 {
	return logRetrieverEntityProgressInterval(message, graphName, phaseName, processed, planned, startedAt, nextProgressAt, progress, retrieverProgressEntityInterval)
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

func retrieverNextProgressAt(processed int64, planned int64, nextProgressAt int64) int64 {
	return retrieverNextProgressAtInterval(processed, planned, nextProgressAt, retrieverProgressEntityInterval)
}

func retrieverNextProgressAtInterval(processed int64, planned int64, nextProgressAt int64, interval int64) int64 {
	interval = normalizedProgressInterval(interval)
	if nextProgressAt <= processed {
		nextProgressAt += ((processed-nextProgressAt)/interval + 1) * interval
	}

	if nextProgressAt >= planned {
		return 0
	}

	return nextProgressAt
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
