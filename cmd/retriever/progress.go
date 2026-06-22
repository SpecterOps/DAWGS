package main

import (
	"log/slog"
	"time"
)

const retrieverProgressEntityInterval int64 = 250_000

func retrieverInitialProgressAt(planned int64) int64 {
	if planned <= retrieverProgressEntityInterval {
		return 0
	}
	return retrieverProgressEntityInterval
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

func logRetrieverEntityProgress(message string, graphName string, phaseName phase, processed int64, planned int64, startedAt time.Time, nextProgressAt int64) int64 {
	if nextProgressAt == 0 || processed < nextProgressAt || processed >= planned {
		return nextProgressAt
	}

	slog.Info(message,
		slog.String("graph", graphName),
		slog.String("phase", string(phaseName)),
		slog.Int64("processed", processed),
		slog.Int64("planned_count", planned),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
		slog.Float64("entities_per_second", perSecond(processed, time.Since(startedAt))),
	)

	return retrieverNextProgressAt(processed, planned, nextProgressAt)
}

func retrieverNextProgressAt(processed int64, planned int64, nextProgressAt int64) int64 {
	for nextProgressAt <= processed {
		nextProgressAt += retrieverProgressEntityInterval
	}
	if nextProgressAt >= planned {
		return 0
	}
	return nextProgressAt
}

func perSecond(count int64, elapsed time.Duration) float64 {
	if count == 0 || elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}
