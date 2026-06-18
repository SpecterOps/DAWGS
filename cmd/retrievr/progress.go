package main

import (
	"log/slog"
	"time"
)

const retrievrProgressEntityInterval int64 = 250_000

func retrievrInitialProgressAt(planned int64) int64 {
	if planned <= retrievrProgressEntityInterval {
		return 0
	}
	return retrievrProgressEntityInterval
}

func logRetrievrEntityProgress(message string, graphName string, phaseName phase, processed int64, planned int64, startedAt time.Time, nextProgressAt int64) int64 {
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

	return retrievrNextProgressAt(processed, planned, nextProgressAt)
}

func retrievrNextProgressAt(processed int64, planned int64, nextProgressAt int64) int64 {
	for nextProgressAt <= processed {
		nextProgressAt += retrievrProgressEntityInterval
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
