package util

import (
	"log/slog"
	"sync/atomic"
	"time"
)

// slogMeasureID assigns process-local identifiers to function timing measurements.
var slogMeasureID = &atomic.Int64{}

// SLogSampleRepeated returns a sampler that logs elapsed and average milliseconds between calls.
// Calls to the returned sampler must be serialized; its call arguments supply the log fields.
func SLogSampleRepeated(functionName string, args ...any) func(args ...any) {
	var (
		measurementID = 1
		then          = time.Now()
		last          = then
	)

	return func(args ...any) {
		var (
			now        = time.Now()
			timingArgs = []any{
				slog.String("fn", functionName),
				slog.Int("sample_id", measurementID),
				slog.Int64("elapsed_ms", now.Sub(last).Milliseconds()),
				slog.Int64("avg_elapsed_ms", now.Sub(then).Milliseconds()/int64(measurementID)),
				slog.Int64("total_elapsed_ms", now.Sub(then).Milliseconds()),
			}
		)

		slog.Info("SLogSampleRepeated", append(args, timingArgs...)...)

		last = now
		measurementID += 1
	}
}

// SLogMeasureFunction logs entry and returns a completion logger sharing the measurement ID and initial fields.
func SLogMeasureFunction(functionName string, args ...any) func(args ...any) {
	var (
		then          = time.Now()
		measurementID = slogMeasureID.Add(1)
		allArgs       = append(args, slog.String("fn", functionName), slog.Int64("measurement_id", measurementID))
	)

	slog.Info("SLogMeasureFunction", append(allArgs, slog.String("state", "enter"))...)

	return func(args ...any) {
		exitArgs := append(allArgs, slog.Duration("elapsed", time.Since(then)), slog.String("state", "exit"))
		exitArgs = append(exitArgs, args...)

		slog.Info("SLogMeasureFunction", exitArgs...)
	}
}

// SLogError logs msg with the text of a non-nil error and the supplied structured fields.
func SLogError(msg string, err error, args ...any) {
	allArgs := append([]any{slog.String("err", err.Error())}, args...)
	slog.Error(msg, allArgs...)
}
