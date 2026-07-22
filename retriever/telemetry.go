package retriever

import (
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type runtimeTelemetry struct {
	heapAlloc uint64
	heapInuse uint64
	sys       uint64
	numGC     uint32
	rss       uint64
}

func sampleRuntimeTelemetry() runtimeTelemetry {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	return runtimeTelemetry{
		heapAlloc: stats.HeapAlloc,
		heapInuse: stats.HeapInuse,
		sys:       stats.Sys,
		numGC:     stats.NumGC,
		rss:       currentRSS(),
	}
}

func (s *ProgressEvent) withRuntimeTelemetry() {
	if s.HeapAlloc != 0 || s.HeapInuse != 0 || s.Sys != 0 || s.NumGC != 0 || s.RSS != 0 {
		return
	}

	telemetry := sampleRuntimeTelemetry()
	s.HeapAlloc = telemetry.heapAlloc
	s.HeapInuse = telemetry.heapInuse
	s.Sys = telemetry.sys
	s.NumGC = telemetry.numGC
	s.RSS = telemetry.rss
}

func currentRSS() uint64 {
	contents, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}

	fields := strings.Fields(string(contents))
	if len(fields) < 2 {
		return 0
	}

	residentPages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}

	return residentPages * uint64(os.Getpagesize())
}

func logInfoWithRuntimeTelemetry(message string, args ...any) {
	telemetry := sampleRuntimeTelemetry()
	args = append(args,
		slog.Uint64("heap_alloc_bytes", telemetry.heapAlloc),
		slog.Uint64("heap_inuse_bytes", telemetry.heapInuse),
		slog.Uint64("sys_bytes", telemetry.sys),
		slog.Uint64("gc_count", uint64(telemetry.numGC)),
		slog.Uint64("rss_bytes", telemetry.rss),
	)
	slog.Info(message, args...)
}
