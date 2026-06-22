package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestBenchSamplingHelpers(t *testing.T) {
	if got := benchPlannedCount(10, 3); got != 3 {
		t.Fatalf("planned sampled count = %d", got)
	}
	if got := benchPlannedCount(10, 0); got != 10 {
		t.Fatalf("planned full count = %d", got)
	}
	if got := benchPlannedCount(3, 10); got != 3 {
		t.Fatalf("planned capped count = %d", got)
	}
	if got := benchPlannedCount(0, 10); got != 0 {
		t.Fatalf("planned empty count = %d", got)
	}
	if got := retrieverBatchLimit(3, 10); got != 3 {
		t.Fatalf("batch limit for remainder = %d", got)
	}
	if got := retrieverBatchLimit(20, 10); got != 10 {
		t.Fatalf("batch limit for full batch = %d", got)
	}
	if got := retrieverInitialProgressAt(retrieverProgressEntityInterval); got != 0 {
		t.Fatalf("unexpected progress threshold for exact interval: %d", got)
	}
	if got := retrieverInitialProgressAt(retrieverProgressEntityInterval + 1); got != retrieverProgressEntityInterval {
		t.Fatalf("unexpected progress threshold: %d", got)
	}
}

func TestBenchFormattingHelpers(t *testing.T) {
	if got := perSecond(10, 2*time.Second); got != 5 {
		t.Fatalf("perSecond = %f", got)
	}
	if got := perSecond(10, 0); got != 0 {
		t.Fatalf("perSecond with zero duration = %f", got)
	}

	var buffer bytes.Buffer
	writeBenchReport(&buffer, benchReport{
		Graphs: []benchGraphReport{{
			Name: "default",
			Results: []benchResult{{
				Workers:           2,
				BatchSize:         100,
				SampleSize:        2,
				NodeCount:         3,
				EdgeCount:         4,
				NodeProcessed:     2,
				EdgeProcessed:     2,
				TotalWallMillis:   50,
				EntitiesPerSecond: 140,
				NodeDBReadMillis:  10,
				EdgeDBReadMillis:  20,
			}},
		}},
	})
	output := buffer.String()
	for _, expected := range []string{"graph: default", "workers=2", "sample_size=2", "nodes=2/3", "edges=2/4", "entities_per_sec=140.00", "db_read_ms=30"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("bench report missing %q in %q", expected, output)
		}
	}
}
