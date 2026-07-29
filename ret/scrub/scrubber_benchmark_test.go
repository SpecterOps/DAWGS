package scrub_test

import (
	"testing"

	"github.com/specterops/dawgs/ret/scrub"
)

var scrubBenchmarkCounts scrub.ActionCounts

func BenchmarkScrubPlanReuse(b *testing.B) {
	config := scrub.DefaultConfig()
	config.Salt = "benchmark-salt"
	scrubber, err := scrub.New(config)
	if err != nil {
		b.Fatal(err)
	}
	properties := map[string]any{
		"name":        "Alice Example",
		"objectid":    "S-1-5-21-111-222-333-1001",
		"description": "fixed benchmark description",
		"enabled":     true,
	}
	scrubber.Scrub(properties)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		scrubBenchmarkCounts = scrubber.Scrub(properties)
	}
}
