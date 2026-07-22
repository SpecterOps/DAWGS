package retriever

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/specterops/dawgs/graph"
)

const representativeNodeCount = 1_845_833

func benchmarkRetainedHeap(b *testing.B, build func() any) {
	b.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	b.StartTimer()
	retained := build()
	b.StopTimer()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	var retainedBytes uint64
	if after.HeapAlloc > before.HeapAlloc {
		retainedBytes = after.HeapAlloc - before.HeapAlloc
	}
	b.ReportMetric(float64(retainedBytes), "retained-B")
	runtime.KeepAlive(retained)
}

func BenchmarkRetainedHeapQueryHydration(b *testing.B) {
	for _, count := range []int{DefaultBatchSize, 10 * DefaultBatchSize, 100 * DefaultBatchSize} {
		b.Run(fmt.Sprintf("entities_%d", count), func(b *testing.B) {
			benchmarkRetainedHeap(b, func() any {
				properties := graph.AsProperties(map[string]any{"name": "representative"})
				entities := make([]*graph.Node, count)
				for index := range entities {
					entities[index] = graph.NewNode(graph.ID(index+1), properties, graph.StringKind("Node"))
				}
				return entities
			})
		})
	}
}

func BenchmarkRetainedHeapRegistryFreeScrub(b *testing.B) {
	benchmarkRetainedHeap(b, func() any {
		scrubber, err := newScrubber(nil, "benchmark-salt")
		if err != nil {
			b.Fatal(err)
		}
		for index := 0; index < representativeNodeCount; index++ {
			scrubber.scrubPropertiesWithCounts(map[string]any{
				"objectid": fmt.Sprintf("S-1-5-21-1-2-3-%d", index),
			})
		}
		return scrubber
	})
}

func BenchmarkRetainedHeapMetricsRepresentativeNodes(b *testing.B) {
	benchmarkRetainedHeap(b, func() any {
		builder := newMetricsBuilder("representative", representativeNodeCount)
		for index := 0; index < representativeNodeCount; index++ {
			if err := builder.observeDatabaseNode(graph.ID(index+1), []string{"Node"}); err != nil {
				b.Fatal(err)
			}
		}
		return builder
	})
}

func BenchmarkRetainedHeapReadOnlyProperties(b *testing.B) {
	benchmarkRetainedHeap(b, func() any {
		values := make([]*graph.Properties, representativeNodeCount)
		for index := range values {
			values[index] = graph.AsProperties(map[string]any{"index": index})
			if values[index].Modified != nil || values[index].Deleted != nil {
				b.Fatal("read-only properties allocated mutation tracking maps")
			}
		}
		return values
	})
}

func BenchmarkEdgeMetricsAllocation(b *testing.B) {
	builder := newMetricsBuilder("edges", 2)
	if err := builder.observeDatabaseNode(1, []string{"Start"}); err != nil {
		b.Fatal(err)
	}
	if err := builder.observeDatabaseNode(2, []string{"End"}); err != nil {
		b.Fatal(err)
	}
	// Intern the kind before measuring the steady-state edge path.
	if err := builder.observeDatabaseRelationship(1, 2, "Edge"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := builder.observeDatabaseRelationship(1, 2, "Edge"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEdgeScrubAllocation(b *testing.B) {
	scrubber, err := newScrubber(nil, "benchmark-salt")
	if err != nil {
		b.Fatal(err)
	}
	properties := map[string]any{
		"objectid": "S-1-5-21-1-2-3-1000",
		"name":     "alice@example.com",
	}
	scrubber.scrubPropertiesWithCounts(properties)

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		scrubber.scrubPropertiesWithCounts(properties)
	}
}

func BenchmarkRetainedHeapJSONLZstdWriterLifecycle(b *testing.B) {
	benchmarkRetainedHeap(b, func() any {
		var manifests []FileManifest
		for index := 0; index < 10; index++ {
			path := filepath.Join(b.TempDir(), fmt.Sprintf("fragment-%02d.jsonl.zst", index))
			writer, err := newCompressedJSONLinesWriter(path, CompressionZstd, DefaultZstdLevel)
			if err != nil {
				b.Fatal(err)
			}
			for record := 0; record < 1_000; record++ {
				if err := writer.Write(FragmentNode{ID: fmt.Sprint(record), Kinds: []string{"Node"}}); err != nil {
					b.Fatal(err)
				}
			}
			manifest, err := writer.Close()
			if err != nil {
				b.Fatal(err)
			}
			manifests = append(manifests, manifest)
		}
		return manifests
	})
}
