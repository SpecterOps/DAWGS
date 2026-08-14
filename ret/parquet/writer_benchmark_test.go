package parquet

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
)

var (
	parquetBenchmarkArtifact nodeFixtureArtifact
	parquetBenchmarkRow      NodeRow
)

func BenchmarkParquetVARIANTConversion(b *testing.B) {
	node := entity.Node{
		SourceID: "1",
		Kinds:    []string{"User", "Principal"},
		Properties: map[string]any{
			"name":    "alice",
			"enabled": true,
			"score":   int64(42),
			"nested": map[string]any{
				"labels": []any{"one", "two", "three"},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := validateProperties(node.Properties); err != nil {
			b.Fatal(err)
		}
		parquetBenchmarkRow = nodeRow(node)
	}
}

func BenchmarkParquetVARIANTWriting(b *testing.B) {
	nodes := make([]entity.Node, 256)
	for index := range nodes {
		nodes[index] = entity.Node{
			SourceID: fmt.Sprintf("%d", index+1),
			Kinds:    []string{"User", "Principal"},
			Properties: map[string]any{
				"name":  fmt.Sprintf("user-%d", index),
				"score": int64(index),
				"nested": map[string]any{
					"enabled": index%2 == 0,
					"groups":  []any{"one", "two"},
				},
			},
		}
	}
	config := Config{}
	directory := b.TempDir()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		path := filepath.Join(directory, fmt.Sprintf("nodes-%d.parquet", index))
		artifact, err := writeNodesFixture(path, filepath.Base(path), config, nodes)
		if err != nil {
			b.Fatal(err)
		}
		parquetBenchmarkArtifact = artifact
	}
}
