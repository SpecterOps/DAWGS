package jsonl_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
)

var jsonlBenchmarkArtifact jsonl.NodeArtifact

func BenchmarkJSONLWriting(b *testing.B) {
	nodes := make([]entity.Node, 256)
	for index := range nodes {
		nodes[index] = entity.Node{
			SourceID: fmt.Sprintf("%d", index+1),
			Kinds:    []string{"User", "Principal"},
			Properties: map[string]any{
				"name":    fmt.Sprintf("user-%d", index),
				"enabled": index%2 == 0,
				"score":   index,
			},
		}
	}
	config := jsonl.Config{Enabled: true, Codec: jsonl.CodecZstd}
	directory := b.TempDir()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		path := filepath.Join(directory, fmt.Sprintf("nodes-%d.jsonl.zst", index))
		artifact, err := jsonl.WriteNodes(path, filepath.Base(path), config, nodes)
		if err != nil {
			b.Fatal(err)
		}
		jsonlBenchmarkArtifact = artifact
	}
}
