package retriever

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/specterops/dawgs/graph"
)

func BenchmarkLoadFragmentPath(b *testing.B) {
	const (
		nodeCount = 10_000
		edgeCount = nodeCount - 1
		shardSize = 2_500
	)

	dir := b.TempDir()
	nodes := make([]FragmentNode, nodeCount)
	for index := range nodes {
		nodes[index] = FragmentNode{
			ID:    strconv.Itoa(index),
			Kinds: []string{"User"},
			Properties: map[string]any{
				"name": "user-" + strconv.Itoa(index),
				"team": "team-" + strconv.Itoa(index%100),
			},
		}
	}

	edges := make([]FragmentEdge, edgeCount)
	for index := range edges {
		edges[index] = FragmentEdge{
			StartID: strconv.Itoa(index),
			EndID:   strconv.Itoa(index + 1),
			Kind:    "ReportsTo",
			Properties: map[string]any{
				"source": "benchmark",
			},
		}
	}

	files := make([]FileManifest, 0, 8)
	for start, shard := 0, 1; start < len(nodes); start, shard = start+shardSize, shard+1 {
		end := min(start+shardSize, len(nodes))
		path := "nodes-" + strconv.Itoa(shard) + ".jsonl.gz"
		entry, err := writeCompressedJSONLines(filepath.Join(dir, path), CompressionGzip, DefaultZstdLevel, nodes[start:end])
		if err != nil {
			b.Fatalf("write node benchmark fragment: %v", err)
		}
		files = append(files, FileManifest{
			Phase:             PhaseNodes,
			Path:              path,
			Count:             entry.Rows,
			CompressedBytes:   entry.CompressedBytes,
			UncompressedBytes: entry.UncompressedBytes,
			SHA256:            entry.SHA256,
		})
	}
	for start, shard := 0, 1; start < len(edges); start, shard = start+shardSize, shard+1 {
		end := min(start+shardSize, len(edges))
		path := "edges-" + strconv.Itoa(shard) + ".jsonl.gz"
		entry, err := writeCompressedJSONLines(filepath.Join(dir, path), CompressionGzip, DefaultZstdLevel, edges[start:end])
		if err != nil {
			b.Fatalf("write edge benchmark fragment: %v", err)
		}
		files = append(files, FileManifest{
			Phase:             PhaseEdges,
			Path:              path,
			Count:             entry.Rows,
			CompressedBytes:   entry.CompressedBytes,
			UncompressedBytes: entry.UncompressedBytes,
			SHA256:            entry.SHA256,
		})
	}

	value := newValidTestManifest(1)
	value.Graphs = []GraphManifest{{
		Name:      "benchmark",
		NodeCount: nodeCount,
		EdgeCount: edgeCount,
		Files:     files,
	}}

	var compressedBytes int64
	for _, fileEntry := range files {
		compressedBytes += fileEntry.CompressedBytes
	}
	b.SetBytes(compressedBytes * 2)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := verifyLoadFragments(dir, value); err != nil {
			b.Fatalf("preflight benchmark fragments: %v", err)
		}

		nodeMap := make(map[string]graph.ID, nodeCount)
		var loadedNodes, loadedEdges int
		for _, fileEntry := range files {
			switch fileEntry.Phase {
			case PhaseNodes:
				if _, err := readNodeFragmentFile(dir, value.Compression, fileEntry, func(item FragmentNode) error {
					loadedNodes++
					nodeMap[item.ID] = graph.ID(loadedNodes)
					return nil
				}); err != nil {
					b.Fatalf("load node benchmark fragment: %v", err)
				}

			case PhaseEdges:
				if _, err := readEdgeFragmentFile(dir, value.Compression, fileEntry, func(item FragmentEdge) error {
					if _, err := resolveFragmentEdge(item, nodeMap); err != nil {
						return err
					}
					loadedEdges++
					return nil
				}); err != nil {
					b.Fatalf("load edge benchmark fragment: %v", err)
				}
			}
		}

		if loadedNodes != nodeCount || loadedEdges != edgeCount {
			b.Fatalf("unexpected benchmark counts: nodes=%d edges=%d", loadedNodes, loadedEdges)
		}
	}
}
