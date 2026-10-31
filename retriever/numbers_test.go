package retriever

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFragmentReadersPreserveNumbers(t *testing.T) {
	want := map[string]any{
		"large": int64(9007199254740993), "wide": int64(4611686018427387905),
		"min": int64(math.MinInt64), "max": int64(math.MaxInt64), "small": int64(7),
		"fraction": 1.25, "string": "9007199254740993",
		"nested": map[string]any{"array": []any{int64(9007199254740993), 1.25, nil, true,
			map[string]any{"negative": int64(-9007199254740993)}}},
	}
	for _, codec := range []CompressionCodec{CompressionNone, CompressionGzip, CompressionZstd} {
		for _, verified := range []bool{false, true} {
			t.Run(string(codec)+"/verified="+fmt.Sprint(verified), func(t *testing.T) {
				dir := t.TempDir()
				nodeEntry, err := writeCompressedJSONLines(filepath.Join(dir, "nodes"), codec, DefaultZstdLevel,
					[]FragmentNode{{ID: "a", Properties: want}})
				require.NoError(t, err)
				nodeEntry.Phase, nodeEntry.Path = PhaseNodes, "nodes"
				count, err := decodeNodeFragmentFile(dir, codec, nodeEntry, verified, func(node FragmentNode) error {
					require.Equal(t, want, node.Properties)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 1, count)
				edgeEntry, err := writeCompressedJSONLines(filepath.Join(dir, "edges"), codec, DefaultZstdLevel,
					[]FragmentEdge{{StartID: "a", EndID: "a", Kind: "LOOP", Properties: want}})
				require.NoError(t, err)
				edgeEntry.Phase, edgeEntry.Path = PhaseEdges, "edges"
				count, err = decodeEdgeFragmentFile(dir, codec, edgeEntry, verified, func(edge FragmentEdge) error {
					require.Equal(t, want, edge.Properties)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 1, count)
			})
		}
	}
}

func TestLoadRejectsUnsupportedNumbersBeforeDatabaseAccess(t *testing.T) {
	for _, value := range []string{"9223372036854775808", "-9223372036854775809", "1e309"} {
		for _, phase := range []Phase{PhaseNodes, PhaseEdges} {
			t.Run(string(phase)+"/"+value, func(t *testing.T) {
				dir := t.TempDir()
				properties := map[string]any{"nested": []any{map[string]any{"number": json.Number(value)}}}
				nodeProperties, edgeProperties := properties, map[string]any(nil)
				if phase == PhaseEdges {
					nodeProperties, edgeProperties = nil, properties
				}
				nodeEntry, err := writeCompressedJSONLines(filepath.Join(dir, "nodes"), CompressionNone, DefaultZstdLevel,
					[]FragmentNode{{ID: "a", Properties: nodeProperties}})
				require.NoError(t, err)
				nodeEntry.Phase, nodeEntry.Path = PhaseNodes, "nodes"
				edgeEntry, err := writeCompressedJSONLines(filepath.Join(dir, "edges"), CompressionNone, DefaultZstdLevel,
					[]FragmentEdge{{StartID: "a", EndID: "a", Kind: "LOOP", Properties: edgeProperties}})
				require.NoError(t, err)
				edgeEntry.Phase, edgeEntry.Path = PhaseEdges, "edges"
				manifest := newValidTestManifest(1)
				manifest.Compression = CompressionNone
				manifest.Graphs = []GraphManifest{{Name: "default", NodeCount: 1, EdgeCount: 1, Files: []FileManifest{nodeEntry, edgeEntry}}}
				require.NoError(t, WriteManifest(dir, manifest))
				_, err = Load(context.Background(), nil, "pg", LoadOptions{InputDir: dir, BatchSize: 1})
				require.ErrorContains(t, err, `property "number"`)
			})
		}
	}
}
