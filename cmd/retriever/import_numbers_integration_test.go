//go:build manual_integration || integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/retriever"
	"github.com/stretchr/testify/require"
)

func TestImportExactNumbers(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING not set")
	}
	ctx := context.Background()
	db, driverName, err := openDatabase(ctx, databaseConfig{Connection: connection})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(ctx)) })
	target := graph.Graph{
		Name:  fmt.Sprintf("import_numbers_%d", time.Now().UnixNano()),
		Nodes: graph.StringsToKinds([]string{"ImportExactNode"}),
		Edges: graph.StringsToKinds([]string{"ImportExactEdge"}),
	}
	require.NoError(t, db.AssertSchema(ctx, graph.Schema{Graphs: []graph.Graph{target}, DefaultGraph: target}))
	// Retriever loads require an empty target. Reset the selected test graph,
	// including fixtures from earlier packages when Neo4j ignores graph names.
	clear := func() {
		require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			tx = tx.WithGraph(graph.Graph{Name: target.Name})
			if err := tx.Relationships().Delete(); err != nil {
				return err
			}
			return tx.Nodes().Delete()
		}))
	}
	t.Cleanup(clear)

	// Keep the input tokens independent of the import decoder and its normalization.
	const properties = `{"large":9007199254740993,"negative":-9007199254740993,
		"wide":4611686018427387905,"min":-9223372036854775808,"max":9223372036854775807,
		"small":7,"fraction":1.25,"exponent":1.25e2,"string":"9007199254740993","enabled":true,
		"array":[9007199254740993,-9007199254740993,-9223372036854775808,9223372036854775807]}`
	const expectedProperties = `{"large":9007199254740993,"negative":-9007199254740993,
		"wide":4611686018427387905,"min":-9223372036854775808,"max":9223372036854775807,
		"small":7,"fraction":1.25,"exponent":125,"string":"9007199254740993","enabled":true,
		"array":[9007199254740993,-9007199254740993,-9223372036854775808,9223372036854775807]}`
	var compactProperties bytes.Buffer
	require.NoError(t, json.Compact(&compactProperties, []byte(properties)))
	nodes := fmt.Sprintf(`{"id":"a","kinds":["ImportExactNode"],"properties":%s}
{"id":"b","kinds":["ImportExactNode"],"properties":%s}
`, compactProperties.String(), compactProperties.String())
	edges := fmt.Sprintf(`{"start_id":"a","end_id":"b","kind":"ImportExactEdge","properties":%s}
`, compactProperties.String())
	for _, format := range []string{"document", "none", "gzip", "zstd"} {
		t.Run(format, func(t *testing.T) {
			clear()
			if format == "document" {
				input := fmt.Sprintf(`{"graph":{"nodes":[%s],"edges":[%s]}}`,
					strings.ReplaceAll(strings.TrimSpace(nodes), "\n", ","), strings.TrimSpace(edges))
				ids, err := opengraph.Load(ctx, db, strings.NewReader(input))
				require.NoError(t, err)
				require.Len(t, ids, 2)
			} else {
				dir := t.TempDir()
				codec := retriever.CompressionCodec(format)
				files := []retriever.FileManifest{
					writeImportNumberFragment(t, dir, codec, retriever.PhaseNodes, nodes, 2),
					writeImportNumberFragment(t, dir, codec, retriever.PhaseEdges, edges, 1),
				}
				manifest := retriever.Manifest{
					Format: "retriever-jsonl-collection-v1", IDStrategy: "source_database_id_string",
					Driver: driverName, Compression: codec, Source: retriever.SourceMetadata{GraphCount: 1},
					Scrub: retriever.ScrubMetadata{Mode: retriever.ScrubNone},
					Schema: retriever.SchemaMetadata{Graphs: []retriever.GraphSchemaMetadata{{
						Name: target.Name, NodeKinds: []string{"ImportExactNode"}, EdgeKinds: []string{"ImportExactEdge"},
					}}},
					Graphs: []retriever.GraphManifest{{Name: target.Name, NodeCount: 2, EdgeCount: 1, Files: files}},
				}
				require.NoError(t, retriever.WriteManifest(dir, manifest))
				result, err := retriever.Load(ctx, db, driverName, retriever.LoadOptions{InputDir: dir, BatchSize: 1})
				require.NoError(t, err)
				require.Equal(t, int64(2), result.NodeCount)
				require.Equal(t, int64(1), result.EdgeCount)
			}

			// These reads start after Load commits. PostgreSQL JSON text avoids its
			// unrelated graph-result float64 decoder; Neo4j exposes native Bolt values.
			require.NoError(t, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
				for _, entity := range []string{"node", "edge"} {
					statement := "MATCH (n:ImportExactNode) RETURN properties(n)"
					parameters := map[string]any(nil)
					wantCount := 2
					if entity == "edge" {
						statement = "MATCH ()-[e:ImportExactEdge]->() RETURN properties(e)"
						wantCount = 1
					}
					if driverName == pg.DriverName {
						statement = "select e.properties::text from " + entity + " e join graph g on g.id = e.graph_id where g.name = @graph_name"
						parameters = map[string]any{"graph_name": target.Name}
					}
					result := tx.Raw(statement, parameters)
					count := 0
					for result.Next() {
						value := result.Values()[0]
						var encoded string
						if driverName == pg.DriverName {
							encoded = value.(string)
						} else {
							payload, err := json.Marshal(value)
							require.NoError(t, err)
							encoded = string(payload)
						}
						// Compare raw JSON text so neighboring large integers cannot
						// pass through an assertion that decodes to float64.
						var got, want map[string]json.RawMessage
						require.NoError(t, json.Unmarshal([]byte(encoded), &got))
						require.NoError(t, json.Unmarshal([]byte(expectedProperties), &want))
						require.Len(t, got, len(want))
						for key, expected := range want {
							var actualCompact, expectedCompact bytes.Buffer
							require.NoError(t, json.Compact(&actualCompact, got[key]))
							require.NoError(t, json.Compact(&expectedCompact, expected))
							require.Equal(t, expectedCompact.String(), actualCompact.String(), "%s property %s", entity, key)
						}
						count++
					}
					result.Close()
					require.NoError(t, result.Error())
					require.Equal(t, wantCount, count, entity)
				}
				return nil
			}))
		})
	}
}

func writeImportNumberFragment(t *testing.T, dir string, codec retriever.CompressionCodec, phase retriever.Phase, payload string, count int) retriever.FileManifest {
	t.Helper()
	var compressed bytes.Buffer
	var writer io.WriteCloser
	switch codec {
	case retriever.CompressionNone:
		compressed.WriteString(payload)
	case retriever.CompressionGzip:
		writer = gzip.NewWriter(&compressed)
	case retriever.CompressionZstd:
		var err error
		writer, err = zstd.NewWriter(&compressed)
		require.NoError(t, err)
	}
	if writer != nil {
		_, err := io.WriteString(writer, payload)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	name := string(phase) + ".jsonl"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), compressed.Bytes(), 0o600))
	digest := sha256.Sum256(compressed.Bytes())
	return retriever.FileManifest{
		Phase: phase, Path: name, Count: count, SHA256: hex.EncodeToString(digest[:]),
		CompressedBytes: int64(compressed.Len()), UncompressedBytes: int64(len(payload)),
	}
}
