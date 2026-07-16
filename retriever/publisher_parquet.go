package retriever

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	ParquetManifestFormat   = "retriever-parquet-export-v1"
	ParquetManifestFileName = "parquet/manifest.json"
	ParquetSuccessFileName  = "parquet/_SUCCESS"
)

type ParquetManifest struct {
	Format             string                 `json:"format"`
	GeneratedAt        time.Time              `json:"generated_at"`
	Compression        string                 `json:"compression"`
	PropertiesEncoding string                 `json:"properties_encoding"`
	Graphs             []ParquetGraphManifest `json:"graphs"`
}

type ParquetGraphManifest struct {
	Name      string                `json:"name"`
	NodeCount int64                 `json:"node_count"`
	EdgeCount int64                 `json:"edge_count"`
	Files     []ParquetFileManifest `json:"files"`
}

type ParquetFileManifest struct {
	Phase        Phase          `json:"phase"`
	Path         string         `json:"path"`
	Count        int            `json:"count"`
	Bytes        int64          `json:"bytes"`
	SHA256       string         `json:"sha256"`
	ActionCounts map[string]int `json:"action_counts"`
}

type parquetPublication struct {
	ManifestPath string
	SuccessPath  string
}

type parquetPublisher interface {
	AddFragment(shardSummary, parquetFragmentMetadata)
	AddGraph(string, int64, int64)
	PublishManifest(context.Context) (string, error)
	PublishSuccess(context.Context) (string, error)
}

type parquetCollectionPublisher struct {
	workspace collectionWorkspace
	manifest  ParquetManifest
	files     map[string][]ParquetFileManifest
}

func newParquetCollectionPublisher(workspace collectionWorkspace, graphCount int) *parquetCollectionPublisher {
	return &parquetCollectionPublisher{
		workspace: workspace,
		manifest: ParquetManifest{
			Format:             ParquetManifestFormat,
			GeneratedAt:        time.Now().UTC(),
			Compression:        "zstd",
			PropertiesEncoding: "json",
			Graphs:             make([]ParquetGraphManifest, 0, graphCount),
		},
		files: make(map[string][]ParquetFileManifest, graphCount),
	}
}

func (s *parquetCollectionPublisher) AddFragment(summary shardSummary, metadata parquetFragmentMetadata) {
	s.files[summary.ID.Graph] = append(s.files[summary.ID.Graph], ParquetFileManifest{
		Phase:        summary.ID.Phase,
		Path:         metadata.Path,
		Count:        metadata.Rows,
		Bytes:        metadata.Bytes,
		SHA256:       metadata.SHA256,
		ActionCounts: cloneActionCounts(summary.ActionCounts),
	})
}

func (s *parquetCollectionPublisher) AddGraph(name string, nodeCount, edgeCount int64) {
	s.manifest.Graphs = append(s.manifest.Graphs, ParquetGraphManifest{
		Name:      name,
		NodeCount: nodeCount,
		EdgeCount: edgeCount,
		Files:     append([]ParquetFileManifest(nil), s.files[name]...),
	})
}

func (s *parquetCollectionPublisher) PublishManifest(ctx context.Context) (string, error) {
	payload, err := encodeParquetManifest(s.manifest)
	if err != nil {
		return "", err
	}
	return s.workspace.Publish(ctx, ParquetManifestFileName, payload)
}

func (s *parquetCollectionPublisher) PublishSuccess(ctx context.Context) (string, error) {
	return s.workspace.Publish(ctx, ParquetSuccessFileName, []byte(ParquetManifestFormat+"\n"))
}

func publishDumpOutputs(ctx context.Context, jsonl collectionPublisher, parquet parquetPublisher) (collectionPublication, parquetPublication, error) {
	var parquetResult parquetPublication
	if parquet != nil {
		path, err := parquet.PublishManifest(ctx)
		if err != nil {
			return collectionPublication{}, parquetResult, err
		}
		parquetResult.ManifestPath = path
	}

	jsonlResult, err := jsonl.Publish(ctx)
	if err != nil {
		return collectionPublication{}, parquetResult, err
	}

	if parquet != nil {
		path, err := parquet.PublishSuccess(ctx)
		if err != nil {
			return jsonlResult, parquetResult, err
		}
		parquetResult.SuccessPath = path
	}
	return jsonlResult, parquetResult, nil
}

func ReadParquetManifest(outputDir string) (ParquetManifest, error) {
	var manifest ParquetManifest
	payload, err := os.ReadFile(filepath.Join(outputDir, filepath.FromSlash(ParquetManifestFileName)))
	if err != nil {
		return manifest, fmt.Errorf("read Parquet manifest: %w", err)
	}
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return manifest, fmt.Errorf("decode Parquet manifest: %w", err)
	}
	if err := manifest.validate(); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func encodeParquetManifest(manifest ParquetManifest) ([]byte, error) {
	if err := manifest.validate(); err != nil {
		return nil, err
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode Parquet manifest: %w", err)
	}
	return append(payload, '\n'), nil
}

func (s ParquetManifest) validate() error {
	if s.Format != ParquetManifestFormat {
		return fmt.Errorf("unsupported Parquet manifest format %q", s.Format)
	}
	if s.Compression != "zstd" {
		return fmt.Errorf("unsupported Parquet compression %q", s.Compression)
	}
	if s.PropertiesEncoding != "json" {
		return fmt.Errorf("unsupported Parquet properties encoding %q", s.PropertiesEncoding)
	}

	seenGraphs := make(map[string]struct{}, len(s.Graphs))
	seenPaths := map[string]struct{}{}
	for _, graph := range s.Graphs {
		if graph.Name == "" {
			return fmt.Errorf("Parquet manifest graph entry has empty name")
		}
		if _, seen := seenGraphs[graph.Name]; seen {
			return fmt.Errorf("Parquet manifest contains duplicate graph %q", graph.Name)
		}
		seenGraphs[graph.Name] = struct{}{}

		var nodes, edges int64
		seenEdgePhase := false
		for _, file := range graph.Files {
			if file.Path == "" || file.Count < 0 || file.Bytes < 0 || file.SHA256 == "" {
				return fmt.Errorf("Parquet manifest contains invalid file entry %+v", file)
			}
			if _, seen := seenPaths[file.Path]; seen {
				return fmt.Errorf("Parquet manifest contains duplicate path %q", file.Path)
			}
			seenPaths[file.Path] = struct{}{}
			switch file.Phase {
			case PhaseNodes:
				if seenEdgePhase {
					return fmt.Errorf("Parquet manifest graph %q lists node file after edge file", graph.Name)
				}
				nodes += int64(file.Count)
			case PhaseEdges:
				seenEdgePhase = true
				edges += int64(file.Count)
			default:
				return fmt.Errorf("Parquet manifest graph %q contains unsupported phase %q", graph.Name, file.Phase)
			}
		}
		if graph.NodeCount != nodes || graph.EdgeCount != edges {
			return fmt.Errorf("Parquet manifest graph %q counts (%d nodes, %d edges) do not match file totals (%d nodes, %d edges)", graph.Name, graph.NodeCount, graph.EdgeCount, nodes, edges)
		}
	}
	return nil
}

func (s ParquetManifest) Validate() error {
	return s.validate()
}
