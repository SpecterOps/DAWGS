package retriever

import (
	"fmt"
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"
)

const (
	manifestFormat = "retriever-jsonl-collection-v1"
	idStrategy     = "source_database_id_string"
)

type Phase string

const (
	PhaseNodes Phase = "nodes"
	PhaseEdges Phase = "edges"
)

type CompressionCodec string

const (
	CompressionDisabled CompressionCodec = ""
	CompressionGzip     CompressionCodec = "gzip"
	CompressionZstd     CompressionCodec = "zstd"
)

type ScrubMode string

const (
	ScrubNone ScrubMode = "none"
	ScrubFull ScrubMode = "full"
)

type Manifest struct {
	Format           string           `json:"format"`
	GeneratedAt      time.Time        `json:"generated_at"`
	RetrieverVersion string           `json:"retriever_version,omitempty"`
	Driver           string           `json:"driver"`
	Source           SourceMetadata   `json:"source"`
	Compression      CompressionCodec `json:"compression"`
	CompressionLevel int              `json:"compression_level,omitempty"`
	Scrub            ScrubMetadata    `json:"scrub"`
	IDStrategy       string           `json:"id_strategy"`
	Schema           SchemaMetadata   `json:"schema"`
	Graphs           []GraphManifest  `json:"graphs"`
	Metrics          *MetricsManifest `json:"metrics,omitempty"`
	Warnings         []string         `json:"warnings,omitempty"`
}

type SourceMetadata struct {
	GraphCount int `json:"graph_count"`
}

type ScrubMetadata struct {
	Mode             ScrubMode      `json:"mode"`
	RulesVersion     string         `json:"rules_version,omitempty"`
	SaltProvided     bool           `json:"salt_provided"`
	NodeActionCounts map[string]int `json:"node_action_counts"`
	EdgeActionCounts map[string]int `json:"edge_action_counts"`
}

type SchemaMetadata struct {
	Graphs []GraphSchemaMetadata `json:"graphs"`
}

type GraphSchemaMetadata struct {
	Name      string   `json:"name"`
	NodeKinds []string `json:"node_kinds"`
	EdgeKinds []string `json:"edge_kinds"`
}

type GraphManifest struct {
	Name             string         `json:"name"`
	NodeCount        int64          `json:"node_count"`
	EdgeCount        int64          `json:"edge_count"`
	NodeActionCounts map[string]int `json:"node_action_counts"`
	EdgeActionCounts map[string]int `json:"edge_action_counts"`
	Files            []FileManifest `json:"files"`
}

type FileManifest struct {
	Phase             Phase          `json:"phase"`
	Path              string         `json:"path"`
	Count             int            `json:"count"`
	CompressedBytes   int64          `json:"compressed_bytes"`
	UncompressedBytes int64          `json:"uncompressed_bytes"`
	SHA256            string         `json:"sha256"`
	ActionCounts      map[string]int `json:"action_counts"`
}

func (s FileManifest) rowCount() int {
	return s.Count
}

type FragmentNode struct {
	ID         string         `json:"id"`
	Kinds      []string       `json:"kinds"`
	Properties map[string]any `json:"properties,omitempty"`
}

type FragmentEdge struct {
	StartID    string         `json:"start_id"`
	EndID      string         `json:"end_id"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties,omitempty"`
}

func newManifest(driverName string, codec CompressionCodec, compressionLevel int, scrub ScrubMetadata, graphCount int) Manifest {
	return Manifest{
		Format:      manifestFormat,
		GeneratedAt: time.Now().UTC(),
		Driver:      driverName,
		Source: SourceMetadata{
			GraphCount: graphCount,
		},
		Compression:      codec,
		CompressionLevel: compressionLevel,
		Scrub:            scrub,
		IDStrategy:       idStrategy,
		Graphs:           make([]GraphManifest, 0, graphCount),
	}
}

func (s Manifest) validate() error {
	if !isSupportedManifestFormat(s.Format) {
		return fmt.Errorf("unsupported manifest format %q", s.Format)
	}

	if s.IDStrategy != idStrategy {
		return fmt.Errorf("unsupported ID strategy %q", s.IDStrategy)
	}

	if err := validateCompression(s.Compression); err != nil {
		return err
	}

	switch s.Scrub.Mode {
	case ScrubNone, ScrubFull:
	default:
		return fmt.Errorf("unsupported scrub mode %q", s.Scrub.Mode)
	}

	if s.Source.GraphCount != len(s.Graphs) {
		return fmt.Errorf("manifest graph_count %d does not match %d graph entries", s.Source.GraphCount, len(s.Graphs))
	}

	seenGraphs := map[string]struct{}{}
	for _, graphEntry := range s.Graphs {
		if graphEntry.Name == "" {
			return fmt.Errorf("manifest graph entry has empty name")
		}

		if _, seen := seenGraphs[graphEntry.Name]; seen {
			return fmt.Errorf("manifest contains duplicate graph %q", graphEntry.Name)
		}

		seenGraphs[graphEntry.Name] = struct{}{}

		var nodeFiles, edgeFiles int64
		seenEdgePhase := false

		for _, fileEntry := range graphEntry.Files {
			if fileEntry.Path == "" {
				return fmt.Errorf("manifest graph %q contains empty file path", graphEntry.Name)
			}

			switch fileEntry.Phase {
			case PhaseNodes:
				if seenEdgePhase {
					return fmt.Errorf("manifest graph %q lists node file after edge file", graphEntry.Name)
				}

				nodeFiles += int64(fileEntry.Count)

			case PhaseEdges:
				seenEdgePhase = true
				edgeFiles += int64(fileEntry.Count)

			default:
				return fmt.Errorf("manifest graph %q contains unsupported phase %q", graphEntry.Name, fileEntry.Phase)
			}

			if fileEntry.Count < 0 {
				return fmt.Errorf("manifest file %q has negative count", fileEntry.Path)
			}

			if fileEntry.CompressedBytes < 0 || fileEntry.UncompressedBytes < 0 {
				return fmt.Errorf("manifest file %q has negative byte size", fileEntry.Path)
			}

			if fileEntry.SHA256 == "" {
				return fmt.Errorf("manifest file %q is missing sha256", fileEntry.Path)
			}
		}

		if graphEntry.NodeCount != nodeFiles {
			return fmt.Errorf("manifest graph %q node_count %d does not match node file total %d", graphEntry.Name, graphEntry.NodeCount, nodeFiles)
		}

		if graphEntry.EdgeCount != edgeFiles {
			return fmt.Errorf("manifest graph %q edge_count %d does not match edge file total %d", graphEntry.Name, graphEntry.EdgeCount, edgeFiles)
		}
	}

	if s.Metrics != nil {
		if err := validateMetricsManifest(*s.Metrics, s.Graphs); err != nil {
			return err
		}
	}

	return nil
}

func (s Manifest) Validate() error {
	return s.validate()
}

func isSupportedManifestFormat(format string) bool {
	return format == manifestFormat
}

func IsSupportedManifestFormat(format string) bool {
	return isSupportedManifestFormat(format)
}

func graphSchemaFromMetadata(metadata GraphSchemaMetadata) graph.Graph {
	return graph.Graph{
		Name:  metadata.Name,
		Nodes: graph.StringsToKinds(metadata.NodeKinds),
		Edges: graph.StringsToKinds(metadata.EdgeKinds),
	}
}

func GraphSchemaFromMetadata(metadata GraphSchemaMetadata) graph.Graph {
	return graphSchemaFromMetadata(metadata)
}

func stringsFromKindSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}

	sort.Strings(values)

	return values
}

func addKindsToSet(set map[string]struct{}, values []string) {
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
}

func addActionCounts(target map[string]int, source map[string]int) {
	for key, count := range source {
		target[key] += count
	}
}

func cloneActionCounts(source map[string]int) map[string]int {
	if len(source) == 0 {
		return map[string]int{}
	}

	target := make(map[string]int, len(source))
	for key, count := range source {
		target[key] = count
	}

	return target
}
