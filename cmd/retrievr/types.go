package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"
)

const (
	manifestFormat = "retrievr-opengraph-collection-v1"
	idStrategy     = "source_database_id_string"
)

type phase string

const (
	phaseNodes phase = "nodes"
	phaseEdges phase = "edges"
)

type compressionCodec string

const (
	compressionNone compressionCodec = ""
	compressionGzip compressionCodec = "gzip"
	compressionZstd compressionCodec = "zstd"
)

type scrubMode string

const (
	scrubNone scrubMode = "none"
	scrubFull scrubMode = "full"
)

type manifest struct {
	Format           string           `json:"format"`
	GeneratedAt      time.Time        `json:"generated_at"`
	RetrievrVersion  string           `json:"retrievr_version,omitempty"`
	Driver           string           `json:"driver"`
	Source           sourceMetadata   `json:"source"`
	Compression      compressionCodec `json:"compression"`
	CompressionLevel int              `json:"compression_level,omitempty"`
	Scrub            scrubMetadata    `json:"scrub"`
	IDStrategy       string           `json:"id_strategy"`
	Schema           schemaMetadata   `json:"schema"`
	Graphs           []graphManifest  `json:"graphs"`
	Warnings         []string         `json:"warnings,omitempty"`
}

type sourceMetadata struct {
	GraphCount int `json:"graph_count"`
}

type scrubMetadata struct {
	Mode             scrubMode      `json:"mode"`
	RulesVersion     string         `json:"rules_version,omitempty"`
	SaltProvided     bool           `json:"salt_provided"`
	NodeActionCounts map[string]int `json:"node_action_counts"`
	EdgeActionCounts map[string]int `json:"edge_action_counts"`
}

type schemaMetadata struct {
	Graphs []graphSchemaMetadata `json:"graphs"`
}

type graphSchemaMetadata struct {
	Name      string   `json:"name"`
	NodeKinds []string `json:"node_kinds"`
	EdgeKinds []string `json:"edge_kinds"`
}

type graphManifest struct {
	Name             string         `json:"name"`
	NodeCount        int64          `json:"node_count"`
	EdgeCount        int64          `json:"edge_count"`
	NodeActionCounts map[string]int `json:"node_action_counts"`
	EdgeActionCounts map[string]int `json:"edge_action_counts"`
	Files            []fileManifest `json:"files"`
}

type fileManifest struct {
	Phase             phase          `json:"phase"`
	Path              string         `json:"path"`
	Count             int            `json:"count"`
	CompressedBytes   int64          `json:"compressed_bytes"`
	UncompressedBytes int64          `json:"uncompressed_bytes"`
	SHA256            string         `json:"sha256"`
	ActionCounts      map[string]int `json:"action_counts"`
}

type nodeFragment struct {
	Phase phase          `json:"phase"`
	Items []fragmentNode `json:"items"`
}

type fragmentNode struct {
	ID         string         `json:"id"`
	Kinds      []string       `json:"kinds"`
	Properties map[string]any `json:"properties,omitempty"`
}

type edgeFragment struct {
	Phase phase          `json:"phase"`
	Items []fragmentEdge `json:"items"`
}

type fragmentEdge struct {
	StartID    string         `json:"start_id"`
	EndID      string         `json:"end_id"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties,omitempty"`
}

func newManifest(driverName string, codec compressionCodec, compressionLevel int, scrub scrubMetadata, graphCount int) manifest {
	return manifest{
		Format:      manifestFormat,
		GeneratedAt: time.Now().UTC(),
		Driver:      driverName,
		Source: sourceMetadata{
			GraphCount: graphCount,
		},
		Compression:      codec,
		CompressionLevel: compressionLevel,
		Scrub:            scrub,
		IDStrategy:       idStrategy,
		Graphs:           make([]graphManifest, 0, graphCount),
	}
}

func (s manifest) validate() error {
	if s.Format != manifestFormat {
		return fmt.Errorf("unsupported manifest format %q", s.Format)
	}
	if s.IDStrategy != idStrategy {
		return fmt.Errorf("unsupported ID strategy %q", s.IDStrategy)
	}
	if err := validateCompression(s.Compression); err != nil {
		return err
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
			case phaseNodes:
				if seenEdgePhase {
					return fmt.Errorf("manifest graph %q lists node file after edge file", graphEntry.Name)
				}
				nodeFiles += int64(fileEntry.Count)
			case phaseEdges:
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

	return nil
}

func graphSchemaFromMetadata(metadata graphSchemaMetadata) graph.Graph {
	return graph.Graph{
		Name:  metadata.Name,
		Nodes: graph.StringsToKinds(metadata.NodeKinds),
		Edges: graph.StringsToKinds(metadata.EdgeKinds),
	}
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
