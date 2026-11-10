package retriever

import (
	"fmt"
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"
)

const (
	// manifestFormat identifies the current JSONL collection manifest wire format.
	manifestFormat = "retriever-jsonl-collection-v1"

	// idStrategy identifies source database IDs serialized as strings for import remapping.
	idStrategy = "source_database_id_string"
)

// Phase identifies the node or relationship stage of a fragment.
type Phase string

const (
	// PhaseNodes identifies fragments containing node records.
	PhaseNodes Phase = "nodes"

	// PhaseEdges identifies fragments containing relationship records.
	PhaseEdges Phase = "edges"
)

// CompressionCodec identifies fragment compression and also carries an unset option value.
type CompressionCodec string

const (
	// CompressionDisabled marks an unset compression option rather than a valid manifest codec.
	CompressionDisabled CompressionCodec = ""

	// CompressionNone stores JSONL fragments without compression.
	CompressionNone CompressionCodec = "none"

	// CompressionGzip stores fragments using gzip compression.
	CompressionGzip CompressionCodec = "gzip"

	// CompressionZstd stores fragments using Zstandard compression.
	CompressionZstd CompressionCodec = "zstd"
)

// ScrubMode identifies the property scrub policy recorded in a collection.
type ScrubMode string

const (
	// ScrubNone preserves source properties without applying scrub rules.
	ScrubNone ScrubMode = "none"

	// ScrubFull applies the configured property scrub rules.
	ScrubFull ScrubMode = "full"
)

// Manifest describes a versioned collection of graph fragments and optional verification metrics.
type Manifest struct {
	// Format identifies the supported collection manifest format.
	Format string `json:"format"`

	// GeneratedAt records when the collection manifest was created.
	GeneratedAt time.Time `json:"generated_at"`

	// RetrieverVersion identifies the producing retriever build when available.
	RetrieverVersion string `json:"retriever_version,omitempty"`

	// Driver names the source database backend.
	Driver string `json:"driver"`

	// Source summarizes the source graph inventory.
	Source SourceMetadata `json:"source"`

	// Compression selects fragment compression; use CompressionNone for uncompressed JSONL.
	Compression CompressionCodec `json:"compression"`

	// CompressionLevel records the codec-specific compression level when supplied.
	CompressionLevel int `json:"compression_level,omitempty"`

	// Scrub records the applied scrub policy and aggregate action counts.
	Scrub ScrubMetadata `json:"scrub"`

	// IDStrategy identifies how source database IDs are represented in fragments.
	IDStrategy string `json:"id_strategy"`

	// Schema retains graph and kind declarations independently of entity fragments.
	Schema SchemaMetadata `json:"schema"`

	// Graphs lists each graph and its ordered node-then-edge fragments.
	Graphs []GraphManifest `json:"graphs"`

	// Metrics contains optional source metrics used to verify a later load.
	Metrics *MetricsManifest `json:"metrics,omitempty"`

	// Warnings records nonfatal observations made while producing the collection.
	Warnings []string `json:"warnings,omitempty"`
}

// SourceMetadata summarizes the graph inventory represented by a collection.
type SourceMetadata struct {
	// GraphCount must equal the number of graph entries in the manifest.
	GraphCount int `json:"graph_count"`
}

// ScrubMetadata records the scrub mode and per-entity action totals for a collection.
type ScrubMetadata struct {
	// Mode selects whether the collection was left intact or scrubbed.
	Mode ScrubMode `json:"mode"`

	// RulesVersion identifies the applied scrub rules when scrubbing is enabled.
	RulesVersion string `json:"rules_version,omitempty"`

	// SaltProvided records whether the producer received a caller-supplied scrub salt.
	SaltProvided bool `json:"salt_provided"`

	// NodeActionCounts counts scrub actions on node properties by action name.
	NodeActionCounts map[string]int `json:"node_action_counts"`

	// EdgeActionCounts counts scrub actions on relationship properties by action name.
	EdgeActionCounts map[string]int `json:"edge_action_counts"`
}

// SchemaMetadata retains graph schema declarations for collection import.
type SchemaMetadata struct {
	// Graphs contains graph names and their node and relationship kinds.
	Graphs []GraphSchemaMetadata `json:"graphs"`
}

// GraphSchemaMetadata describes one graph's named kinds independently of entity counts.
type GraphSchemaMetadata struct {
	// Name is the graph name used when reconstructing its schema.
	Name string `json:"name"`

	// NodeKinds contains the declared node kind names.
	NodeKinds []string `json:"node_kinds"`

	// EdgeKinds contains the declared relationship kind names.
	EdgeKinds []string `json:"edge_kinds"`
}

// GraphManifest describes entity counts and ordered fragment files for one graph.
type GraphManifest struct {
	// Name is the graph name preserved by the collection.
	Name string `json:"name"`

	// NodeCount must equal the sum of node fragment record counts.
	NodeCount int64 `json:"node_count"`

	// EdgeCount must equal the sum of relationship fragment record counts.
	EdgeCount int64 `json:"edge_count"`

	// NodeActionCounts counts node-property scrub actions within this graph.
	NodeActionCounts map[string]int `json:"node_action_counts"`

	// EdgeActionCounts counts relationship-property scrub actions within this graph.
	EdgeActionCounts map[string]int `json:"edge_action_counts"`

	// Files lists node fragments before relationship fragments for ordered loading.
	Files []FileManifest `json:"files"`
}

// FileManifest records integrity metadata and entity counts for one collection fragment.
type FileManifest struct {
	// Phase identifies whether records contain nodes or relationships.
	Phase Phase `json:"phase"`

	// Path locates the fragment relative to the collection directory.
	Path string `json:"path"`

	// Count is the nonnegative number of entity records in the fragment.
	Count int `json:"count"`

	// CompressedBytes is the stored fragment size in bytes.
	CompressedBytes int64 `json:"compressed_bytes"`

	// UncompressedBytes is the decoded JSONL size in bytes.
	UncompressedBytes int64 `json:"uncompressed_bytes"`

	// SHA256 is the integrity digest associated with the stored fragment.
	SHA256 string `json:"sha256"`

	// ActionCounts counts property scrub actions applied to this fragment by action name.
	ActionCounts map[string]int `json:"action_counts"`
}

// FragmentNode is a JSONL node record retaining source identity for endpoint remapping.
type FragmentNode struct {
	// ID is the source database node identifier encoded as a string.
	ID string `json:"id"`

	// Kinds contains the node's kind names.
	Kinds []string `json:"kinds"`

	// Properties contains dynamic graph properties and is omitted when empty.
	Properties map[string]any `json:"properties,omitempty"`
}

// FragmentEdge is a JSONL relationship record whose endpoints refer to source node IDs.
type FragmentEdge struct {
	// StartID is the source identifier of the start node.
	StartID string `json:"start_id"`

	// EndID is the source identifier of the end node.
	EndID string `json:"end_id"`

	// Kind is the relationship kind name.
	Kind string `json:"kind"`

	// Properties contains dynamic graph properties and is omitted when empty.
	Properties map[string]any `json:"properties,omitempty"`
}

// newManifest initializes a collection manifest with its format, source count, codec, and scrub policy.
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

// validate checks supported vocabulary, graph uniqueness, fragment ordering, and count consistency.
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

// Validate checks manifest metadata and optional metrics without opening or verifying fragment files.
func (s Manifest) Validate() error {
	return s.validate()
}

// isSupportedManifestFormat reports whether format exactly matches the current collection format.
func isSupportedManifestFormat(format string) bool {
	return format == manifestFormat
}

// IsSupportedManifestFormat reports whether the collection reader accepts the exact format identifier.
func IsSupportedManifestFormat(format string) bool {
	return isSupportedManifestFormat(format)
}

// graphSchemaFromMetadata converts recorded kind names into a graph schema declaration.
func graphSchemaFromMetadata(metadata GraphSchemaMetadata) graph.Graph {
	return graph.Graph{
		Name:  metadata.Name,
		Nodes: graph.StringsToKinds(metadata.NodeKinds),
		Edges: graph.StringsToKinds(metadata.EdgeKinds),
	}
}

// GraphSchemaFromMetadata reconstructs a graph declaration from recorded graph and kind names.
func GraphSchemaFromMetadata(metadata GraphSchemaMetadata) graph.Graph {
	return graphSchemaFromMetadata(metadata)
}

// stringsFromKindSet returns the set's keys in deterministic lexical order.
func stringsFromKindSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}

	sort.Strings(values)

	return values
}

// addKindsToSet merges nonempty kind names into the supplied mutable set.
func addKindsToSet(set map[string]struct{}, values []string) {
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
}

// addActionCounts accumulates each source action count into the mutable target map.
func addActionCounts(target map[string]int, source map[string]int) {
	for key, count := range source {
		target[key] += count
	}
}

// cloneActionCounts copies action totals into a new non-nil map, including for empty input.
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
