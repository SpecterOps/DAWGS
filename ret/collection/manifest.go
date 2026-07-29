package collection

import (
	"encoding/hex"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

const (
	Format       = "ret-collection-v1"
	ManifestName = "manifest.json"
)

type Manifest struct {
	Format    string        `json:"format"`
	CreatedAt time.Time     `json:"created_at"`
	Outputs   OutputConfig  `json:"outputs"`
	Scrub     ScrubMetadata `json:"scrub"`
	Graphs    []Graph       `json:"graphs"`
}

type OutputConfig struct {
	JSONL   *JSONLOutput   `json:"jsonl,omitempty"`
	Parquet *ParquetOutput `json:"parquet,omitempty"`
}

type JSONLOutput struct {
	SchemaVersion string `json:"schema_version"`
	Codec         string `json:"codec"`
	Level         int    `json:"level"`
}

type ParquetOutput struct {
	SchemaVersion string `json:"schema_version"`
}

type ScrubMetadata struct {
	Enabled          bool   `json:"enabled"`
	RulesFingerprint string `json:"rules_fingerprint,omitempty"`
	SaltFingerprint  string `json:"salt_fingerprint,omitempty"`
}

type Graph struct {
	Name               string               `json:"name"`
	NodeCount          int64                `json:"node_count"`
	RelationshipCount  int64                `json:"relationship_count"`
	KindCatalog        []string             `json:"kind_catalog"`
	NodeShards         []NodeShard          `json:"node_shards"`
	RelationshipShards []RelationshipShard  `json:"relationship_shards"`
	Metrics            metrics.GraphMetrics `json:"metrics"`
}

func (s Manifest) Validate() error {
	if s.Format != Format {
		return fmt.Errorf("collection format %q does not match %q", s.Format, Format)
	}
	if s.CreatedAt.IsZero() {
		return fmt.Errorf("collection created_at is required")
	}
	_, offset := s.CreatedAt.Zone()
	if offset != 0 {
		return fmt.Errorf("collection created_at must be UTC")
	}
	if err := s.Outputs.validate(); err != nil {
		return err
	}
	if err := s.Scrub.validate(); err != nil {
		return err
	}

	graphNames := make(map[string]struct{}, len(s.Graphs))
	artifactPaths := make(map[string]struct{})
	for graphIndex := range s.Graphs {
		graph := &s.Graphs[graphIndex]
		if err := validateGraphName(graph.Name); err != nil {
			return fmt.Errorf("graph %d graph name: %w", graphIndex+1, err)
		}
		if _, found := graphNames[graph.Name]; found {
			return fmt.Errorf("duplicate graph name %q", graph.Name)
		}
		graphNames[graph.Name] = struct{}{}

		if err := validateGraph(*graph, s.Outputs, s.Scrub.Enabled, artifactPaths); err != nil {
			return fmt.Errorf("graph %q: %w", graph.Name, err)
		}
	}

	return nil
}

func (s OutputConfig) validate() error {
	if s.JSONL == nil && s.Parquet == nil {
		return fmt.Errorf("collection must enable at least one output")
	}
	if s.JSONL != nil {
		if s.JSONL.SchemaVersion != jsonl.SchemaVersion {
			return fmt.Errorf("collection JSONL schema %q does not match %q", s.JSONL.SchemaVersion, jsonl.SchemaVersion)
		}
		config := jsonl.Config{
			Enabled: true,
			Codec:   jsonl.Codec(s.JSONL.Codec),
			Level:   s.JSONL.Level,
		}
		if err := config.Validate(); err != nil {
			return fmt.Errorf("collection JSONL output: %w", err)
		}
	}
	if s.Parquet != nil && s.Parquet.SchemaVersion != parquet.SchemaVersion {
		return fmt.Errorf("collection Parquet schema %q does not match %q", s.Parquet.SchemaVersion, parquet.SchemaVersion)
	}

	return nil
}

func (s ScrubMetadata) validate() error {
	if s.Enabled {
		if !isLowerHexDigest(s.RulesFingerprint) {
			return fmt.Errorf("enabled scrub rules fingerprint must be 64 lowercase hexadecimal characters")
		}
		if !isLowerHexDigest(s.SaltFingerprint) {
			return fmt.Errorf("enabled scrub salt fingerprint must be 64 lowercase hexadecimal characters")
		}
		return nil
	}
	if s.RulesFingerprint != "" || s.SaltFingerprint != "" {
		return fmt.Errorf("disabled scrub metadata must not contain fingerprints")
	}

	return nil
}

func validateGraphName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("is empty")
	}
	if name == "." || name == ".." || path.Clean(name) != name {
		return fmt.Errorf("%q is not a clean path segment", name)
	}
	if strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, '\x00') {
		return fmt.Errorf("%q is not a single safe path segment", name)
	}

	return nil
}

func validateGraph(graph Graph, outputs OutputConfig, scrubEnabled bool, artifactPaths map[string]struct{}) error {
	if graph.NodeCount < 0 {
		return fmt.Errorf("node count must not be negative: %d", graph.NodeCount)
	}
	if graph.RelationshipCount < 0 {
		return fmt.Errorf("relationship count must not be negative: %d", graph.RelationshipCount)
	}
	if err := validateKindCatalog(graph.KindCatalog); err != nil {
		return err
	}

	empty := graph.NodeCount == 0 && graph.RelationshipCount == 0
	if empty && (len(graph.NodeShards) != 0 || len(graph.RelationshipShards) != 0) {
		return fmt.Errorf("empty graph must not contain shards")
	}

	nodeTotal, err := validateNodeShards(graph.Name, graph.NodeShards, outputs, scrubEnabled, artifactPaths)
	if err != nil {
		return err
	}
	if nodeTotal != graph.NodeCount {
		return fmt.Errorf("node shard total %d does not match graph node count %d", nodeTotal, graph.NodeCount)
	}

	relationshipTotal, err := validateRelationshipShards(graph.Name, graph.RelationshipShards, outputs, scrubEnabled, artifactPaths)
	if err != nil {
		return err
	}
	if relationshipTotal != graph.RelationshipCount {
		return fmt.Errorf("relationship shard total %d does not match graph relationship count %d", relationshipTotal, graph.RelationshipCount)
	}

	if err := validateMetrics(graph.Metrics, graph.NodeCount, graph.RelationshipCount); err != nil {
		return err
	}

	return nil
}

func validateKindCatalog(catalog []string) error {
	seen := make(map[string]struct{}, len(catalog))
	for index, kind := range catalog {
		if kind == "" {
			return fmt.Errorf("kind catalog entry %d is empty", index+1)
		}
		if _, found := seen[kind]; found {
			return fmt.Errorf("kind catalog entry %d duplicates %q", index+1, kind)
		}
		seen[kind] = struct{}{}
	}

	return nil
}

func validateNodeShards(
	graph string,
	shards []NodeShard,
	outputs OutputConfig,
	scrubEnabled bool,
	artifactPaths map[string]struct{},
) (int64, error) {
	var total int64
	var lastSourceID uint64
	for offset, shard := range shards {
		if err := validateLogicalShard(
			"node",
			offset,
			shard.Index,
			shard.Count,
			shard.LastSourceID,
			shard.ScrubCounts,
			scrubEnabled,
			shard.JSONL != nil,
			shard.Parquet != nil,
			outputs,
			lastSourceID,
		); err != nil {
			return 0, err
		}
		lastSourceID = shard.LastSourceID

		if shard.JSONL != nil {
			expected := NodeJSONLPath(graph, shard.Index, jsonl.Codec(outputs.JSONL.Codec))
			if err := validateJSONLArtifact(
				"node",
				shard.Index,
				shard.Count,
				shard.JSONL.SchemaVersion,
				shard.JSONL.Path,
				shard.JSONL.Codec,
				shard.JSONL.SHA256,
				shard.JSONL.Level,
				shard.JSONL.Count,
				shard.JSONL.UncompressedBytes,
				shard.JSONL.StoredBytes,
				expected,
				*outputs.JSONL,
				artifactPaths,
			); err != nil {
				return 0, err
			}
		}
		if shard.Parquet != nil {
			expected := NodeParquetPath(graph, shard.Index)
			if err := validateParquetArtifact(
				"node",
				shard.Index,
				shard.Count,
				shard.Parquet.SchemaVersion,
				shard.Parquet.Path,
				shard.Parquet.SHA256,
				shard.Parquet.Count,
				shard.Parquet.StoredBytes,
				expected,
				artifactPaths,
			); err != nil {
				return 0, err
			}
		}

		var ok bool
		total, ok = addNonnegative(total, shard.Count)
		if !ok {
			return 0, fmt.Errorf("node shard total overflows int64")
		}
	}

	return total, nil
}

func validateRelationshipShards(
	graph string,
	shards []RelationshipShard,
	outputs OutputConfig,
	scrubEnabled bool,
	artifactPaths map[string]struct{},
) (int64, error) {
	var total int64
	var lastSourceID uint64
	for offset, shard := range shards {
		if err := validateLogicalShard(
			"relationship",
			offset,
			shard.Index,
			shard.Count,
			shard.LastSourceID,
			shard.ScrubCounts,
			scrubEnabled,
			shard.JSONL != nil,
			shard.Parquet != nil,
			outputs,
			lastSourceID,
		); err != nil {
			return 0, err
		}
		lastSourceID = shard.LastSourceID

		if shard.JSONL != nil {
			expected := RelationshipJSONLPath(graph, shard.Index, jsonl.Codec(outputs.JSONL.Codec))
			if err := validateJSONLArtifact(
				"relationship",
				shard.Index,
				shard.Count,
				shard.JSONL.SchemaVersion,
				shard.JSONL.Path,
				shard.JSONL.Codec,
				shard.JSONL.SHA256,
				shard.JSONL.Level,
				shard.JSONL.Count,
				shard.JSONL.UncompressedBytes,
				shard.JSONL.StoredBytes,
				expected,
				*outputs.JSONL,
				artifactPaths,
			); err != nil {
				return 0, err
			}
		}
		if shard.Parquet != nil {
			expected := RelationshipParquetPath(graph, shard.Index)
			if err := validateParquetArtifact(
				"relationship",
				shard.Index,
				shard.Count,
				shard.Parquet.SchemaVersion,
				shard.Parquet.Path,
				shard.Parquet.SHA256,
				shard.Parquet.Count,
				shard.Parquet.StoredBytes,
				expected,
				artifactPaths,
			); err != nil {
				return 0, err
			}
		}

		var ok bool
		total, ok = addNonnegative(total, shard.Count)
		if !ok {
			return 0, fmt.Errorf("relationship shard total overflows int64")
		}
	}

	return total, nil
}

func validateLogicalShard(
	entityType string,
	offset, index int,
	count int64,
	lastSourceID uint64,
	counts scrub.ActionCounts,
	scrubEnabled, hasJSONL, hasParquet bool,
	outputs OutputConfig,
	previousSourceID uint64,
) error {
	expectedIndex := offset + 1
	if index != expectedIndex {
		return fmt.Errorf("%s shard index: got %d want %d", entityType, index, expectedIndex)
	}
	if hasJSONL != (outputs.JSONL != nil) || hasParquet != (outputs.Parquet != nil) {
		return fmt.Errorf("%s shard %d output mismatch with globally enabled outputs", entityType, index)
	}
	if count <= 0 {
		return fmt.Errorf("%s shard %d count must be positive", entityType, index)
	}
	if lastSourceID == 0 {
		return fmt.Errorf("%s shard %d last source ID must be nonzero", entityType, index)
	}
	if previousSourceID != 0 && lastSourceID <= previousSourceID {
		return fmt.Errorf("%s shard %d last source ID %d does not increase after %d", entityType, index, lastSourceID, previousSourceID)
	}
	if err := validateScrubCounts(counts, scrubEnabled); err != nil {
		return fmt.Errorf("%s shard %d: %w", entityType, index, err)
	}

	return nil
}

func validateScrubCounts(counts scrub.ActionCounts, enabled bool) error {
	if !enabled && len(counts) != 0 {
		return fmt.Errorf("scrub counts are present while scrubbing is disabled")
	}
	for action, count := range counts {
		if action == "" {
			return fmt.Errorf("scrub count has an empty action name")
		}
		if count < 0 {
			return fmt.Errorf("scrub count for %q must not be negative", action)
		}
	}

	return nil
}

func validateJSONLArtifact(
	entityType string,
	shardIndex int,
	shardCount int64,
	schemaVersion, artifactPath, codec, sha256 string,
	level int,
	count, uncompressedBytes, storedBytes int64,
	expectedPath string,
	output JSONLOutput,
	artifactPaths map[string]struct{},
) error {
	prefix := fmt.Sprintf("%s shard %d JSONL", entityType, shardIndex)
	if schemaVersion != output.SchemaVersion {
		return fmt.Errorf("%s schema %q does not match configured JSONL schema %q", prefix, schemaVersion, output.SchemaVersion)
	}
	if codec != output.Codec {
		return fmt.Errorf("%s codec %q does not match configured JSONL codec %q", prefix, codec, output.Codec)
	}
	if level != output.Level {
		return fmt.Errorf("%s level %d does not match configured JSONL level %d", prefix, level, output.Level)
	}
	if count != shardCount {
		return fmt.Errorf("%s count %d does not match shard count %d", prefix, count, shardCount)
	}
	if !isLowerHexDigest(sha256) {
		return fmt.Errorf("%s SHA-256 must be 64 lowercase hexadecimal characters", prefix)
	}
	if uncompressedBytes <= 0 {
		return fmt.Errorf("%s uncompressed bytes must be positive", prefix)
	}
	if storedBytes <= 0 {
		return fmt.Errorf("%s stored bytes must be positive", prefix)
	}
	if err := validateArtifactPath(artifactPath, expectedPath, artifactPaths); err != nil {
		return fmt.Errorf("%s path: %w", prefix, err)
	}

	return nil
}

func validateParquetArtifact(
	entityType string,
	shardIndex int,
	shardCount int64,
	schemaVersion, artifactPath, sha256 string,
	count, storedBytes int64,
	expectedPath string,
	artifactPaths map[string]struct{},
) error {
	prefix := fmt.Sprintf("%s shard %d Parquet", entityType, shardIndex)
	if schemaVersion != parquet.SchemaVersion {
		return fmt.Errorf("%s schema %q does not match %q", prefix, schemaVersion, parquet.SchemaVersion)
	}
	if count != shardCount {
		return fmt.Errorf("%s count %d does not match shard count %d", prefix, count, shardCount)
	}
	if !isLowerHexDigest(sha256) {
		return fmt.Errorf("%s SHA-256 must be 64 lowercase hexadecimal characters", prefix)
	}
	if storedBytes <= 0 {
		return fmt.Errorf("%s stored bytes must be positive", prefix)
	}
	if err := validateArtifactPath(artifactPath, expectedPath, artifactPaths); err != nil {
		return fmt.Errorf("%s path: %w", prefix, err)
	}

	return nil
}

func validateArtifactPath(artifactPath, expectedPath string, paths map[string]struct{}) error {
	if _, err := SafeJoin(".", artifactPath); err != nil {
		return err
	}
	if artifactPath != expectedPath {
		return fmt.Errorf("%q does not match deterministic path %q", artifactPath, expectedPath)
	}
	if _, found := paths[artifactPath]; found {
		return fmt.Errorf("%q is duplicated", artifactPath)
	}
	paths[artifactPath] = struct{}{}

	return nil
}

func validateMetrics(value metrics.GraphMetrics, nodeCount, relationshipCount int64) error {
	if value.NodeCount != nodeCount {
		return fmt.Errorf("metrics node count %d does not match graph node count %d", value.NodeCount, nodeCount)
	}
	if value.RelationshipCount != relationshipCount {
		return fmt.Errorf("metrics relationship count %d does not match graph relationship count %d", value.RelationshipCount, relationshipCount)
	}

	nodeSequenceTotal, err := validateHistogram(value.NodeKindSequences, func(key string) error {
		_, err := parseOrderedKindsKey(key)
		return err
	})
	if err != nil {
		return fmt.Errorf("metrics node kind sequences: %w", err)
	}
	if nodeSequenceTotal != nodeCount {
		return fmt.Errorf("metrics node kind sequences sum %d does not match node count %d", nodeSequenceTotal, nodeCount)
	}

	relationshipKindTotal, err := validateHistogram(value.RelationshipKinds, func(key string) error {
		if key == "" {
			return fmt.Errorf("key is empty")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("metrics relationship kinds: %w", err)
	}
	if relationshipKindTotal != relationshipCount {
		return fmt.Errorf("metrics relationship kinds sum %d does not match relationship count %d", relationshipKindTotal, relationshipCount)
	}

	if err := validateDegreeHistogram("inbound", value.InboundDegreeHistogram, nodeCount, relationshipCount); err != nil {
		return err
	}
	if err := validateDegreeHistogram("outbound", value.OutboundDegreeHistogram, nodeCount, relationshipCount); err != nil {
		return err
	}

	endpointTotal, err := validateHistogram(value.EndpointShapeHistogram, validateEndpointShapeKey)
	if err != nil {
		return fmt.Errorf("metrics endpoint shape histogram: %w", err)
	}
	if endpointTotal != relationshipCount {
		return fmt.Errorf("metrics endpoint shape histogram sum %d does not match relationship count %d", endpointTotal, relationshipCount)
	}
	if !strings.HasPrefix(value.Fingerprint, "sha256:") || !isLowerHexDigest(strings.TrimPrefix(value.Fingerprint, "sha256:")) {
		return fmt.Errorf("metrics fingerprint must have sha256: followed by 64 lowercase hexadecimal characters")
	}

	return nil
}

func validateHistogram(histogram map[string]int64, validateKey func(string) error) (int64, error) {
	var total int64
	for key, count := range histogram {
		if err := validateKey(key); err != nil {
			return 0, fmt.Errorf("invalid key %q: %w", key, err)
		}
		if count <= 0 {
			return 0, fmt.Errorf("count for %q must be positive", key)
		}
		var ok bool
		total, ok = addNonnegative(total, count)
		if !ok {
			return 0, fmt.Errorf("counts overflow int64")
		}
	}

	return total, nil
}

func validateDegreeHistogram(name string, histogram map[string]int64, nodeCount, relationshipCount int64) error {
	var weightedTotal int64
	histogramTotal, err := validateHistogram(histogram, func(key string) error {
		degree, parseErr := strconv.ParseInt(key, 10, 64)
		if parseErr != nil || degree < 0 || strconv.FormatInt(degree, 10) != key {
			return fmt.Errorf("degree must be a canonical nonnegative integer")
		}
		count := histogram[key]
		if degree != 0 && count > math.MaxInt64/degree {
			return fmt.Errorf("weighted degree overflows int64")
		}
		if weightedTotal > math.MaxInt64-degree*count {
			return fmt.Errorf("weighted degree total overflows int64")
		}
		weightedTotal += degree * count
		return nil
	})
	if err != nil {
		return fmt.Errorf("metrics %s degree histogram: %w", name, err)
	}
	if histogramTotal != nodeCount {
		return fmt.Errorf("metrics %s degree histogram sum %d does not match node count %d", name, histogramTotal, nodeCount)
	}
	if weightedTotal != relationshipCount {
		return fmt.Errorf(
			"metrics %s degree histogram: %s degree total %d does not match relationship count %d",
			name,
			name,
			weightedTotal,
			relationshipCount,
		)
	}

	return nil
}

func validateEndpointShapeKey(key string) error {
	segments, err := parseOrderedKindsKey(key)
	if err != nil {
		return err
	}
	if len(segments) != 3 {
		return fmt.Errorf("endpoint shape must contain exactly three segments")
	}
	if _, err := parseOrderedKindsKey(segments[0]); err != nil {
		return fmt.Errorf("start kind sequence: %w", err)
	}
	if segments[1] == "" {
		return fmt.Errorf("relationship kind is empty")
	}
	if _, err := parseOrderedKindsKey(segments[2]); err != nil {
		return fmt.Errorf("end kind sequence: %w", err)
	}

	return nil
}

func parseOrderedKindsKey(key string) ([]string, error) {
	segments := make([]string, 0)
	for offset := 0; offset < len(key); {
		colon := strings.IndexByte(key[offset:], ':')
		if colon < 1 {
			return nil, fmt.Errorf("missing length prefix")
		}
		colon += offset
		lengthText := key[offset:colon]
		length, err := strconv.Atoi(lengthText)
		if err != nil || length < 0 || strconv.Itoa(length) != lengthText {
			return nil, fmt.Errorf("invalid length prefix %q", lengthText)
		}
		start := colon + 1
		end := start + length
		if end < start || end > len(key) {
			return nil, fmt.Errorf("segment length %d exceeds remaining key", length)
		}
		segments = append(segments, key[start:end])
		offset = end
	}

	return segments, nil
}

func isLowerHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	if _, err := hex.DecodeString(value); err != nil {
		return false
	}
	return strings.ToLower(value) == value
}

func addNonnegative(left, right int64) (int64, bool) {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return 0, false
	}
	return left + right, true
}
