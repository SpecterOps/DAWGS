package collection

import (
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
	"github.com/stretchr/testify/require"
)

func TestManifestAllowsEverySupportedOutputCombination(t *testing.T) {
	t.Run("JSONL only", func(t *testing.T) {
		manifest := fixtureManifest()
		manifest.Outputs.Parquet = nil
		manifest.Graphs[0].NodeShards[0].Parquet = nil
		manifest.Graphs[0].RelationshipShards[0].Parquet = nil

		require.NoError(t, manifest.Validate())
	})

	t.Run("Parquet only", func(t *testing.T) {
		manifest := fixtureManifest()
		manifest.Outputs.JSONL = nil
		manifest.Graphs[0].NodeShards[0].JSONL = nil
		manifest.Graphs[0].RelationshipShards[0].JSONL = nil

		require.NoError(t, manifest.Validate())
	})

	t.Run("both", func(t *testing.T) {
		require.NoError(t, fixtureManifest().Validate())
	})
}

func TestManifestAllowsEmptyGraphWithoutShards(t *testing.T) {
	manifest := fixtureManifest()
	manifest.Graphs = append(manifest.Graphs, Graph{
		Name:        "empty",
		KindCatalog: []string{},
		Metrics:     metrics.NewBuilder().Finalize(),
	})

	require.NoError(t, manifest.Validate())
}

func TestManifestRejectsInvalidTopLevelMetadata(t *testing.T) {
	tests := map[string]struct {
		mutate  func(*Manifest)
		message string
	}{
		"format": {
			mutate:  func(value *Manifest) { value.Format = "ret-collection-v2" },
			message: "format",
		},
		"zero creation time": {
			mutate:  func(value *Manifest) { value.CreatedAt = time.Time{} },
			message: "created_at",
		},
		"non-UTC creation time": {
			mutate: func(value *Manifest) {
				value.CreatedAt = time.Date(2026, time.July, 28, 12, 0, 0, 0, time.FixedZone("east", 3600))
			},
			message: "UTC",
		},
		"no output": {
			mutate:  func(value *Manifest) { value.Outputs = OutputConfig{} },
			message: "output",
		},
		"JSONL schema": {
			mutate:  func(value *Manifest) { value.Outputs.JSONL.SchemaVersion = "wrong" },
			message: "JSONL schema",
		},
		"JSONL codec": {
			mutate:  func(value *Manifest) { value.Outputs.JSONL.Codec = "zip" },
			message: "JSONL",
		},
		"JSONL level": {
			mutate:  func(value *Manifest) { value.Outputs.JSONL.Level = 99 },
			message: "level",
		},
		"Parquet schema": {
			mutate:  func(value *Manifest) { value.Outputs.Parquet.SchemaVersion = "wrong" },
			message: "Parquet schema",
		},
		"enabled scrub rules fingerprint": {
			mutate:  func(value *Manifest) { value.Scrub.RulesFingerprint = "" },
			message: "rules fingerprint",
		},
		"enabled scrub salt fingerprint": {
			mutate:  func(value *Manifest) { value.Scrub.SaltFingerprint = strings.Repeat("A", 64) },
			message: "salt fingerprint",
		},
		"disabled scrub fingerprints": {
			mutate: func(value *Manifest) {
				value.Scrub.Enabled = false
			},
			message: "disabled",
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := fixtureManifest()
			testCase.mutate(&manifest)

			require.ErrorContains(t, manifest.Validate(), testCase.message)
		})
	}
}

func TestManifestRejectsInvalidGraphMetadata(t *testing.T) {
	tests := map[string]struct {
		mutate  func(*Manifest)
		message string
	}{
		"empty graph name": {
			mutate:  func(value *Manifest) { value.Graphs[0].Name = "" },
			message: "graph name",
		},
		"unsafe graph traversal": {
			mutate:  func(value *Manifest) { value.Graphs[0].Name = "../escape" },
			message: "graph name",
		},
		"unsafe graph slash": {
			mutate:  func(value *Manifest) { value.Graphs[0].Name = "a/b" },
			message: "graph name",
		},
		"unsafe graph backslash": {
			mutate:  func(value *Manifest) { value.Graphs[0].Name = `a\b` },
			message: "graph name",
		},
		"unsafe graph dot": {
			mutate:  func(value *Manifest) { value.Graphs[0].Name = "." },
			message: "graph name",
		},
		"duplicate graph": {
			mutate: func(value *Manifest) {
				value.Graphs = append(value.Graphs, Graph{Name: value.Graphs[0].Name})
			},
			message: "duplicate graph",
		},
		"empty catalog entry": {
			mutate:  func(value *Manifest) { value.Graphs[0].KindCatalog[1] = "" },
			message: "kind catalog",
		},
		"duplicate catalog entry": {
			mutate:  func(value *Manifest) { value.Graphs[0].KindCatalog[1] = value.Graphs[0].KindCatalog[0] },
			message: "kind catalog",
		},
		"node total": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeCount++ },
			message: "node shard total",
		},
		"relationship total": {
			mutate:  func(value *Manifest) { value.Graphs[0].RelationshipCount++ },
			message: "relationship shard total",
		},
		"empty graph shards": {
			mutate: func(value *Manifest) {
				value.Graphs[0].NodeCount = 0
				value.Graphs[0].RelationshipCount = 0
			},
			message: "empty graph",
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := fixtureManifest()
			testCase.mutate(&manifest)

			require.ErrorContains(t, manifest.Validate(), testCase.message)
		})
	}
}

func TestManifestRejectsInvalidLogicalShards(t *testing.T) {
	tests := map[string]struct {
		mutate  func(*Manifest)
		message string
	}{
		"outputless node shard": {
			mutate: func(value *Manifest) {
				value.Graphs[0].NodeShards[0].JSONL = nil
				value.Graphs[0].NodeShards[0].Parquet = nil
			},
			message: "output mismatch",
		},
		"missing global output": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL = nil },
			message: "output mismatch",
		},
		"noncontiguous index": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].Index = 2 },
			message: "index",
		},
		"zero count": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].Count = 0 },
			message: "count",
		},
		"zero cursor": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].LastSourceID = 0 },
			message: "last source ID",
		},
		"nonincreasing cursor": {
			mutate: func(value *Manifest) {
				first := value.Graphs[0].NodeShards[0]
				first.Index = 2
				first.LastSourceID = value.Graphs[0].NodeShards[0].LastSourceID
				value.Graphs[0].NodeShards = append(value.Graphs[0].NodeShards, first)
				value.Graphs[0].NodeCount += first.Count
				value.Graphs[0].Metrics.NodeCount += first.Count
			},
			message: "last source ID",
		},
		"negative preserve scrub count": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].ScrubCounts.Preserve = -1 },
			message: "scrub count",
		},
		"negative pseudonymize scrub count": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].ScrubCounts.Pseudonymize = -1 },
			message: "scrub count",
		},
		"negative redact scrub count": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].ScrubCounts.Redact = -1 },
			message: "scrub count",
		},
		"negative shift timestamp scrub count": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].ScrubCounts.ShiftTimestamp = -1 },
			message: "scrub count",
		},
		"disabled scrub count": {
			mutate: func(value *Manifest) {
				value.Scrub = ScrubMetadata{}
			},
			message: "scrubbing is disabled",
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := fixtureManifest()
			testCase.mutate(&manifest)

			require.ErrorContains(t, manifest.Validate(), testCase.message)
		})
	}
}

func TestManifestRejectsInvalidConcreteArtifactMetadata(t *testing.T) {
	tests := map[string]struct {
		mutate  func(*Manifest)
		message string
	}{
		"JSONL schema": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.SchemaVersion = "wrong" },
			message: "JSONL schema",
		},
		"JSONL codec": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.Codec = jsonl.CodecGzip },
			message: "JSONL codec",
		},
		"JSONL level": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.Level = 1 },
			message: "JSONL level",
		},
		"JSONL count": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.Count++ },
			message: "JSONL count",
		},
		"JSONL checksum": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.SHA256 = "abc" },
			message: "JSONL SHA-256",
		},
		"JSONL bytes": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.StoredBytes = 0 },
			message: "JSONL stored bytes",
		},
		"Parquet schema": {
			mutate:  func(value *Manifest) { value.Graphs[0].RelationshipShards[0].Parquet.SchemaVersion = "wrong" },
			message: "Parquet schema",
		},
		"Parquet count": {
			mutate:  func(value *Manifest) { value.Graphs[0].RelationshipShards[0].Parquet.Count++ },
			message: "Parquet count",
		},
		"Parquet checksum": {
			mutate:  func(value *Manifest) { value.Graphs[0].RelationshipShards[0].Parquet.SHA256 = "" },
			message: "Parquet SHA-256",
		},
		"Parquet bytes": {
			mutate:  func(value *Manifest) { value.Graphs[0].RelationshipShards[0].Parquet.StoredBytes = -1 },
			message: "Parquet stored bytes",
		},
		"unsafe path": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.Path = "../nodes.jsonl" },
			message: "path",
		},
		"wrong deterministic path": {
			mutate:  func(value *Manifest) { value.Graphs[0].NodeShards[0].JSONL.Path = "nodes.jsonl" },
			message: "path",
		},
		"duplicate path": {
			mutate: func(value *Manifest) {
				value.Graphs[0].RelationshipShards[0].Parquet.Path = value.Graphs[0].NodeShards[0].Parquet.Path
			},
			message: "path",
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := fixtureManifest()
			testCase.mutate(&manifest)

			require.ErrorContains(t, manifest.Validate(), testCase.message)
		})
	}
}

func TestManifestRejectsStructurallyInvalidMetrics(t *testing.T) {
	tests := map[string]struct {
		mutate  func(*metrics.GraphMetrics)
		message string
	}{
		"top-level count": {
			mutate:  func(value *metrics.GraphMetrics) { value.NodeCount++ },
			message: "metrics node count",
		},
		"negative histogram count": {
			mutate:  func(value *metrics.GraphMetrics) { value.NodeKindSequences["4:User"] = -1 },
			message: "node kind sequences",
		},
		"malformed node kind key": {
			mutate:  func(value *metrics.GraphMetrics) { value.NodeKindSequences = map[string]int64{"User": 2} },
			message: "node kind sequences",
		},
		"empty relationship kind": {
			mutate:  func(value *metrics.GraphMetrics) { value.RelationshipKinds = map[string]int64{"": 1} },
			message: "relationship kinds",
		},
		"degree key": {
			mutate:  func(value *metrics.GraphMetrics) { value.InboundDegreeHistogram = map[string]int64{"01": 2} },
			message: "inbound degree histogram",
		},
		"degree count": {
			mutate:  func(value *metrics.GraphMetrics) { value.OutboundDegreeHistogram = map[string]int64{"0": 2} },
			message: "outbound degree histogram",
		},
		"degree total": {
			mutate:  func(value *metrics.GraphMetrics) { value.InboundDegreeHistogram = map[string]int64{"1": 2} },
			message: "inbound degree total",
		},
		"endpoint shape key": {
			mutate:  func(value *metrics.GraphMetrics) { value.EndpointShapeHistogram = map[string]int64{"broken": 1} },
			message: "endpoint shape histogram",
		},
		"fingerprint": {
			mutate:  func(value *metrics.GraphMetrics) { value.Fingerprint = "sha256:ABC" },
			message: "fingerprint",
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := fixtureManifest()
			testCase.mutate(&manifest.Graphs[0].Metrics)

			require.ErrorContains(t, manifest.Validate(), testCase.message)
		})
	}
}

func fixtureManifest() Manifest {
	builder := metrics.NewBuilder()
	mustObserveNode(builder, entity.Node{SourceID: "1", Kinds: []string{"User"}})
	mustObserveNode(builder, entity.Node{SourceID: "2", Kinds: []string{"Group"}})
	mustObserveRelationship(builder, entity.Relationship{
		SourceID: "10",
		StartID:  "1",
		EndID:    "2",
		Kind:     "MEMBER_OF",
	})

	return Manifest{
		Format:    Format,
		CreatedAt: time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC),
		Outputs: OutputConfig{
			JSONL: &JSONLOutput{
				SchemaVersion: jsonl.SchemaVersion,
				Codec:         string(jsonl.CodecZstd),
				Level:         3,
			},
			Parquet: &ParquetOutput{SchemaVersion: parquet.SchemaVersion},
		},
		Scrub: ScrubMetadata{
			Enabled:          true,
			RulesFingerprint: strings.Repeat("a", 64),
			SaltFingerprint:  strings.Repeat("b", 64),
		},
		Graphs: []Graph{{
			Name:              "bloodhound",
			NodeCount:         2,
			RelationshipCount: 1,
			KindCatalog:       []string{"User", "Group", "MEMBER_OF"},
			NodeShards: []NodeShard{{
				Index:        1,
				Count:        2,
				LastSourceID: 10,
				ScrubCounts:  scrub.ActionCounts{Pseudonymize: 2},
				JSONL: &JSONLArtifact{
					Path: NodeJSONLPath("bloodhound", 1, jsonl.CodecZstd),
					Artifact: jsonl.Artifact{
						SchemaVersion:     jsonl.SchemaVersion,
						Codec:             jsonl.CodecZstd,
						SHA256:            strings.Repeat("c", 64),
						Level:             3,
						Count:             2,
						UncompressedBytes: 128,
						StoredBytes:       80,
					},
				},
				Parquet: &ParquetArtifact{
					Path: NodeParquetPath("bloodhound", 1),
					Artifact: parquet.Artifact{
						SchemaVersion: parquet.SchemaVersion,
						SHA256:        strings.Repeat("d", 64),
						Count:         2,
						StoredBytes:   256,
					},
				},
			}},
			RelationshipShards: []RelationshipShard{{
				Index:        1,
				Count:        1,
				LastSourceID: 20,
				ScrubCounts:  scrub.ActionCounts{Preserve: 1},
				JSONL: &JSONLArtifact{
					Path: RelationshipJSONLPath("bloodhound", 1, jsonl.CodecZstd),
					Artifact: jsonl.Artifact{
						SchemaVersion:     jsonl.SchemaVersion,
						Codec:             jsonl.CodecZstd,
						SHA256:            strings.Repeat("e", 64),
						Level:             3,
						Count:             1,
						UncompressedBytes: 96,
						StoredBytes:       64,
					},
				},
				Parquet: &ParquetArtifact{
					Path: RelationshipParquetPath("bloodhound", 1),
					Artifact: parquet.Artifact{
						SchemaVersion: parquet.SchemaVersion,
						SHA256:        strings.Repeat("f", 64),
						Count:         1,
						StoredBytes:   192,
					},
				},
			}},
			Metrics: builder.Finalize(),
		}},
	}
}

func mustObserveNode(builder *metrics.Builder, node entity.Node) {
	if err := builder.ObserveNode(node); err != nil {
		panic(err)
	}
}

func mustObserveRelationship(builder *metrics.Builder, relationship entity.Relationship) {
	if err := builder.ObserveRelationship(relationship); err != nil {
		panic(err)
	}
}
