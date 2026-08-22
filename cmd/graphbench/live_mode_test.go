// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

// TestExistingGraphManifestCorpusSafetyAndRedaction verifies that live-graph mode rejects mutations and strips query, parameter, plan, row, reference, and error disclosures from artifacts.
func TestExistingGraphManifestCorpusSafetyAndRedaction(t *testing.T) {
	manifest := ExistingGraphAnchorManifest{
		Version:  1,
		Checksum: "manifest",
		Anchors: map[string]ExistingGraphAnchor{
			"source": {
				LogicalKey: "safe-source",
			}, "target": {
				LogicalKey: "safe-target",
			},
		},
	}
	readCase := ScaleCase{
		Name:           "read",
		Dataset:        "live",
		Category:       "live",
		Cypher:         `MATCH (n) WHERE n.note = 'create is text' AND id(n) = $source RETURN n`,
		NodeParams:     map[string]string{"source": "source"},
		CandidateModes: []ExecutionMode{ModePostgresSQL},
	}
	require.NoError(t, validateExistingGraphCorpus(ScaleCorpus{
		Cases: []ScaleCase{readCase},
	}, manifest))

	writeCase := readCase
	writeCase.Name = "write"
	writeCase.Cypher = "MATCH (n) DELETE n"
	require.ErrorContains(t, validateExistingGraphCorpus(ScaleCorpus{
		Cases: []ScaleCase{writeCase},
	}, manifest), "mutation keyword")
	writeCase.Cypher = "MATCH (n) RETURN n"
	writeCase.WriteScenario = &WriteScenario{}
	require.ErrorContains(t, validateExistingGraphCorpus(ScaleCorpus{
		Cases: []ScaleCase{writeCase},
	}, manifest), "write_scenario")

	record := CaseResult{
		Cypher:       readCase.Cypher,
		Params:       map[string]any{"source": 42},
		NodeParams:   map[string]string{"source": "source"},
		ObservedRows: []string{"sensitive-property"},
		PostgresPlan: []string{"Index Cond: id = 42"},
		Error:        "unmapped-node:77",
		PostgresReferences: []PostgresReferenceResult{{
			ObservedRows: []string{"reference-sensitive-property"},
		}},
		ExistingGraph: &ExistingGraphRun{Attempts: []ExistingGraphAttempt{{
			Error: "attempt-sensitive-property 42",
		}}},
	}
	redactExistingGraphRecord(&record, manifest, map[string]graph.ID{"source": 42})
	require.Empty(t, record.Cypher)
	require.Empty(t, record.Params)
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, record.NodeParams["source"])
	require.NotContains(t, record.NodeParams["source"], "safe-source")
	require.NotContains(t, record.ObservedRows[0], "sensitive-property")
	require.NotContains(t, record.PostgresPlan[0], "42")
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, record.Error)
	require.NotContains(t, record.Error, "77")
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, record.PostgresReferences[0].ObservedRows[0])
	require.NotContains(t, record.PostgresReferences[0].ObservedRows[0], "reference-sensitive-property")
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, record.ExistingGraph.Attempts[0].Error)
	require.NotContains(t, record.ExistingGraph.Attempts[0].Error, "attempt-sensitive-property")
}

// TestExistingGraphManifestRequiresGraphAndLogicalContentIdentity verifies manifest checksums bind graph/content identity and that each anchor chooses exactly one complete logical or physical identity form.
func TestExistingGraphManifestRequiresGraphAndLogicalContentIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "anchors.json")
	valid := `{"version":1,"graph":"integration_test","content_identity":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","anchors":{"source":{"logical_key":"safe-source"}}}`
	require.NoError(t, os.WriteFile(path, []byte(valid), 0o600))
	manifest, err := loadExistingGraphAnchorManifest(path)
	require.NoError(t, err)
	require.Equal(t, "integration_test", manifest.Graph)
	require.Regexp(t, `^[0-9a-f]{64}$`, manifest.Checksum)

	physical := `{"version":1,"graph":"integration_test","content_identity":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","anchors":{"source":{"physical_id":42,"content_sha256":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}}}`
	require.NoError(t, os.WriteFile(path, []byte(physical), 0o600))
	manifest, err = loadExistingGraphAnchorManifest(path)
	require.NoError(t, err)
	require.Equal(t, int64(42), *manifest.Anchors["source"].PhysicalID)

	require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"graph":"integration_test","content_identity":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","anchors":{"source":{"physical_id":42}}}`), 0o600))
	_, err = loadExistingGraphAnchorManifest(path)
	require.ErrorContains(t, err, "content_sha256")

	require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"graph":"integration_test","content_identity":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","anchors":{"source":{"logical_key":"safe-source","physical_id":42,"content_sha256":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}}}`), 0o600))
	_, err = loadExistingGraphAnchorManifest(path)
	require.ErrorContains(t, err, "exactly one")

	require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"graph":"integration_test","anchors":{"source":{"logical_key":"safe-source"}}}`), 0o600))
	_, err = loadExistingGraphAnchorManifest(path)
	require.ErrorContains(t, err, "content_identity")
}

// TestPhysicalExistingGraphAnchorRedactionUsesContentIdentity verifies that artifacts replace a physical anchor ID with an opaque digest derived from its content identity.
func TestPhysicalExistingGraphAnchorRedactionUsesContentIdentity(t *testing.T) {
	id := int64(42)
	manifest := ExistingGraphAnchorManifest{
		Anchors: map[string]ExistingGraphAnchor{
			"source": {
				PhysicalID:    &id,
				ContentSHA256: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			},
		},
	}
	record := CaseResult{
		NodeParams: map[string]string{"source": "source"},
	}
	redactExistingGraphRecord(&record, manifest, map[string]graph.ID{"source": graph.ID(id)})
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, record.NodeParams["source"])
	require.NotContains(t, record.NodeParams["source"], "42")
}

// TestExistingGraphCheckpointIsIdentityBoundAndResumable verifies round-trip recovery only for matching manifest, corpus, and run identities and rejects duplicate completed records.
func TestExistingGraphCheckpointIsIdentityBoundAndResumable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	records := []CaseResult{{
		Dataset:        "live",
		Name:           "case",
		WorkloadSHA256: "workload",
		ExecutionMode:  ModePostgresSQL,
		Status:         StatusOK,
		Environment: &RunEnvironment{
			ArtifactSchemaVersion: 2,
			CorpusSHA256:          "corpus",
			RunIdentitySHA256:     "run",
			RunUUID:               "run-uuid",
		},
	}}
	require.NoError(t, writeExistingGraphCheckpoint(path, "manifest", "corpus", "run", records))
	loaded, err := readExistingGraphCheckpoint(path, "manifest", "corpus", "run")
	require.NoError(t, err)
	require.Equal(t, records, loaded)
	_, err = readExistingGraphCheckpoint(path, "other", "corpus", "run")
	require.ErrorContains(t, err, "identity")
	_, err = readExistingGraphCheckpoint(path, "manifest", "corpus", "other-run")
	require.ErrorContains(t, err, "identity")

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var checkpoint existingGraphCheckpoint
	require.NoError(t, json.Unmarshal(raw, &checkpoint))
	require.Equal(t, existingGraphCheckpointVersion, checkpoint.Version)

	checkpoint.Records = append(checkpoint.Records, checkpoint.Records[0])
	raw, err = json.Marshal(checkpoint)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	_, err = readExistingGraphCheckpoint(path, "manifest", "corpus", "run")
	require.ErrorContains(t, err, "duplicate record")
}

// TestExistingGraphPlanRedactionPreservesJSONNumbers verifies that plan redaction replaces IDs inside text without corrupting numeric cardinality fields in the JSON document.
func TestExistingGraphPlanRedactionPreservesJSONNumbers(t *testing.T) {
	raw := json.RawMessage(`[{"Plan":{"Plan Rows":42,"Index Cond":"id = 42"}}]`)
	redacted := redactPlanJSON(raw, map[string]graph.ID{"source": 42})
	require.JSONEq(t, `[{"Plan":{"Plan Rows":42,"Index Cond":"id = <anchor-id>"}}]`, string(redacted))
}

// TestExistingGraphProgressIsAppendOnlyJSONL verifies that successive progress events remain two independently parseable JSON Lines records.
func TestExistingGraphProgressIsAppendOnlyJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	require.NoError(t, appendExistingGraphProgress(path, ExistingGraphProgress{
		Stage:   "case",
		CaseKey: "one",
	}))
	require.NoError(t, appendExistingGraphProgress(path, ExistingGraphProgress{
		Stage:   "plan",
		CaseKey: "one",
	}))
	require.NoError(t, scanCheckpointJSONL(path))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, 2, len(splitNonEmptyLines(string(raw))))
}

// TestCompleteGateRejectsAdaptiveExistingGraphArtifacts verifies that discovery-selected live-graph measurements cannot enter a complete performance gate.
func TestCompleteGateRejectsAdaptiveExistingGraphArtifacts(t *testing.T) {
	records := []CaseResult{{
		ExistingGraph: &ExistingGraphRun{
			Adaptive: true,
		},
	}}
	require.ErrorContains(t, validatePerformanceArtifactSelections(records, records, false), "adaptive-discovery")
}

// TestExistingGraphCorpusIdentityIsStable verifies that corpus identity is deterministic and changes when either query text or expected cardinality changes.
func TestExistingGraphCorpusIdentityIsStable(t *testing.T) {
	zero := int64(0)
	corpus := ScaleCorpus{
		Cases: []ScaleCase{{
			Name:     "case",
			Dataset:  "live",
			Category: "live",
			Cypher:   "RETURN 1",
			Expected: ExpectedResult{
				RowCount: &zero,
			},
			Params:         testutil.Params{},
			CandidateModes: []ExecutionMode{ModePostgresSQL},
		}},
	}
	require.Equal(t, corpusIdentity(corpus), corpusIdentity(corpus))
	changedQuery := corpus
	changedQuery.Cases = append([]ScaleCase(nil), corpus.Cases...)
	changedQuery.Cases[0].Cypher = "RETURN 2"
	require.NotEqual(t, corpusIdentity(corpus), corpusIdentity(changedQuery))

	changedExpected := corpus
	changedExpected.Cases = append([]ScaleCase(nil), corpus.Cases...)
	one := int64(1)
	changedExpected.Cases[0].Expected.RowCount = &one
	require.NotEqual(t, corpusIdentity(corpus), corpusIdentity(changedExpected))
}

// TestExistingGraphCompletedWorkloadsAreFixtureBound verifies that resume records are accepted only for known cases with the same fixture checksum and workload digest.
func TestExistingGraphCompletedWorkloadsAreFixtureBound(t *testing.T) {
	corpus := ScaleCorpus{Cases: []ScaleCase{{
		Name:           "case",
		Dataset:        "live",
		Cypher:         "RETURN 1",
		CandidateModes: []ExecutionMode{ModePostgresSQL},
	}}}
	fixture := FixtureMetadata{Dataset: "existing_graph", Checksum: "manifest:content:schema:index"}
	expected := newCaseResult(corpus.Cases[0], ModePostgresSQL, nil)
	attachFixtureMetadata(&expected, fixture)
	completed := map[string]string{existingGraphCaseKey(ModePostgresSQL, corpus.Cases[0]): expected.WorkloadSHA256}
	require.NoError(t, validateCompletedWorkloads(completed, corpus, fixture))

	changedFixture := fixture
	changedFixture.Checksum = "manifest:other-content:schema:index"
	require.ErrorContains(t, validateCompletedWorkloads(completed, corpus, changedFixture), "workload identity")
	require.ErrorContains(t, validateCompletedWorkloads(map[string]string{"postgres_sql/other/case": "digest"}, corpus, fixture), "unknown workload")
}

// splitNonEmptyLines separates platform-independent line endings and discards empty records.
func splitNonEmptyLines(value string) []string {
	var lines []string
	for _, line := range regexp.MustCompile(`\r?\n`).Split(value, -1) {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
