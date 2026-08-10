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
	}
	redactExistingGraphRecord(&record, manifest, map[string]graph.ID{"source": 42})
	require.Empty(t, record.Cypher)
	require.Empty(t, record.Params)
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, record.NodeParams["source"])
	require.NotContains(t, record.NodeParams["source"], "safe-source")
	require.NotContains(t, record.ObservedRows[0], "sensitive-property")
	require.NotContains(t, record.PostgresPlan[0], "42")
	require.NotContains(t, record.Error, "77")
	require.Contains(t, record.Error, "unmapped-node:<redacted-id>")
}

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

func TestExistingGraphCheckpointIsIdentityBoundAndResumable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	records := []CaseResult{{
		Dataset:       "live",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
	}}
	require.NoError(t, writeExistingGraphCheckpoint(path, "manifest", "corpus", records))
	loaded, err := readExistingGraphCheckpoint(path, "manifest", "corpus")
	require.NoError(t, err)
	require.Equal(t, records, loaded)
	_, err = readExistingGraphCheckpoint(path, "other", "corpus")
	require.ErrorContains(t, err, "identity")

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var checkpoint existingGraphCheckpoint
	require.NoError(t, json.Unmarshal(raw, &checkpoint))
	require.Equal(t, existingGraphCheckpointVersion, checkpoint.Version)
}

func TestExistingGraphPlanRedactionPreservesJSONNumbers(t *testing.T) {
	raw := json.RawMessage(`[{"Plan":{"Plan Rows":42,"Index Cond":"id = 42"}}]`)
	redacted := redactPlanJSON(raw, map[string]graph.ID{"source": 42})
	require.JSONEq(t, `[{"Plan":{"Plan Rows":42,"Index Cond":"id = <anchor-id>"}}]`, string(redacted))
}

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

func TestCompleteGateRejectsAdaptiveExistingGraphArtifacts(t *testing.T) {
	records := []CaseResult{{
		ExistingGraph: &ExistingGraphRun{
			Adaptive: true,
		},
	}}
	require.ErrorContains(t, validatePerformanceArtifactSelections(records, records, false), "adaptive-discovery")
}

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
}

func splitNonEmptyLines(value string) []string {
	var lines []string
	for _, line := range regexp.MustCompile(`\r?\n`).Split(value, -1) {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
