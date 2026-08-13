// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

func writePromotionManifestWithPassingEvidence(t *testing.T, manifest PromotionManifest) string {
	t.Helper()
	directory := t.TempDir()
	manifest.Evidence = map[string]PromotionEvidenceReference{}
	for _, role := range requiredPromotionEvidenceRoles {
		document := map[string]any{"passed": true, "promotion_identity": promotionEvidenceIdentity(manifest)}
		switch role {
		case "aa":
			document = map[string]any{"order_balanced": true, "cases": []any{map[string]any{"name": "case"}}, "promotion_identity": promotionEvidenceIdentity(manifest)}
		case "confirmation", "performance":
			document = map[string]any{"promotion_eligible": true, "promotion_identity": promotionEvidenceIdentity(manifest)}
		}
		raw, err := json.Marshal(document)
		require.NoError(t, err)
		path := role + ".json"
		require.NoError(t, os.WriteFile(filepath.Join(directory, path), raw, 0o600))
		digest := sha256.Sum256(raw)
		manifest.Evidence[role] = PromotionEvidenceReference{Path: path, SHA256: hex.EncodeToString(digest[:])}
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(directory, "promotion.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}

func TestVerifyPromotionManifestRequiresExactOrientationProbeContract(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ExpansionSearchPolicyOrientationProbeV1), SelectorVersion: "orientation-probe-v1",
		ExecutionBoundary: "guarded_dual_arm", FallbackExecutor: string(optimize.ExpansionSearchStepwiseForward),
		SourceCommit: "deadbeef", SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps: orientationPromotionCaps(),
		Buckets: []PromotionBucket{{
			Name: "fixed-suffix", QuerySHA256: []string{digest}, Direction: "outbound", ObservationMode: "endpoint_ids",
			MinimumDepth: 0, MaximumDepth: 16, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"},
		}},
	}

	verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, base))
	require.NoError(t, err)
	require.True(t, verification.Passed, verification.Reasons)

	tests := []struct {
		name   string
		mutate func(*PromotionManifest)
		reason string
	}{
		{
			name: "boundary", mutate: func(manifest *PromotionManifest) { manifest.ExecutionBoundary = "inline_statement" },
			reason: "orientation-probe-v1 requires the guarded_dual_arm production boundary",
		},
		{
			name: "fallback", mutate: func(manifest *PromotionManifest) { manifest.FallbackExecutor = "EXPANSION-SUFFIX-SEEDED-REVERSE" },
			reason: "orientation-probe-v1 requires EXPANSION-STEPWISE-FORWARD as its exact fallback",
		},
		{
			name: "extra cap", mutate: func(manifest *PromotionManifest) { manifest.Caps["extra_limit"] = 1 },
			reason: "orientation-probe-v1 requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps",
		},
		{
			name: "missing cap", mutate: func(manifest *PromotionManifest) { delete(manifest.Caps, "root_row_limit") },
			reason: "orientation-probe-v1 requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps",
		},
		{
			name: "root cap", mutate: func(manifest *PromotionManifest) { manifest.Caps["root_row_limit"]-- },
			reason: "orientation-probe-v1 cap root_row_limit must equal 512",
		},
		{
			name: "reverse seed cap", mutate: func(manifest *PromotionManifest) { manifest.Caps["reverse_seed_row_limit"]-- },
			reason: "orientation-probe-v1 cap reverse_seed_row_limit must equal 512",
		},
		{
			name: "directional degree cap", mutate: func(manifest *PromotionManifest) { manifest.Caps["directional_degree_row_limit"]-- },
			reason: "orientation-probe-v1 cap directional_degree_row_limit must equal 16384",
		},
		{
			name: "state cap", mutate: func(manifest *PromotionManifest) { manifest.Caps["state_limit"]-- },
			reason: "orientation-probe-v1 cap state_limit must equal 4096",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := base
			manifest.Caps = clonePromotionCaps(base.Caps)
			test.mutate(&manifest)
			verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, manifest))
			require.NoError(t, err)
			require.False(t, verification.Passed)
			require.Contains(t, verification.Reasons, test.reason)
		})
	}
}

func TestVerifyPromotionManifestRequiresStaticV6CanonicalInboundContract(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		SelectorVersion: optimize.ShortestPathSelectorStaticV6, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalWitness),
		SourceCommit:     "deadbeef", SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps: map[string]int64{"state_limit": 100_000, "predecessor_limit": 100_000, "enumeration_limit": 100_000, "output_bytes_limit": 64 << 20},
		Buckets: []PromotionBucket{{
			Name: "canonical-inbound-depth64", QuerySHA256: []string{digest}, Direction: "inbound", ObservationMode: "one_path",
			MinimumDepth: 1, MaximumDepth: 64, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"},
		}},
	}

	verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, base))
	require.NoError(t, err)
	require.True(t, verification.Passed, verification.Reasons)

	tests := []struct {
		name   string
		mutate func(*PromotionManifest)
		reason string
	}{
		{
			name: "selector", mutate: func(manifest *PromotionManifest) { manifest.SelectorVersion = "sp-static-v5-contained" },
			reason: "SP-I1 canonical witness requires selector sp-static-v6",
		},
		{
			name: "outbound", mutate: func(manifest *PromotionManifest) { manifest.Buckets[0].Direction = "outbound" },
			reason: "SP-I1 canonical witness bucket canonical-inbound-depth64 must be the qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
		{
			name: "maximum", mutate: func(manifest *PromotionManifest) { manifest.Buckets[0].MaximumDepth = 63 },
			reason: "SP-I1 canonical witness bucket canonical-inbound-depth64 must be the qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
		{
			name: "kinds", mutate: func(manifest *PromotionManifest) { manifest.Buckets[0].RelationshipKindCount = 2 },
			reason: "SP-I1 canonical witness bucket canonical-inbound-depth64 must be the qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := base
			manifest.Caps = clonePromotionCaps(base.Caps)
			manifest.Buckets = clonePromotionBuckets(base.Buckets)
			test.mutate(&manifest)
			verification, err := verifyPromotionManifest(writePromotionManifestWithPassingEvidence(t, manifest))
			require.NoError(t, err)
			require.False(t, verification.Passed)
			require.Contains(t, verification.Reasons, test.reason)
		})
	}
}

func TestVerifyPromotionManifestRequiresCompleteImmutableEvidenceClosure(t *testing.T) {
	directory := t.TempDir()
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "SP-B2-C-MIN-LEVEL-D", SelectorVersion: "sp-static-v5", ExecutionBoundary: "stored_helper",
		SourceCommit: "deadbeef", SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps: map[string]int64{"visited_nodes": 1000},
		Buckets: []PromotionBucket{{
			Name: "deep-inbound-distance", QuerySHA256: []string{digest}, Direction: "inbound",
			ObservationMode: "distance", MinimumDepth: 5, MaximumDepth: 16,
			QualificationSplit: []string{"training", "holdout"},
		}},
	}
	evidence := map[string]PromotionEvidenceReference{}
	for _, role := range requiredPromotionEvidenceRoles {
		document := map[string]any{"passed": true, "promotion_identity": promotionEvidenceIdentity(manifest)}
		switch role {
		case "aa":
			document = map[string]any{"order_balanced": true, "cases": []any{map[string]any{"name": "case"}}, "promotion_identity": promotionEvidenceIdentity(manifest)}
		case "confirmation", "performance":
			document = map[string]any{"promotion_eligible": true, "promotion_identity": promotionEvidenceIdentity(manifest)}
		}
		raw, err := json.Marshal(document)
		require.NoError(t, err)
		path := role + ".json"
		require.NoError(t, os.WriteFile(filepath.Join(directory, path), raw, 0o600))
		digest := sha256.Sum256(raw)
		evidence[role] = PromotionEvidenceReference{Path: path, SHA256: hex.EncodeToString(digest[:])}
	}
	manifest.Evidence = evidence
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := filepath.Join(directory, "promotion.json")
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o600))

	verification, err := verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.True(t, verification.Passed, verification.Reasons)
	require.NotEmpty(t, verification.ManifestSHA256)

	delete(manifest.Evidence, "operational")
	raw, err = json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o600))
	verification, err = verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "required evidence role operational is missing")
}

func TestVerifyPromotionEvidenceRejectsEveryCrossBindingMismatch(t *testing.T) {
	directory := t.TempDir()
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "candidate-a", SelectorVersion: "selector", ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: "incumbent", SourceCommit: "commit", SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps: map[string]int64{"cap": 1}, Buckets: []PromotionBucket{{
			Name: "bucket", QuerySHA256: []string{digest}, Direction: "outbound", ObservationMode: "one_path",
			MinimumDepth: 1, MaximumDepth: 4, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"},
		}},
	}
	tests := map[string]func(*PromotionEvidenceIdentity){
		"candidate":       func(identity *PromotionEvidenceIdentity) { identity.Candidate = "candidate-b" },
		"selector":        func(identity *PromotionEvidenceIdentity) { identity.SelectorVersion = "other-selector" },
		"boundary":        func(identity *PromotionEvidenceIdentity) { identity.ExecutionBoundary = "stored_helper" },
		"fallback":        func(identity *PromotionEvidenceIdentity) { identity.FallbackExecutor = "other-incumbent" },
		"source commit":   func(identity *PromotionEvidenceIdentity) { identity.SourceCommit = "other-commit" },
		"source digest":   func(identity *PromotionEvidenceIdentity) { identity.SourceSHA256 = strings.Repeat("1", 64) },
		"binary digest":   func(identity *PromotionEvidenceIdentity) { identity.BinarySHA256 = strings.Repeat("2", 64) },
		"corpus digest":   func(identity *PromotionEvidenceIdentity) { identity.CorpusSHA256 = strings.Repeat("3", 64) },
		"cap":             func(identity *PromotionEvidenceIdentity) { identity.Caps["cap"] = 2 },
		"bucket envelope": func(identity *PromotionEvidenceIdentity) { identity.Buckets[0].MaximumDepth = 8 },
		"query cohort": func(identity *PromotionEvidenceIdentity) {
			identity.Buckets[0].QuerySHA256[0] = strings.Repeat("4", 64)
		},
		"qualification split": func(identity *PromotionEvidenceIdentity) {
			identity.Buckets[0].QualificationSplit = []string{"training"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			wrong := promotionEvidenceIdentity(manifest)
			mutate(&wrong)
			document := map[string]any{"passed": true, "promotion_identity": wrong}
			raw, err := json.Marshal(document)
			require.NoError(t, err)
			path := "resource.json"
			require.NoError(t, os.WriteFile(filepath.Join(directory, path), raw, 0o600))
			sum := sha256.Sum256(raw)
			reference := PromotionEvidenceReference{Path: path, SHA256: hex.EncodeToString(sum[:])}
			err = verifyPromotionEvidence(directory, "resource", reference, promotionEvidenceIdentity(manifest))
			require.EqualError(t, err, "promotion identity does not match manifest")
		})
	}
}

func TestBindPromotionEvidenceReportCopiesCompleteManifestIdentity(t *testing.T) {
	directory := t.TempDir()
	digest := strings.Repeat("a", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "candidate", SelectorVersion: "selector", ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: "incumbent", SourceCommit: "commit", SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps: map[string]int64{"cap": 7}, Buckets: []PromotionBucket{{Name: "bucket", QuerySHA256: []string{digest}, QualificationSplit: []string{"training", "holdout"}}},
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := filepath.Join(directory, "manifest.json")
	inputPath := filepath.Join(directory, "input.json")
	outputPath := filepath.Join(directory, "output.json")
	require.NoError(t, os.WriteFile(manifestPath, manifestRaw, 0o600))
	require.NoError(t, os.WriteFile(inputPath, []byte(`{"passed":true}`), 0o600))
	require.NoError(t, bindPromotionEvidenceReport(manifestPath, "resource", inputPath, outputPath))

	boundRaw, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var bound struct {
		Passed            bool                      `json:"passed"`
		PromotionIdentity PromotionEvidenceIdentity `json:"promotion_identity"`
	}
	require.NoError(t, json.Unmarshal(boundRaw, &bound))
	require.True(t, bound.Passed)
	require.Equal(t, promotionEvidenceIdentity(manifest), bound.PromotionIdentity)
}

func TestVerifyPromotionManifestRejectsVersionOne(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "manifest.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":1}`), 0o600))
	verification, err := verifyPromotionManifest(path)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	require.Contains(t, verification.Reasons, "manifest version must be 2")
}

func TestVerifyPromotionManifestRejectsEscapingOrMutatedEvidence(t *testing.T) {
	directory := t.TempDir()
	manifestPath := filepath.Join(directory, "promotion.json")
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "candidate", SelectorVersion: "selector", ExecutionBoundary: "inline_statement", SourceCommit: "commit",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps:     map[string]int64{"cap": 1},
		Buckets:  []PromotionBucket{{Name: "bucket", QuerySHA256: []string{digest}, QualificationSplit: []string{"training", "holdout"}}},
		Evidence: map[string]PromotionEvidenceReference{},
	}
	for _, role := range requiredPromotionEvidenceRoles {
		manifest.Evidence[role] = PromotionEvidenceReference{Path: "../outside.json", SHA256: digest}
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o600))

	verification, err := verifyPromotionManifest(manifestPath)
	require.NoError(t, err)
	require.False(t, verification.Passed)
	for _, role := range requiredPromotionEvidenceRoles {
		require.Contains(t, verification.Reasons, role+": path escapes the manifest directory")
	}
}
