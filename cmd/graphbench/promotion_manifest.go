// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

// promotionManifestVersion reserves the stable protocol value used to recognize promotion manifest version across artifacts and executions.
const promotionManifestVersion = 2

// requiredPromotionEvidenceRoles contains the frozen required promotion evidence roles declaration consulted by package validation.
var requiredPromotionEvidenceRoles = []string{
	"aa", "confirmation", "performance", "resource", "reference_closure", "operational",
}

// orientationPromotionCaps returns the resource limits enforced for orientation promotion.
func orientationPromotionCaps() map[string]int64 {
	return map[string]int64{
		"root_row_limit":               optimize.ExpansionSearchOrientationRootRowLimit,
		"reverse_seed_row_limit":       optimize.ExpansionSearchOrientationReverseSeedRowLimit,
		"directional_degree_row_limit": optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit,
		"state_limit":                  optimize.ExpansionSearchOrientationStateLimit,
	}
}

// validateStaticV6CanonicalInboundBucket validates static v6 canonical inbound bucket.
func validateStaticV6CanonicalInboundBucket(bucket PromotionBucket) error {
	if bucket.Direction != "inbound" || bucket.ObservationMode != string(optimize.ShortestPathObservationOnePath) ||
		bucket.MinimumDepth != 1 || bucket.MaximumDepth != 64 || bucket.RelationshipKindCount != 1 || bucket.UntypedRelationship {
		return fmt.Errorf("SP-I1 canonical witness bucket %s must be the qualified inbound typed single-kind one-path depth 1..64 envelope", bucket.Name)
	}
	return nil
}

// PromotionEvidenceReference groups state that must remain consistent while processing promotion evidence reference.
type PromotionEvidenceReference struct {
	// Path identifies the filesystem path.
	Path string `json:"path"`
	// SHA256 binds the referenced  content by SHA-256 digest.
	SHA256 string `json:"sha256"`
}

// PromotionBucket groups state that must remain consistent while processing promotion bucket.
type PromotionBucket struct {
	// Name identifies the name.
	Name string `json:"name"`
	// QuerySHA256 binds the referenced query content by SHA-256 digest.
	QuerySHA256 []string `json:"query_sha256"`
	// Direction selects the traversal orientation covered by the contract.
	Direction string `json:"direction,omitempty"`
	// ObservationMode identifies the observation mode.
	ObservationMode string `json:"observation_mode,omitempty"`
	// MinimumDepth sets the inclusive lower traversal-depth bound.
	MinimumDepth int `json:"minimum_depth,omitempty"`
	// MaximumDepth sets the inclusive upper traversal-depth bound.
	MaximumDepth int `json:"maximum_depth,omitempty"`
	// RelationshipKindCount records the number of relationship kind count.
	RelationshipKindCount int `json:"relationship_kind_count,omitempty"`
	// UntypedRelationship indicates whether untyped relationship applies.
	UntypedRelationship bool `json:"untyped_relationship,omitempty"`
	// QualificationSplit assigns the workload to training, holdout, or diagnostic evidence.
	QualificationSplit []string `json:"qualification_split"`
}

// PromotionManifest is the sole authorization record consumed by a rollout.
// It binds one immutable candidate and selector to source, binary, corpus,
// caps, exact query cohorts, and every required passing report.
type PromotionManifest struct {
	// Version identifies the schema version for version.
	Version int `json:"version"`
	// Candidate identifies the execution strategy being evaluated or authorized.
	Candidate string `json:"candidate"`
	// SelectorVersion identifies the schema version for selector version.
	SelectorVersion string `json:"selector_version"`
	// ExecutionBoundary supplies the execution boundary input to the PromotionManifest contract.
	ExecutionBoundary string `json:"execution_boundary"`
	// FallbackExecutor supplies the fallback executor input to the PromotionManifest contract.
	FallbackExecutor string `json:"fallback_executor,omitempty"`
	// SourceCommit supplies the source commit input to the PromotionManifest contract.
	SourceCommit string `json:"source_commit"`
	// SourceSHA256 binds the referenced source content by SHA-256 digest.
	SourceSHA256 string `json:"source_sha256"`
	// BinarySHA256 binds the referenced binary content by SHA-256 digest.
	BinarySHA256 string `json:"binary_sha256"`
	// CorpusSHA256 binds the referenced corpus content by SHA-256 digest.
	CorpusSHA256 string `json:"corpus_sha256"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps map[string]int64 `json:"caps"`
	// Buckets supplies the buckets input to the PromotionManifest contract.
	Buckets []PromotionBucket `json:"buckets"`
	// Evidence supplies the evidence input to the PromotionManifest contract.
	Evidence map[string]PromotionEvidenceReference `json:"evidence"`
}

// PromotionEvidenceIdentity is repeated verbatim by every evidence report.
// It deliberately excludes evidence paths and digests, avoiding a circular
// dependency while binding the report to every authorization-relevant field.
type PromotionEvidenceIdentity struct {
	// Candidate identifies the execution strategy being evaluated or authorized.
	Candidate string `json:"candidate"`
	// SelectorVersion identifies the schema version for selector version.
	SelectorVersion string `json:"selector_version"`
	// ExecutionBoundary supplies the execution boundary input to the PromotionEvidenceIdentity contract.
	ExecutionBoundary string `json:"execution_boundary"`
	// FallbackExecutor supplies the fallback executor input to the PromotionEvidenceIdentity contract.
	FallbackExecutor string `json:"fallback_executor,omitempty"`
	// SourceCommit supplies the source commit input to the PromotionEvidenceIdentity contract.
	SourceCommit string `json:"source_commit"`
	// SourceSHA256 binds the referenced source content by SHA-256 digest.
	SourceSHA256 string `json:"source_sha256"`
	// BinarySHA256 binds the referenced binary content by SHA-256 digest.
	BinarySHA256 string `json:"binary_sha256"`
	// CorpusSHA256 binds the referenced corpus content by SHA-256 digest.
	CorpusSHA256 string `json:"corpus_sha256"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps map[string]int64 `json:"caps"`
	// Buckets supplies the buckets input to the PromotionEvidenceIdentity contract.
	Buckets []PromotionBucket `json:"buckets"`
}

// promotionEvidenceIdentity derives the stable identity used to compare promotion evidence.
func promotionEvidenceIdentity(manifest PromotionManifest) PromotionEvidenceIdentity {
	return PromotionEvidenceIdentity{
		Candidate:         manifest.Candidate,
		SelectorVersion:   manifest.SelectorVersion,
		ExecutionBoundary: manifest.ExecutionBoundary,
		FallbackExecutor:  manifest.FallbackExecutor,
		SourceCommit:      manifest.SourceCommit,
		SourceSHA256:      manifest.SourceSHA256,
		BinarySHA256:      manifest.BinarySHA256,
		CorpusSHA256:      manifest.CorpusSHA256,
		Caps:              clonePromotionCaps(manifest.Caps),
		Buckets:           clonePromotionBuckets(manifest.Buckets),
	}
}

// clonePromotionCaps returns an independent copy of promotion caps.
func clonePromotionCaps(input map[string]int64) map[string]int64 {
	result := make(map[string]int64, len(input))
	for name, value := range input {
		result[name] = value
	}
	return result
}

// clonePromotionBuckets returns an independent copy of promotion buckets.
func clonePromotionBuckets(input []PromotionBucket) []PromotionBucket {
	result := append([]PromotionBucket(nil), input...)
	for idx := range result {
		result[idx].QuerySHA256 = append([]string(nil), result[idx].QuerySHA256...)
		result[idx].QualificationSplit = append([]string(nil), result[idx].QualificationSplit...)
	}
	return result
}

// PromotionManifestVerification groups state that must remain consistent while processing promotion manifest verification.
type PromotionManifestVerification struct {
	// Version identifies the schema version for version.
	Version int `json:"version"`
	// ManifestSHA256 binds the referenced manifest content by SHA-256 digest.
	ManifestSHA256 string `json:"manifest_sha256"`
	// Candidate identifies the execution strategy being evaluated or authorized.
	Candidate string `json:"candidate,omitempty"`
	// SelectorVersion identifies the schema version for selector version.
	SelectorVersion string `json:"selector_version,omitempty"`
	// Passed indicates whether passed applies.
	Passed bool `json:"passed"`
	// Reasons explains each failed or inapplicable validation gate.
	Reasons []string `json:"reasons,omitempty"`
}

// verifyPromotionManifest verifies promotion manifest.
func verifyPromotionManifest(path string) (PromotionManifestVerification, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return PromotionManifestVerification{}, err
	}
	digest := sha256.Sum256(raw)
	verification := PromotionManifestVerification{
		Version:        promotionManifestVersion,
		ManifestSHA256: hex.EncodeToString(digest[:]),
		Passed:         true,
	}
	var manifest PromotionManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return PromotionManifestVerification{}, fmt.Errorf("decode promotion manifest: %w", err)
	}
	verification.Candidate = manifest.Candidate
	verification.SelectorVersion = manifest.SelectorVersion
	addReason := func(reason string) {
		verification.Passed = false
		verification.Reasons = append(verification.Reasons, reason)
	}
	if manifest.Version != promotionManifestVersion {
		addReason("manifest version must be 2")
	}
	if strings.TrimSpace(manifest.Candidate) == "" || strings.TrimSpace(manifest.SelectorVersion) == "" {
		addReason("candidate and selector_version are required")
	}
	if manifest.ExecutionBoundary != "inline_statement" && manifest.ExecutionBoundary != "stored_helper" && manifest.ExecutionBoundary != "guarded_dual_arm" {
		addReason("execution_boundary must identify the measured production boundary")
	}
	for name, value := range map[string]string{"source_sha256": manifest.SourceSHA256, "binary_sha256": manifest.BinarySHA256, "corpus_sha256": manifest.CorpusSHA256} {
		if !isLowerHexSHA256(value) {
			addReason(name + " must be a lowercase SHA-256 digest")
		}
	}
	if strings.TrimSpace(manifest.SourceCommit) == "" {
		addReason("source_commit is required")
	}
	if len(manifest.Caps) == 0 {
		addReason("at least one immutable candidate cap is required")
	}
	for name, limit := range manifest.Caps {
		if strings.TrimSpace(name) == "" || limit <= 0 {
			addReason("candidate caps must have nonempty names and positive limits")
		}
	}
	if manifest.Candidate == "ASP-I1-U-DAG+MAT-M0" {
		expectedCaps := map[string]struct{}{
			"state_limit": {}, "predecessor_limit": {}, "enumeration_limit": {}, "output_bytes_limit": {},
		}
		if manifest.ExecutionBoundary != "guarded_dual_arm" {
			addReason("ASP-I1 requires the guarded_dual_arm production boundary")
		}
		if manifest.FallbackExecutor != "ASP-A1-DAG" {
			addReason("ASP-I1 requires ASP-A1-DAG as its exact fallback")
		}
		if len(manifest.Caps) != len(expectedCaps) {
			addReason("ASP-I1 requires exactly state, predecessor, enumeration, and output-byte caps")
		}
		for name := range expectedCaps {
			if manifest.Caps[name] <= 0 {
				addReason("ASP-I1 cap " + name + " must be positive")
			}
		}
	}
	if manifest.Candidate == "SP-I1-C-WE+MAT-M0" {
		expectedCaps := map[string]struct{}{
			"state_limit": {}, "predecessor_limit": {}, "enumeration_limit": {}, "output_bytes_limit": {},
		}
		if manifest.ExecutionBoundary != "guarded_dual_arm" {
			addReason("SP-I1 canonical witness requires the guarded_dual_arm production boundary")
		}
		if manifest.FallbackExecutor != "SP-S4-C-WE+MAT-M0" {
			addReason("SP-I1 canonical witness requires SP-S4-C-WE+MAT-M0 as its exact fallback")
		}
		if len(manifest.Caps) != len(expectedCaps) {
			addReason("SP-I1 canonical witness requires exactly state, predecessor, enumeration, and output-byte caps")
		}
		for name := range expectedCaps {
			if manifest.Caps[name] <= 0 {
				addReason("SP-I1 canonical witness cap " + name + " must be positive")
			}
		}
		if manifest.SelectorVersion != optimize.ShortestPathSelectorStaticV6 {
			addReason("SP-I1 canonical witness requires selector " + optimize.ShortestPathSelectorStaticV6)
		}
	}
	if manifest.Candidate == string(optimize.ExpansionSearchPolicyOrientationProbeV1) {
		expectedCaps := orientationPromotionCaps()
		if manifest.ExecutionBoundary != "guarded_dual_arm" {
			addReason("orientation-probe-v1 requires the guarded_dual_arm production boundary")
		}
		if manifest.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) {
			addReason("orientation-probe-v1 requires EXPANSION-STEPWISE-FORWARD as its exact fallback")
		}
		if len(manifest.Caps) != len(expectedCaps) {
			addReason("orientation-probe-v1 requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps")
		}
		for name, expected := range expectedCaps {
			if manifest.Caps[name] != expected {
				addReason(fmt.Sprintf("orientation-probe-v1 cap %s must equal %d", name, expected))
			}
		}
	}
	if len(manifest.Buckets) == 0 {
		addReason("at least one authorized bucket is required")
	}
	seenBuckets := map[string]struct{}{}
	for _, bucket := range manifest.Buckets {
		if bucket.Name == "" || len(bucket.QuerySHA256) == 0 {
			addReason("every bucket requires a name and query allowlist")
			continue
		}
		if _, found := seenBuckets[bucket.Name]; found {
			addReason("bucket " + bucket.Name + " is duplicated")
		}
		seenBuckets[bucket.Name] = struct{}{}
		for _, query := range bucket.QuerySHA256 {
			if !isLowerHexSHA256(query) {
				addReason("bucket " + bucket.Name + " contains an invalid query digest")
			}
		}
		if !containsString(bucket.QualificationSplit, "training") || !containsString(bucket.QualificationSplit, "holdout") {
			addReason("bucket " + bucket.Name + " must bind training and holdout evidence")
		}
		if manifest.Candidate == "ASP-I1-U-DAG+MAT-M0" {
			if (bucket.Direction != "outbound" && bucket.Direction != "inbound") || bucket.ObservationMode != "all_paths" || bucket.MinimumDepth != 1 || bucket.MaximumDepth < 1 || bucket.MaximumDepth > 64 {
				addReason("ASP-I1 bucket " + bucket.Name + " is outside the directed all-paths depth envelope")
			}
			if bucket.RelationshipKindCount < 0 || bucket.UntypedRelationship != (bucket.RelationshipKindCount == 0) {
				addReason("ASP-I1 bucket " + bucket.Name + " has inconsistent relationship-kind metadata")
			}
		}
		if manifest.Candidate == "SP-I1-C-WE+MAT-M0" {
			if err := validateStaticV6CanonicalInboundBucket(bucket); err != nil {
				addReason(err.Error())
			}
		}
	}
	base := filepath.Dir(path)
	for _, role := range requiredPromotionEvidenceRoles {
		reference, found := manifest.Evidence[role]
		if !found {
			addReason("required evidence role " + role + " is missing")
			continue
		}
		if err := verifyPromotionEvidence(base, role, reference, promotionEvidenceIdentity(manifest)); err != nil {
			addReason(role + ": " + err.Error())
		}
	}
	sort.Strings(verification.Reasons)
	return verification, nil
}

// writePromotionManifestVerification writes promotion manifest verification.
func writePromotionManifestVerification(path, output string) (bool, error) {
	verification, err := verifyPromotionManifest(path)
	if err != nil {
		return false, err
	}
	raw, err := json.MarshalIndent(verification, "", "  ")
	if err != nil {
		return false, err
	}
	if output == "" {
		_, err = os.Stdout.Write(append(raw, '\n'))
	} else {
		err = os.WriteFile(output, append(raw, '\n'), 0o644)
	}
	return verification.Passed, err
}

// verifyPromotionEvidence verifies promotion evidence.
func verifyPromotionEvidence(base, role string, reference PromotionEvidenceReference, expectedIdentity PromotionEvidenceIdentity) error {
	if filepath.IsAbs(reference.Path) || reference.Path == "" {
		return fmt.Errorf("path must be a nonempty relative path")
	}
	clean := filepath.Clean(reference.Path)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes the manifest directory")
	}
	raw, err := os.ReadFile(filepath.Join(base, clean))
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return fmt.Errorf("SHA-256 mismatch")
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		return fmt.Errorf("decode report: %w", err)
	}
	identityRaw, found := document["promotion_identity"]
	if !found {
		return fmt.Errorf("report has no promotion_identity")
	}
	encodedIdentity, err := json.Marshal(identityRaw)
	if err != nil {
		return fmt.Errorf("encode promotion identity: %w", err)
	}
	var actualIdentity PromotionEvidenceIdentity
	if err := json.Unmarshal(encodedIdentity, &actualIdentity); err != nil {
		return fmt.Errorf("decode promotion identity: %w", err)
	}
	if !reflect.DeepEqual(actualIdentity, expectedIdentity) {
		return fmt.Errorf("promotion identity does not match manifest")
	}
	switch role {
	case "aa":
		if balanced, _ := document["order_balanced"].(bool); !balanced {
			return fmt.Errorf("A/A report is not order balanced")
		}
		if cases, _ := document["cases"].([]any); len(cases) == 0 {
			return fmt.Errorf("A/A report has no cases")
		}
	case "confirmation", "performance":
		if eligible, _ := document["promotion_eligible"].(bool); !eligible {
			return fmt.Errorf("report is not promotion eligible")
		}
	default:
		if passed, _ := document["passed"].(bool); !passed {
			return fmt.Errorf("report did not pass")
		}
	}
	return nil
}

// bindPromotionEvidenceReport attaches the manifest's authorization identity
// to an already generated role-specific report. The final manifest may then
// checksum the bound report without creating an identity/digest cycle.
func bindPromotionEvidenceReport(manifestPath, role, inputPath, outputPath string) error {
	if !containsString(requiredPromotionEvidenceRoles, role) {
		return fmt.Errorf("unsupported promotion evidence role %q", role)
	}
	manifestRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var manifest PromotionManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return fmt.Errorf("decode promotion manifest: %w", err)
	}
	reportRaw, err := os.ReadFile(inputPath)
	if err != nil {
		return err
	}
	var report map[string]any
	if err := json.Unmarshal(reportRaw, &report); err != nil {
		return fmt.Errorf("decode evidence report: %w", err)
	}
	report["promotion_identity"] = promotionEvidenceIdentity(manifest)
	bound, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, append(bound, '\n'), 0o644)
}
