// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	pgdriver "github.com/specterops/dawgs/drivers/pg"
)

// promotionManifestVersion reserves the stable protocol value used to recognize promotion manifest version across artifacts and executions.
const promotionManifestVersion = 2

// structuralPromotionManifestVersion adds reusable structural bucket bindings
// while retaining v2's exact-query SQL-anchor protocol.
const structuralPromotionManifestVersion = 3

const topologyPromotionManifestVersion = 4

// topologyFirstUsePromotionManifestVersion freezes the separately qualified
// first-use topology-routing protocol.
const topologyFirstUsePromotionManifestVersion = 5

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

// spI2PromotionCaps returns the exact cap contract preregistered for SP-I2
// evidence, provisional measurement, and any future production admission.
func spI2PromotionCaps() map[string]int64 {
	return map[string]int64{
		"state_limit":    optimize.ShortestPathI2QualifiedStateLimit,
		"frontier_limit": optimize.ShortestPathI2QualifiedFrontierLimit,
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

func validateStaticV8HiddenFanInBucket(bucket PromotionBucket) error {
	if bucket.Direction != "inbound" || bucket.ObservationMode != string(optimize.ShortestPathObservationDistance) ||
		bucket.MinimumDepth != 1 || bucket.MaximumDepth < 1 || bucket.MaximumDepth > 64 || bucket.RelationshipKindCount != 1 || bucket.UntypedRelationship {
		return fmt.Errorf("SP-I2 distance bucket %s must be inbound, typed single-kind, distance-only, and depth-bounded", bucket.Name)
	}
	return nil
}

// validateTopologyFixedSuffixBucket binds every v4 bucket to the classifier
// and SQL-template protocol the PostgreSQL driver will later enforce. This
// prevents a qualification artifact from authorizing a different shape than
// the live route selector.
func validateTopologyFixedSuffixBucket(manifest PromotionManifest, bucket PromotionBucket) error {
	shape := pgdriver.TraversalShape{
		Version:           bucket.StructuralShapeVersion,
		Family:            bucket.StructuralFamily,
		Direction:         bucket.Direction,
		ObservationMode:   bucket.ObservationMode,
		MinimumDepth:      int64(bucket.MinimumDepth),
		MaximumDepth:      int64(bucket.MaximumDepth),
		SuffixLength:      bucket.SuffixLength,
		CandidateStrategy: bucket.CandidateStrategy,
		Fingerprint:       bucket.StructuralShapeSHA256,
	}
	if shape.Version != pgdriver.TraversalFixedSuffixShapeVersion || shape.Family != "fixed_suffix_expansion" ||
		shape.Direction != "outbound" || shape.ObservationMode != string(optimize.ExpansionSearchObservationFullPath) ||
		shape.MinimumDepth != 0 || shape.MaximumDepth != 16 || shape.SuffixLength != 3 ||
		shape.CandidateStrategy != string(optimize.ExpansionSearchSuffixSeededReverse) ||
		!isLowerHexSHA256(shape.Fingerprint) || shape.Fingerprint != pgdriver.TraversalShapeFingerprint(shape) {
		return fmt.Errorf("topology fixed-suffix bucket %s must match the qualified outbound full-path fixed-suffix classifier envelope", bucket.Name)
	}
	if !isLowerHexSHA256(bucket.SQLTemplateSHA256) || bucket.SQLTemplateSHA256 != pgdriver.TraversalSQLTemplateSHA256(manifest.Candidate, manifest.SelectorVersion, manifest.ExecutionBoundary, shape) {
		return fmt.Errorf("topology fixed-suffix bucket %s must bind the driver's v4 SQL template digest", bucket.Name)
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
	// SuffixLength binds the terminal fixed suffix width for a v4 bucket.
	SuffixLength int `json:"suffix_length,omitempty"`
	// CandidateStrategy binds the fixed-suffix optimizer candidate used to
	// derive this v4 bucket.
	CandidateStrategy string `json:"candidate_strategy,omitempty"`
	// StructuralShapeVersion identifies the shared structural classifier for a
	// v3 production-wide bucket.
	StructuralShapeVersion string `json:"structural_shape_version,omitempty"`
	// StructuralFamily binds the SP or ASP classifier family.
	StructuralFamily string `json:"structural_family,omitempty"`
	// StructuralShapeSHA256 binds the query-text-free structural identity.
	StructuralShapeSHA256 string `json:"structural_shape_sha256,omitempty"`
	// SQLTemplateSHA256 binds the reusable candidate SQL template contract.
	SQLTemplateSHA256 string `json:"sql_template_sha256,omitempty"`
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
	// Policy binds the emitted policy generation independently of executor and selector.
	Policy string `json:"policy,omitempty"`
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
	// OperationalCandidateSQLSHA256 binds the exact rendered SQL emitted for
	// the candidate at the operational timing boundary.
	OperationalCandidateSQLSHA256 string `json:"operational_candidate_sql_sha256"`
	// TopologyEstimatorVersion binds the frozen estimator used by topology
	// selected manifest v4 buckets.
	TopologyEstimatorVersion string `json:"topology_estimator_version,omitempty"`
	// SynopsisSchemaVersion binds the compatible published synopsis schema.
	SynopsisSchemaVersion string `json:"synopsis_schema_version,omitempty"`
	// RouteCacheProtocol binds the transaction-owned route-decision contract.
	RouteCacheProtocol string `json:"route_cache_protocol,omitempty"`
	// TopologyThresholds bind the immutable estimator thresholds for manifest v4.
	TopologyThresholds map[string]int64 `json:"topology_thresholds,omitempty"`
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
	// Policy binds the emitted policy generation independently of executor and selector.
	Policy string `json:"policy,omitempty"`
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
	// OperationalCandidateSQLSHA256 binds the exact rendered SQL emitted for
	// the candidate at the operational timing boundary.
	OperationalCandidateSQLSHA256 string           `json:"operational_candidate_sql_sha256"`
	TopologyEstimatorVersion      string           `json:"topology_estimator_version,omitempty"`
	SynopsisSchemaVersion         string           `json:"synopsis_schema_version,omitempty"`
	RouteCacheProtocol            string           `json:"route_cache_protocol,omitempty"`
	TopologyThresholds            map[string]int64 `json:"topology_thresholds,omitempty"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps map[string]int64 `json:"caps"`
	// Buckets supplies the buckets input to the PromotionEvidenceIdentity contract.
	Buckets []PromotionBucket `json:"buckets"`
}

// promotionEvidenceIdentity derives the stable identity used to compare promotion evidence.
func promotionEvidenceIdentity(manifest PromotionManifest) PromotionEvidenceIdentity {
	return PromotionEvidenceIdentity{
		Candidate:                     manifest.Candidate,
		Policy:                        manifest.Policy,
		SelectorVersion:               manifest.SelectorVersion,
		ExecutionBoundary:             manifest.ExecutionBoundary,
		FallbackExecutor:              manifest.FallbackExecutor,
		SourceCommit:                  manifest.SourceCommit,
		SourceSHA256:                  manifest.SourceSHA256,
		BinarySHA256:                  manifest.BinarySHA256,
		CorpusSHA256:                  manifest.CorpusSHA256,
		OperationalCandidateSQLSHA256: manifest.OperationalCandidateSQLSHA256,
		TopologyEstimatorVersion:      manifest.TopologyEstimatorVersion,
		SynopsisSchemaVersion:         manifest.SynopsisSchemaVersion,
		RouteCacheProtocol:            manifest.RouteCacheProtocol,
		TopologyThresholds:            clonePromotionCaps(manifest.TopologyThresholds),
		Caps:                          clonePromotionCaps(manifest.Caps),
		Buckets:                       clonePromotionBuckets(manifest.Buckets),
	}
}

// clonePromotionCaps returns an independent copy of promotion caps.
func clonePromotionCaps(input map[string]int64) map[string]int64 {
	if input == nil {
		return nil
	}
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

// validatePromotionBucketSets enforces the set-valued manifest fields used by
// both final verification and provisional capture. A single SQL anchor is
// meaningful only for one unique query identity, and qualification evidence
// must close the exact training/holdout split rather than a superset.
func validatePromotionBucketSets(version int, buckets []PromotionBucket) []string {
	var reasons []string
	seenBuckets := map[string]struct{}{}
	seenQueries := map[string]string{}

	for _, bucket := range buckets {
		if strings.TrimSpace(bucket.Name) == "" || len(bucket.QuerySHA256) == 0 {
			reasons = append(reasons, "every bucket requires a name and query allowlist")
			continue
		}
		if _, found := seenBuckets[bucket.Name]; found {
			reasons = append(reasons, "bucket "+bucket.Name+" is duplicated")
		}
		seenBuckets[bucket.Name] = struct{}{}

		seenBucketQueries := map[string]struct{}{}
		for _, query := range bucket.QuerySHA256 {
			if !isLowerHexSHA256(query) {
				reasons = append(reasons, "bucket "+bucket.Name+" contains an invalid query digest")
				continue
			}
			if _, duplicate := seenBucketQueries[query]; duplicate {
				reasons = append(reasons, "bucket "+bucket.Name+" duplicates query digest "+query)
			}
			seenBucketQueries[query] = struct{}{}
			if owner, duplicate := seenQueries[query]; duplicate && owner != bucket.Name {
				reasons = append(reasons, "query digest "+query+" is authorized by more than one bucket")
			} else {
				seenQueries[query] = bucket.Name
			}
		}

		if !reflect.DeepEqual(bucket.QualificationSplit, []string{"training", "holdout"}) {
			reasons = append(reasons, "bucket "+bucket.Name+" must bind exactly one training and one holdout qualification split in canonical order")
		}
		if (version == structuralPromotionManifestVersion || version == topologyPromotionManifestVersion || version == topologyFirstUsePromotionManifestVersion) && (bucket.StructuralShapeVersion == "" || bucket.StructuralFamily == "" || !isLowerHexSHA256(bucket.StructuralShapeSHA256) || !isLowerHexSHA256(bucket.SQLTemplateSHA256)) {
			reasons = append(reasons, "structural bucket "+bucket.Name+" requires classifier version, family, shape digest, and SQL template digest")
		}
	}
	if version == promotionManifestVersion && len(seenQueries) != 1 {
		reasons = append(reasons, "operational SQL anchor requires exactly one authorized query digest")
	}
	if (version == structuralPromotionManifestVersion || version == topologyPromotionManifestVersion || version == topologyFirstUsePromotionManifestVersion) && len(seenQueries) == 0 {
		reasons = append(reasons, "structural promotion requires at least one evidence query digest")
	}
	return reasons
}

// validatePromotionEvidenceRoleSet rejects missing and invented evidence
// roles. JSON object keys are unique after strict duplicate-key validation, so
// exact cardinality here closes the role set rather than checking a subset.
func validatePromotionEvidenceRoleSet(evidence map[string]PromotionEvidenceReference) []string {
	var reasons []string
	required := make(map[string]struct{}, len(requiredPromotionEvidenceRoles))
	for _, role := range requiredPromotionEvidenceRoles {
		required[role] = struct{}{}
		if _, found := evidence[role]; !found {
			reasons = append(reasons, "required evidence role "+role+" is missing")
		}
	}
	for role := range evidence {
		if _, found := required[role]; !found {
			reasons = append(reasons, "unsupported evidence role "+role+" is present")
		}
	}
	return reasons
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
	if err := decodePromotionEvidence(raw, &manifest); err != nil {
		return PromotionManifestVerification{}, fmt.Errorf("decode promotion manifest: %w", err)
	}
	verification.Candidate = manifest.Candidate
	verification.SelectorVersion = manifest.SelectorVersion
	addReason := func(reason string) {
		verification.Passed = false
		verification.Reasons = append(verification.Reasons, reason)
	}
	if manifest.Version != promotionManifestVersion && manifest.Version != structuralPromotionManifestVersion && manifest.Version != topologyPromotionManifestVersion && manifest.Version != topologyFirstUsePromotionManifestVersion {
		addReason("manifest version must be 2, 3, 4, or 5")
	}
	if strings.TrimSpace(manifest.Candidate) == "" || strings.TrimSpace(manifest.SelectorVersion) == "" {
		addReason("candidate and selector_version are required")
	}
	if manifest.ExecutionBoundary != "inline_statement" && manifest.ExecutionBoundary != "stored_helper" && manifest.ExecutionBoundary != "guarded_dual_arm" && manifest.ExecutionBoundary != "transaction_retry" && manifest.ExecutionBoundary != "first_use_transaction_retry" {
		addReason("execution_boundary must identify the measured production boundary")
	}
	for name, value := range map[string]string{"source_sha256": manifest.SourceSHA256, "binary_sha256": manifest.BinarySHA256, "corpus_sha256": manifest.CorpusSHA256} {
		if !isLowerHexSHA256(value) {
			addReason(name + " must be a lowercase SHA-256 digest")
		}
	}
	if !isLowerHexSHA256(manifest.OperationalCandidateSQLSHA256) {
		addReason("operational_candidate_sql_sha256 must be a lowercase SHA-256 digest")
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
	if manifest.Candidate == string(optimize.ShortestPathExecutorI2GuardedDistance) {
		expectedCaps := spI2PromotionCaps()
		if manifest.ExecutionBoundary != "guarded_dual_arm" {
			addReason("SP-I2 distance requires the guarded_dual_arm production boundary")
		}
		if manifest.FallbackExecutor != string(optimize.ShortestPathExecutorS4CanonicalDistance) {
			addReason("SP-I2 distance requires SP-S4-C-D as its exact fallback")
		}
		if len(manifest.Caps) != len(expectedCaps) {
			addReason("SP-I2 distance requires exactly state and frontier caps")
		}
		for name, expected := range expectedCaps {
			if actual, found := manifest.Caps[name]; !found || actual != expected {
				addReason(fmt.Sprintf("SP-I2 distance cap %s must equal %d", name, expected))
			}
		}
		if manifest.SelectorVersion != optimize.ShortestPathSelectorStaticV8HiddenFanIn {
			addReason("SP-I2 distance requires selector " + optimize.ShortestPathSelectorStaticV8HiddenFanIn)
		}
	}
	if isOrientationProbePolicy(manifest.Candidate) {
		expectedCaps := orientationPromotionCaps()
		if manifest.SelectorVersion != manifest.Candidate {
			addReason(fmt.Sprintf("%s requires the same selector version", manifest.Candidate))
		}
		if manifest.ExecutionBoundary != "guarded_dual_arm" {
			addReason(manifest.Candidate + " requires the guarded_dual_arm production boundary")
		}
		if manifest.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) {
			addReason(manifest.Candidate + " requires EXPANSION-STEPWISE-FORWARD as its exact fallback")
		}
		if len(manifest.Caps) != len(expectedCaps) {
			addReason(manifest.Candidate + " requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps")
		}
		for name, expected := range expectedCaps {
			if manifest.Caps[name] != expected {
				addReason(fmt.Sprintf("%s cap %s must equal %d", manifest.Candidate, name, expected))
			}
		}
	}
	if manifest.Candidate == string(optimize.ExpansionSearchPolicyOrientationProbeV2) {
		addReason("orientation-probe-v2 is terminally rejected because its immutable training overhead gate failed; authorization requires a new policy generation")
	}
	if manifest.Candidate == string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1) {
		expectedCaps := map[string]int64{
			"suffix_row_limit":   optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit,
			"state_limit":        optimize.ExpansionSearchSuffixReverseGuardStateLimit,
			"output_row_limit":   optimize.ExpansionSearchSuffixReverseRetryOutputRowLimit,
			"output_bytes_limit": optimize.ExpansionSearchSuffixReverseRetryOutputBytesLimit,
		}
		if manifest.Version != topologyPromotionManifestVersion || manifest.ExecutionBoundary != "transaction_retry" || manifest.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) {
			addReason("topology fixed-suffix requires manifest v4, transaction_retry, and EXPANSION-STEPWISE-FORWARD fallback")
		}
		if !reflect.DeepEqual(manifest.Caps, expectedCaps) {
			addReason("topology fixed-suffix requires the exact frozen suffix, state, output-row, and output-byte caps")
		}
		if manifest.SelectorVersion != string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1) || manifest.TopologyEstimatorVersion != "topology-fixed-suffix-counts-v1" || manifest.SynopsisSchemaVersion != "topology-synopsis-schema-v2" || manifest.RouteCacheProtocol != "topology-selected-routing-v1" || !reflect.DeepEqual(manifest.TopologyThresholds, map[string]int64{"maximum_edge_to_node_ratio_per_mille": 1000}) {
			addReason("topology fixed-suffix requires its selector, estimator, synopsis schema, and route-cache protocol identities")
		}
	}
	if manifest.Candidate == string(optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1) {
		expectedCaps := map[string]int64{
			"suffix_row_limit":   optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit,
			"state_limit":        optimize.ExpansionSearchSuffixReverseGuardStateLimit,
			"output_row_limit":   optimize.ExpansionSearchSuffixReverseRetryOutputRowLimit,
			"output_bytes_limit": optimize.ExpansionSearchSuffixReverseRetryOutputBytesLimit,
		}
		if manifest.Version != topologyFirstUsePromotionManifestVersion || manifest.ExecutionBoundary != "first_use_transaction_retry" || manifest.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) {
			addReason("topology fixed-suffix first-use requires manifest v5, first_use_transaction_retry, and EXPANSION-STEPWISE-FORWARD fallback")
		}
		if !reflect.DeepEqual(manifest.Caps, expectedCaps) {
			addReason("topology fixed-suffix first-use requires the exact frozen suffix, state, output-row, and output-byte caps")
		}
		if manifest.SelectorVersion != string(optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1) || manifest.TopologyEstimatorVersion != "topology-fixed-suffix-counts-v1" || manifest.SynopsisSchemaVersion != "topology-synopsis-schema-v2" || manifest.RouteCacheProtocol != "topology-selected-first-use-routing-v1" || !reflect.DeepEqual(manifest.TopologyThresholds, map[string]int64{"maximum_edge_to_node_ratio_per_mille": 1000}) {
			addReason("topology fixed-suffix first-use requires its selector, estimator, synopsis schema, and route-cache protocol identities")
		}
	}
	if len(manifest.Buckets) == 0 {
		addReason("at least one authorized bucket is required")
	}
	for _, reason := range validatePromotionBucketSets(manifest.Version, manifest.Buckets) {
		addReason(reason)
	}
	for _, bucket := range manifest.Buckets {
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
		if manifest.Candidate == string(optimize.ShortestPathExecutorI2GuardedDistance) {
			if err := validateStaticV8HiddenFanInBucket(bucket); err != nil {
				addReason(err.Error())
			}
		}
		if manifest.Candidate == string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1) || manifest.Candidate == string(optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1) {
			if err := validateTopologyFixedSuffixBucket(manifest, bucket); err != nil {
				addReason(err.Error())
			}
		}
	}
	for _, reason := range validatePromotionEvidenceRoleSet(manifest.Evidence) {
		addReason(reason)
	}
	base := filepath.Dir(path)
	for _, role := range requiredPromotionEvidenceRoles {
		reference, found := manifest.Evidence[role]
		if !found {
			continue
		}
		if err := verifyPromotionEvidence(base, role, reference, promotionEvidenceIdentity(manifest)); err != nil {
			addReason(role + ": " + err.Error())
		}
	}
	if err := validatePromotionEvidenceClosure(base, manifest.Evidence, promotionEvidenceIdentity(manifest)); err != nil {
		addReason("evidence closure: " + err.Error())
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
	raw, err := readContainedPromotionEvidence(base, reference.Path)
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
	if err := validatePromotionEvidenceDocument(role, raw, expectedIdentity); err != nil {
		return err
	}
	return nil
}

// readContainedPromotionEvidence resolves both the manifest directory and the
// referenced evidence through symlinks, then proves the resolved report is
// still beneath that directory. Lexical path cleaning alone does not stop an
// in-tree symlink from redirecting final authorization to an external file.
func readContainedPromotionEvidence(base, referencePath string) ([]byte, error) {
	if filepath.IsAbs(referencePath) || referencePath == "" {
		return nil, fmt.Errorf("path must be a nonempty relative path")
	}
	clean := filepath.Clean(referencePath)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("path escapes the manifest directory")
	}
	resolvedBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest directory: %w", err)
	}
	resolvedPath, err := filepath.EvalSymlinks(filepath.Join(base, clean))
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(resolvedBase, resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("compare evidence path with manifest directory: %w", err)
	}
	if relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("path escapes the manifest directory through a symlink")
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path must resolve to a regular file")
	}
	return os.ReadFile(resolvedPath)
}

// validatePromotionEvidenceDocument strictly decodes the schema associated
// with a required evidence role. The identity and checksum are checked by the
// caller first; these checks prevent a correctly bound but structurally empty
// or internally contradictory JSON object from satisfying promotion.
func validatePromotionEvidenceDocument(role string, raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	switch role {
	case "aa":
		return validatePromotionAAReport(raw, expectedIdentity)
	case "confirmation":
		return validatePromotionConfirmationReport(raw, expectedIdentity)
	case "performance":
		return validatePromotionPerformanceReport(raw, expectedIdentity)
	case "resource":
		return validatePromotionResourceReport(raw, expectedIdentity)
	case "reference_closure":
		return validatePromotionReferenceClosureReport(raw, expectedIdentity)
	case "operational":
		return validatePromotionOperationalReport(raw, expectedIdentity)
	default:
		return fmt.Errorf("unsupported promotion evidence role %q", role)
	}
}

// decodePromotionEvidence rejects unknown fields and concatenated documents.
// Bound report schemas embed their promotion identity separately because the
// native report producers deliberately avoid a manifest digest cycle.
func decodePromotionEvidence(raw []byte, destination any) error {
	if err := rejectDuplicateJSONObjectKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode report schema: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("report contains trailing JSON data")
		}
		return fmt.Errorf("decode trailing report data: %w", err)
	}
	return nil
}

// rejectDuplicateJSONObjectKeys walks the complete JSON token stream and
// rejects duplicate keys at every nesting level. encoding/json otherwise lets
// a later duplicate silently overwrite an authorization-relevant field.
func rejectDuplicateJSONObjectKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := rejectDuplicateJSONValue(decoder); err != nil {
		return fmt.Errorf("decode report schema: %w", err)
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("report contains trailing JSON data after %v", token)
		}
		return fmt.Errorf("decode trailing report data: %w", err)
	}
	return nil
}

func rejectDuplicateJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON object key %q", key)
			}
			seen[key] = struct{}{}
			if err := rejectDuplicateJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("object has invalid closing delimiter")
		}
	case '[':
		for decoder.More() {
			if err := rejectDuplicateJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("array has invalid closing delimiter")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	return nil
}

type promotionResourceReport struct {
	ResourceGateReport
	PromotionIdentity  PromotionEvidenceIdentity `json:"promotion_identity"`
	NativeReportSHA256 string                    `json:"native_report_sha256"`
	NativeReportBase64 string                    `json:"native_report_base64"`
}

func validatePromotionResourceReport(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionResourceReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("resource report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("resource report promotion identity does not match manifest")
	}
	report := bound.ResourceGateReport
	nativeRaw, err := promotionEmbeddedNativeReport("resource", bound.NativeReportSHA256, bound.NativeReportBase64)
	if err != nil {
		return err
	}
	var native ResourceGateReport
	if err := decodePromotionEvidence(nativeRaw, &native); err != nil {
		return fmt.Errorf("resource native producer report: %w", err)
	}
	if !reflect.DeepEqual(native, report) {
		return fmt.Errorf("resource bound projection differs from its native producer report")
	}
	if report.Version != resourceGateVersion {
		return fmt.Errorf("resource report version must be %d", resourceGateVersion)
	}
	if !lowercaseSHA256(report.ArtifactSHA256) {
		return fmt.Errorf("resource report artifact_sha256 is not a canonical SHA-256 digest")
	}
	if !report.Passed {
		return fmt.Errorf("resource report did not pass")
	}
	if len(report.Cases) == 0 {
		return fmt.Errorf("resource report has no cases")
	}
	expectedLimits, supported := promotionResourceNumericLimits(expectedIdentity)
	if !supported {
		return fmt.Errorf("resource report candidate %q has no registered numeric cap contract", expectedIdentity.Candidate)
	}
	seen := map[string]struct{}{}
	seenInvocations := map[string]struct{}{}
	for _, gateCase := range report.Cases {
		if strings.TrimSpace(gateCase.Dataset) == "" || strings.TrimSpace(gateCase.Name) == "" || strings.TrimSpace(gateCase.Tier) == "" ||
			(gateCase.QualificationSplit != "training" && gateCase.QualificationSplit != "holdout") ||
			!promotionResourceArchitectureAllowed(expectedIdentity.Candidate, gateCase.Architecture) || gateCase.Reference != "" || gateCase.FallbackArchitecture != "" ||
			gateCase.Round < 1 || gateCase.Round > 20 || gateCase.Block != gateCase.Round || strings.TrimSpace(gateCase.RunUUID) == "" ||
			strings.TrimSpace(gateCase.Arm) == "" || gateCase.ArmOrder < 1 || gateCase.ArmOrder > 2 {
			return fmt.Errorf("resource report contains an incomplete case identity")
		}
		if !gateCase.Passed || len(gateCase.Reasons) != 0 {
			return fmt.Errorf("resource report passing disposition contradicts case %s/%s", gateCase.Dataset, gateCase.Name)
		}
		if !reflect.DeepEqual(gateCase.NumericLimits, expectedLimits) || len(gateCase.NumericObserved) != len(expectedLimits) {
			return fmt.Errorf("resource case %s/%s does not use the manifest candidate's exact numeric limits", gateCase.Dataset, gateCase.Name)
		}
		for name, limit := range expectedLimits {
			observed, found := gateCase.NumericObserved[name]
			if !found || observed < 0 || observed > limit {
				return fmt.Errorf("resource case %s/%s observation %s=%d is absent, negative, or exceeds limit %d", gateCase.Dataset, gateCase.Name, name, observed, limit)
			}
		}
		if len(gateCase.RuntimeReceiptChains) < 50 {
			return fmt.Errorf("resource case %s/%s lacks at least 50 candidate runtime receipts", gateCase.Dataset, gateCase.Name)
		}
		if err := validatePromotionReceiptChains(gateCase.RuntimeReceiptChains, expectedIdentity.Candidate, seenInvocations); err != nil {
			return fmt.Errorf("resource case %s/%s: %w", gateCase.Dataset, gateCase.Name, err)
		}
		key := fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%s\x00%s\x00%s", gateCase.Dataset, gateCase.Name, gateCase.Round, gateCase.Block, gateCase.RunUUID, gateCase.Arm, gateCase.Reference)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("resource report duplicates a case decision")
		}
		seen[key] = struct{}{}
	}
	return nil
}

type promotionReferenceClosureReport struct {
	ReferenceClosureReport
	PromotionIdentity  PromotionEvidenceIdentity `json:"promotion_identity"`
	NativeReportSHA256 string                    `json:"native_report_sha256"`
	NativeReportBase64 string                    `json:"native_report_base64"`
}

func validatePromotionReferenceClosureReport(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var bound promotionReferenceClosureReport
	if err := decodePromotionEvidence(raw, &bound); err != nil {
		return fmt.Errorf("reference-closure report: %w", err)
	}
	if !reflect.DeepEqual(bound.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("reference-closure report promotion identity does not match manifest")
	}
	report := bound.ReferenceClosureReport
	nativeRaw, err := promotionEmbeddedNativeReport("reference-closure", bound.NativeReportSHA256, bound.NativeReportBase64)
	if err != nil {
		return err
	}
	var native ReferenceClosureReport
	if err := decodePromotionEvidence(nativeRaw, &native); err != nil {
		return fmt.Errorf("reference-closure native producer report: %w", err)
	}
	if !reflect.DeepEqual(native, report) {
		return fmt.Errorf("reference-closure bound projection differs from its native producer report")
	}
	if report.Version != referenceClosureReportVersion {
		return fmt.Errorf("reference-closure report version must be %d", referenceClosureReportVersion)
	}
	if !lowercaseSHA256(report.ArtifactSHA256) {
		return fmt.Errorf("reference-closure report artifact_sha256 is not a canonical SHA-256 digest")
	}
	if report.Seed != 1 || report.Confidence != defaultConfidenceLevel || report.BootstrapCount != defaultBootstrapCount ||
		math.IsNaN(report.Confidence) || math.IsInf(report.Confidence, 0) || strings.TrimSpace(report.ReferenceName) == "" {
		return fmt.Errorf("reference-closure report has invalid frozen settings")
	}
	if report.Candidate != expectedIdentity.Candidate || report.SourceCommit != expectedIdentity.SourceCommit ||
		report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() || report.BinarySHA256 != expectedIdentity.BinarySHA256 ||
		report.CorpusSHA256 != expectedIdentity.CorpusSHA256 {
		return fmt.Errorf("reference-closure report source, binary, corpus, or candidate identity differs from the manifest")
	}
	if !report.Passed {
		return fmt.Errorf("reference-closure report did not pass")
	}
	if len(report.Cases) == 0 {
		return fmt.Errorf("reference-closure report has no cases")
	}
	seen := map[string]struct{}{}
	seenInvocations := map[string]struct{}{}
	for _, closureCase := range report.Cases {
		if strings.TrimSpace(closureCase.Dataset) == "" || strings.TrimSpace(closureCase.Name) == "" ||
			(closureCase.QualificationSplit != "training" && closureCase.QualificationSplit != "holdout") ||
			!lowercaseSHA256(closureCase.WorkloadSHA256) || promotionIdentityQueryCount(expectedIdentity, closureCase.QuerySHA256) != 1 ||
			closureCase.ReferenceName != report.ReferenceName || strings.TrimSpace(closureCase.ReferenceArchitecture) == "" {
			return fmt.Errorf("reference-closure report contains an incomplete case identity")
		}
		if closureCase.Rounds < 10 || closureCase.Rounds > 20 || closureCase.ProductionSamples < closureCase.Rounds*50 || closureCase.ReferenceSamples < closureCase.Rounds*50 {
			return fmt.Errorf("reference-closure case %s/%s lacks the required rounds or samples", closureCase.Dataset, closureCase.Name)
		}
		if !validRatioInterval(closureCase.MedianRatio) || !validDurationInterval(closureCase.MedianChange) ||
			closureCase.RatioUpperLimit != 1.10 || closureCase.AbsoluteFloor != 100*time.Microsecond ||
			closureCase.ProductionAAResolution < 0 || closureCase.ReferenceAAResolution < 0 ||
			closureCase.AbsoluteResolution < closureCase.AbsoluteFloor || closureCase.AbsoluteGapUpper < 0 {
			return fmt.Errorf("reference-closure case %s/%s has invalid statistical evidence", closureCase.Dataset, closureCase.Name)
		}
		expectedGap := max(absDuration(closureCase.MedianChange.Lower), absDuration(closureCase.MedianChange.Upper))
		if closureCase.AbsoluteGapUpper != expectedGap || closureCase.AbsoluteResolution != max(closureCase.AbsoluteFloor, closureCase.ProductionAAResolution, closureCase.ReferenceAAResolution) {
			return fmt.Errorf("reference-closure case %s/%s has inconsistent derived evidence", closureCase.Dataset, closureCase.Name)
		}
		if !closureCase.Passed || len(closureCase.Reasons) != 0 || closureCase.MedianRatio.Upper > closureCase.RatioUpperLimit && closureCase.AbsoluteGapUpper > closureCase.AbsoluteResolution {
			return fmt.Errorf("reference-closure report passing disposition contradicts case %s/%s", closureCase.Dataset, closureCase.Name)
		}
		if len(closureCase.ProductionRuntimeReceiptChains) != closureCase.ProductionSamples {
			return fmt.Errorf("reference-closure case %s/%s runtime receipt count differs from production samples", closureCase.Dataset, closureCase.Name)
		}
		if err := validatePromotionReceiptChains(closureCase.ProductionRuntimeReceiptChains, expectedIdentity.Candidate, seenInvocations); err != nil {
			return fmt.Errorf("reference-closure case %s/%s: %w", closureCase.Dataset, closureCase.Name, err)
		}
		key := closureCase.Dataset + "\x00" + closureCase.Name
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("reference-closure report duplicates a case decision")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func promotionEmbeddedNativeReport(role, expectedSHA256, encoded string) ([]byte, error) {
	if !lowercaseSHA256(expectedSHA256) {
		return nil, fmt.Errorf("%s native producer report SHA-256 is not canonical", role)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) == 0 {
		return nil, fmt.Errorf("%s report does not contain decodable native producer bytes", role)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != expectedSHA256 {
		return nil, fmt.Errorf("%s native producer report SHA-256 does not match its embedded bytes", role)
	}
	return raw, nil
}

func promotionResourceNumericLimits(identity PromotionEvidenceIdentity) (map[string]int64, bool) {
	switch identity.Candidate {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness), string(optimize.ShortestPathExecutorASPI1DAG):
		return map[string]int64{
			"state_rows":       identity.Caps["state_limit"],
			"predecessor_rows": identity.Caps["predecessor_limit"],
			"output_rows":      identity.Caps["enumeration_limit"],
			"output_bytes":     identity.Caps["output_bytes_limit"],
		}, true
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		return spI2TelemetryCaps(), true
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1), string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		return map[string]int64{
			"forward_seed_rows":       identity.Caps["root_row_limit"],
			"reverse_seed_rows":       identity.Caps["reverse_seed_row_limit"],
			"directional_degree_rows": identity.Caps["directional_degree_row_limit"],
			"state_rows":              identity.Caps["state_limit"],
		}, true
	default:
		return nil, false
	}
}

func promotionResourceArchitectureAllowed(candidate, architecture string) bool {
	if candidate == string(optimize.ExpansionSearchPolicyOrientationProbeV1) || candidate == string(optimize.ExpansionSearchPolicyOrientationProbeV2) {
		return architecture == string(optimize.ExpansionSearchStepwiseForward) || architecture == string(optimize.ExpansionSearchSuffixSeededReverse)
	}
	return architecture == candidate
}

func validRatioInterval(interval RatioInterval) bool {
	return !math.IsNaN(interval.Estimate) && !math.IsNaN(interval.Lower) && !math.IsNaN(interval.Upper) &&
		!math.IsInf(interval.Estimate, 0) && !math.IsInf(interval.Lower, 0) && !math.IsInf(interval.Upper, 0) &&
		interval.Lower > 0 && interval.Lower <= interval.Estimate && interval.Estimate <= interval.Upper
}

func validDurationInterval(interval DurationInterval) bool {
	return interval.Lower <= interval.Estimate && interval.Estimate <= interval.Upper
}

func validatePromotionOperationalReport(raw []byte, expectedIdentity PromotionEvidenceIdentity) error {
	var report OperationalGateReport
	if err := decodePromotionEvidence(raw, &report); err != nil {
		return fmt.Errorf("operational report: %w", err)
	}
	return validateRecomputedOperationalGateReport(report, expectedIdentity)
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
	if err := decodePromotionEvidence(manifestRaw, &manifest); err != nil {
		return fmt.Errorf("decode promotion manifest: %w", err)
	}
	if reasons := validatePromotionBucketSets(manifest.Version, manifest.Buckets); len(reasons) != 0 {
		return fmt.Errorf("promotion manifest has an invalid query/split set: %s", strings.Join(reasons, "; "))
	}
	if manifest.OperationalCandidateSQLSHA256 != "" && !isLowerHexSHA256(manifest.OperationalCandidateSQLSHA256) {
		return fmt.Errorf("promotion manifest has an invalid operational candidate SQL SHA-256")
	}
	reportRaw, err := os.ReadFile(inputPath)
	if err != nil {
		return err
	}
	if err := rejectDuplicateJSONObjectKeys(reportRaw); err != nil {
		return fmt.Errorf("decode evidence report: %w", err)
	}
	var report map[string]any
	decoder := json.NewDecoder(bytes.NewReader(reportRaw))
	decoder.UseNumber()
	if err := decoder.Decode(&report); err != nil {
		return fmt.Errorf("decode evidence report: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode evidence report: trailing JSON data")
		}
		return fmt.Errorf("decode evidence report trailing data: %w", err)
	}
	if _, exists := report["promotion_identity"]; exists {
		return fmt.Errorf("evidence report is already promotion-bound")
	}
	if _, exists := report["native_report_sha256"]; exists {
		return fmt.Errorf("evidence report contains reserved native_report_sha256")
	}
	if _, exists := report["native_report_base64"]; exists {
		return fmt.Errorf("evidence report contains reserved native_report_base64")
	}
	if role == "aa" || role == "resource" || role == "reference_closure" {
		digest := sha256.Sum256(reportRaw)
		report["native_report_sha256"] = hex.EncodeToString(digest[:])
		report["native_report_base64"] = base64.StdEncoding.EncodeToString(reportRaw)
	}
	report["promotion_identity"] = promotionEvidenceIdentity(manifest)
	bound, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, append(bound, '\n'), 0o644)
}
