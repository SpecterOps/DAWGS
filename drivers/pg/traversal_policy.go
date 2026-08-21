package pg

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

// TraversalPolicy is a default-off, query-allowlisted production canary. A
// generation is mandatory whenever a candidate is enabled and is included in
// the translation cache identity.
type TraversalPolicy struct {
	// Generation supplies the generation input to the TraversalPolicy contract.
	Generation uint64 `json:"generation"`
	// PromotionManifestSHA256 binds the referenced promotion manifest content by SHA-256 digest.
	PromotionManifestSHA256 string `json:"promotion_manifest_sha256"`
	// PromotionManifestJSON is the exact verified authorization document. It
	// is intentionally excluded from policy serialization; its digest and
	// content-derived fields form the cache identity.
	PromotionManifestJSON json.RawMessage `json:"-"`
	// QuerySHA256Allowlist binds the referenced query sha256 allowlist content by SHA-256 digest.
	QuerySHA256Allowlist []string `json:"query_sha256_allowlist"`
	// ShortestPathExecutor supplies the shortest path executor input to the TraversalPolicy contract.
	ShortestPathExecutor optimize.ShortestPathExecutor `json:"shortest_path_executor,omitempty"`
	// EnableExpansionOrientation indicates whether enable expansion orientation applies.
	EnableExpansionOrientation bool `json:"enable_expansion_orientation,omitempty"`
	// EnableTopologyFixedSuffix permits the manifest-v4, snapshot-owned
	// fixed-suffix candidate. It never changes ordinary routing: the
	// transaction route-decision cache must independently select the arm.
	EnableTopologyFixedSuffix bool `json:"enable_topology_fixed_suffix,omitempty"`
	// EnableTopologyFixedSuffixFirstUse permits the manifest-v5 first-use
	// selector. It is deliberately independent of the v4 cache-hit protocol.
	EnableTopologyFixedSuffixFirstUse bool `json:"enable_topology_fixed_suffix_first_use,omitempty"`
	// DisableExpansionOrientation is an evidence-free emergency rollback switch
	// for any manifest-authorized orientation selector.
	DisableExpansionOrientation bool `json:"disable_expansion_orientation,omitempty"`
	// DisableTopologyFixedSuffix is the evidence-free emergency rollback
	// switch for the manifest-v4 fixed-suffix candidate.
	DisableTopologyFixedSuffix bool `json:"disable_topology_fixed_suffix,omitempty"`
	// DisableTopologyFixedSuffixFirstUse is the emergency rollback for v5.
	DisableTopologyFixedSuffixFirstUse bool `json:"disable_topology_fixed_suffix_first_use,omitempty"`
	// DisableEndpointSeededReverse indicates whether disable endpoint seeded reverse applies.
	DisableEndpointSeededReverse bool `json:"disable_endpoint_seeded_reverse,omitempty"`
	// DisableInlineASPDAG indicates whether disable inline aspdag applies.
	DisableInlineASPDAG bool `json:"disable_inline_asp_dag,omitempty"`
	// DisableInlineSPWitness indicates whether disable inline sp witness applies.
	DisableInlineSPWitness bool `json:"disable_inline_sp_witness,omitempty"`
	// DisableInlineSPDistance is the emergency rollback switch for SP-I2-C-D.
	DisableInlineSPDistance bool `json:"disable_inline_sp_distance,omitempty"`
	// compiledManifest retains the compiled manifest while TraversalPolicy is assembled or evaluated.
	compiledManifest traversalPromotionManifest
	// compiledBuckets retains the compiled buckets while TraversalPolicy is assembled or evaluated.
	compiledBuckets map[string]traversalPromotionBucket
	// compiledIdentity identifies the compiled identity.
	compiledIdentity string
}

// enabled reports whether the policy changes any production translation behavior.
func (s TraversalPolicy) enabled() bool {
	return s.ShortestPathExecutor != "" || s.EnableExpansionOrientation || s.EnableTopologyFixedSuffix || s.EnableTopologyFixedSuffixFirstUse || s.DisableExpansionOrientation || s.DisableTopologyFixedSuffix || s.DisableTopologyFixedSuffixFirstUse || s.DisableEndpointSeededReverse || s.DisableInlineASPDAG || s.DisableInlineSPWitness || s.DisableInlineSPDistance
}

// rollbackActive reports whether an emergency rollback can change the SQL
// authorized by a promotion manifest. Rollback generations retain their own
// cache identity, but must not compare incumbent SQL with the candidate anchor.
func (s TraversalPolicy) rollbackActive() bool {
	return s.DisableExpansionOrientation || s.DisableTopologyFixedSuffix || s.DisableTopologyFixedSuffixFirstUse || s.DisableEndpointSeededReverse || s.DisableInlineASPDAG || s.DisableInlineSPWitness || s.DisableInlineSPDistance
}

// manifestCandidateEnabled reports whether this policy carries a candidate
// whose authorization depends on a promotion manifest.
func (s TraversalPolicy) manifestCandidateEnabled() bool {
	return s.ShortestPathExecutor != "" || s.EnableExpansionOrientation || s.EnableTopologyFixedSuffix || s.EnableTopologyFixedSuffixFirstUse
}

// rollbackSwitchCount returns the number of emergency controls enabled in the
// policy. Manifest-backed candidates may compose with at most one switch, and
// that switch must be the control dedicated to the candidate family.
func (s TraversalPolicy) rollbackSwitchCount() int {
	count := 0
	for _, enabled := range []bool{
		s.DisableExpansionOrientation,
		s.DisableTopologyFixedSuffix,
		s.DisableTopologyFixedSuffixFirstUse,
		s.DisableEndpointSeededReverse,
		s.DisableInlineASPDAG,
		s.DisableInlineSPWitness,
		s.DisableInlineSPDistance,
	} {
		if enabled {
			count++
		}
	}
	return count
}

// matchingCandidateRollbackActive reports whether exactly one emergency
// switch is enabled and it belongs to the manifest-backed candidate.
func (s TraversalPolicy) matchingCandidateRollbackActive() bool {
	if s.rollbackSwitchCount() != 1 {
		return false
	}
	if s.EnableExpansionOrientation {
		return s.DisableExpansionOrientation
	}
	if s.EnableTopologyFixedSuffix {
		return s.DisableTopologyFixedSuffix
	}
	if s.EnableTopologyFixedSuffixFirstUse {
		return s.DisableTopologyFixedSuffixFirstUse
	}
	switch s.ShortestPathExecutor {
	case optimize.ShortestPathExecutorASPI1DAG:
		return s.DisableInlineASPDAG
	case optimize.ShortestPathExecutorI1CanonicalPredecessorWitness:
		return s.DisableInlineSPWitness
	case optimize.ShortestPathExecutorI2GuardedDistance:
		return s.DisableInlineSPDistance
	default:
		return false
	}
}

// withoutManifestCandidate derives the incumbent-only form used by a matching
// emergency rollback. The installed manifest and policy remain immutable.
func (s TraversalPolicy) withoutManifestCandidate() TraversalPolicy {
	s.ShortestPathExecutor = ""
	s.EnableExpansionOrientation = false
	s.EnableTopologyFixedSuffix = false
	s.EnableTopologyFixedSuffixFirstUse = false
	return s
}

// productionOptions derives validated translation options from the active traversal policy.
func (s TraversalPolicy) productionOptions(query string) (translate.ProductionOptions, error) {
	return s.productionOptionsForShape(query, TraversalShape{})
}

func (s TraversalPolicy) productionOptionsForShape(query string, shape TraversalShape) (translate.ProductionOptions, error) {
	manifest := s.compiledManifest
	if manifest.SelectorVersion == "" && len(s.PromotionManifestJSON) > 0 {
		var err error
		if manifest, err = decodeTraversalPromotionManifest(s.PromotionManifestJSON); err != nil {
			return translate.ProductionOptions{}, fmt.Errorf("decode traversal promotion manifest: %w", err)
		}
	}
	selectorVersion := manifest.SelectorVersion
	if selectorVersion == "" {
		selectorVersion = fmt.Sprintf("traversal-kill-switch-g%d", s.Generation)
		if s.DisableExpansionOrientation && !s.DisableEndpointSeededReverse && !s.DisableInlineASPDAG && !s.DisableInlineSPWitness && !s.DisableInlineSPDistance {
			selectorVersion = fmt.Sprintf("expansion-orientation-kill-switch-g%d", s.Generation)
		} else if s.DisableEndpointSeededReverse && !s.DisableInlineASPDAG {
			selectorVersion = fmt.Sprintf("endpoint-seeded-kill-switch-g%d", s.Generation)
		} else if s.DisableInlineASPDAG && !s.DisableEndpointSeededReverse {
			selectorVersion = fmt.Sprintf("inline-asp-kill-switch-g%d", s.Generation)
		}
	}
	options := translate.ProductionOptions{
		ShortestPathExecutor:         s.ShortestPathExecutor,
		EnableExpansionOrientation:   s.EnableExpansionOrientation && !s.DisableExpansionOrientation,
		DisableEndpointSeededReverse: s.DisableEndpointSeededReverse,
		DisableInlineASPDAG:          s.DisableInlineASPDAG,
		DisableInlineSPWitness:       s.DisableInlineSPWitness,
		DisableInlineSPDistance:      s.DisableInlineSPDistance,
		SelectorVersion:              selectorVersion,
	}
	if (s.EnableTopologyFixedSuffix && !s.DisableTopologyFixedSuffix) || (s.EnableTopologyFixedSuffixFirstUse && !s.DisableTopologyFixedSuffixFirstUse) {
		options.EnableTopologyFixedSuffix = true
		options.TopologyFixedSuffixCaps = &translate.ProductionFixedSuffixCaps{
			SuffixRowLimit:   manifest.Caps["suffix_row_limit"],
			StateLimit:       manifest.Caps["state_limit"],
			OutputRowLimit:   manifest.Caps["output_row_limit"],
			OutputBytesLimit: manifest.Caps["output_bytes_limit"],
		}
	}
	if options.EnableExpansionOrientation {
		options.ExpansionOrientationPolicy = optimize.ExpansionSearchPolicy(manifest.SelectorVersion)
	}
	if s.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG || s.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness || s.ShortestPathExecutor == optimize.ShortestPathExecutorI2GuardedDistance {
		options.ShortestPathCaps = &translate.ProductionShortestPathCaps{
			StateLimit:       manifest.Caps["state_limit"],
			FrontierLimit:    manifest.Caps["frontier_limit"],
			PredecessorLimit: manifest.Caps["predecessor_limit"],
			EnumerationLimit: manifest.Caps["enumeration_limit"],
			OutputBytesLimit: manifest.Caps["output_bytes_limit"],
		}
		queryDigest := TraversalPolicyQuerySHA256(query)
		if bucket, found := s.compiledBuckets[queryDigest]; found {
			options.AuthorizedBucket = &translate.ProductionTraversalBucket{
				Direction:             bucket.Direction,
				ObservationMode:       bucket.ObservationMode,
				MinimumDepth:          bucket.MinimumDepth,
				MaximumDepth:          bucket.MaximumDepth,
				RelationshipKindCount: bucket.RelationshipKindCount,
				UntypedRelationship:   bucket.UntypedRelationship,
			}
		} else if bucket, found := s.authorizedStructuralBucketForShape(shape); found {
			options.AuthorizedBucket = &translate.ProductionTraversalBucket{
				Direction:             bucket.Direction,
				ObservationMode:       bucket.ObservationMode,
				MinimumDepth:          bucket.MinimumDepth,
				MaximumDepth:          bucket.MaximumDepth,
				RelationshipKindCount: bucket.RelationshipKindCount,
				UntypedRelationship:   bucket.UntypedRelationship,
			}
		} else {
			for _, bucket := range manifest.Buckets {
				if !slices.Contains(bucket.QuerySHA256, queryDigest) {
					continue
				}
				options.AuthorizedBucket = &translate.ProductionTraversalBucket{
					Direction:             bucket.Direction,
					ObservationMode:       bucket.ObservationMode,
					MinimumDepth:          bucket.MinimumDepth,
					MaximumDepth:          bucket.MaximumDepth,
					RelationshipKindCount: bucket.RelationshipKindCount,
					UntypedRelationship:   bucket.UntypedRelationship,
				}
				break
			}
		}
	}
	return options, nil
}

// structuralBucketForShape reports an observation-only structural match. It
// does not authorize a candidate: manifest v2 remains exact-query gated until
// the structural evidence schema is independently verified.
func (s TraversalPolicy) structuralBucketForShape(shape TraversalShape) (traversalPromotionBucket, bool) {
	if !shape.Available() || len(s.compiledManifest.Buckets) == 0 {
		return traversalPromotionBucket{}, false
	}
	var matched *traversalPromotionBucket
	for index := range s.compiledManifest.Buckets {
		bucket := &s.compiledManifest.Buckets[index]
		if bucket.Direction != shape.Direction || bucket.ObservationMode != shape.ObservationMode ||
			bucket.MinimumDepth != shape.MinimumDepth || bucket.MaximumDepth != shape.MaximumDepth ||
			bucket.RelationshipKindCount != shape.RelationshipKindCount || bucket.UntypedRelationship != shape.UntypedRelationship ||
			bucket.SuffixLength != shape.SuffixLength || bucket.CandidateStrategy != shape.CandidateStrategy {
			continue
		}
		if matched != nil {
			return traversalPromotionBucket{}, false
		}
		matched = bucket
	}
	if matched == nil {
		return traversalPromotionBucket{}, false
	}
	return *matched, true
}

// authorizedStructuralBucketForShape reports a v3 or v4 manifest-backed
// structural authorization. Version 2 buckets intentionally remain
// observation-only: their SQL anchor binds one exact query, not a reusable SQL
// template. A v4 match authorizes only the route-decision candidate path; it
// does not make ordinary translation select that candidate.
func (s TraversalPolicy) authorizedStructuralBucketForShape(shape TraversalShape) (traversalPromotionBucket, bool) {
	if !shape.Available() || (s.compiledManifest.Version != 3 && s.compiledManifest.Version != 4 && s.compiledManifest.Version != 5) {
		return traversalPromotionBucket{}, false
	}
	var matched *traversalPromotionBucket
	for index := range s.compiledManifest.Buckets {
		bucket := &s.compiledManifest.Buckets[index]
		if bucket.StructuralShapeVersion != shape.Version || bucket.StructuralShapeSHA256 != shape.Fingerprint {
			continue
		}
		if matched != nil {
			return traversalPromotionBucket{}, false
		}
		matched = bucket
	}
	if matched == nil {
		return traversalPromotionBucket{}, false
	}
	return *matched, true
}

func structuralSQLTemplateSHA256(manifest traversalPromotionManifest, bucket traversalPromotionBucket) string {
	return TraversalSQLTemplateSHA256(manifest.Candidate, manifest.SelectorVersion, manifest.ExecutionBoundary, TraversalShape{
		Version:               bucket.StructuralShapeVersion,
		Family:                bucket.StructuralFamily,
		Direction:             bucket.Direction,
		ObservationMode:       bucket.ObservationMode,
		MinimumDepth:          bucket.MinimumDepth,
		MaximumDepth:          bucket.MaximumDepth,
		RelationshipKindCount: bucket.RelationshipKindCount,
		UntypedRelationship:   bucket.UntypedRelationship,
		SuffixLength:          bucket.SuffixLength,
		CandidateStrategy:     bucket.CandidateStrategy,
		Fingerprint:           bucket.StructuralShapeSHA256,
	})
}

// TraversalSQLTemplateSHA256 returns the public template-contract digest for
// a v3 or v4 structural bucket. It binds the candidate and every SQL-shaping
// static fact, but intentionally excludes Cypher identifiers and caller
// values.
func TraversalSQLTemplateSHA256(candidate, selectorVersion, executionBoundary string, shape TraversalShape) string {
	if shape.Version == TraversalFixedSuffixShapeVersion {
		canonical := fmt.Sprintf(
			"topology-sql-template-v1|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%d|%s",
			candidate, selectorVersion, executionBoundary,
			shape.Version, shape.Family, shape.Fingerprint,
			shape.Direction, shape.ObservationMode, shape.MinimumDepth, shape.MaximumDepth,
			shape.SuffixLength, shape.CandidateStrategy,
		)
		digest := sha256.Sum256([]byte(canonical))
		return hex.EncodeToString(digest[:])
	}
	canonical := fmt.Sprintf(
		"structural-sql-template-v1|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%d|%t",
		candidate, selectorVersion, executionBoundary,
		shape.Version, shape.Family, shape.Fingerprint,
		shape.Direction, shape.ObservationMode, shape.MinimumDepth,
		shape.MaximumDepth, shape.RelationshipKindCount, shape.UntypedRelationship,
	)
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:])
}

// traversalPromotionBucket groups state that must remain consistent while processing traversal promotion bucket.
type traversalPromotionBucket struct {
	// Name identifies the qualified workload bucket.
	Name string `json:"name,omitempty"`
	// QuerySHA256 binds the referenced query content by SHA-256 digest.
	QuerySHA256 []string `json:"query_sha256"`
	// QualificationSplit assigns the workload to training, holdout, or diagnostic evidence.
	QualificationSplit []string `json:"qualification_split"`
	// Direction selects the traversal orientation covered by the contract.
	Direction string `json:"direction,omitempty"`
	// ObservationMode identifies the observation mode.
	ObservationMode string `json:"observation_mode,omitempty"`
	// MinimumDepth sets the inclusive lower traversal-depth bound.
	MinimumDepth int64 `json:"minimum_depth,omitempty"`
	// MaximumDepth sets the inclusive upper traversal-depth bound.
	MaximumDepth int64 `json:"maximum_depth,omitempty"`
	// RelationshipKindCount records the number of relationship kind count.
	RelationshipKindCount int `json:"relationship_kind_count,omitempty"`
	// UntypedRelationship indicates whether untyped relationship applies.
	UntypedRelationship bool `json:"untyped_relationship,omitempty"`
	// SuffixLength binds the fixed terminal suffix width for a v4 bucket.
	SuffixLength int `json:"suffix_length,omitempty"`
	// CandidateStrategy binds the optimizer-provided fixed-suffix candidate.
	CandidateStrategy string `json:"candidate_strategy,omitempty"`
	// StructuralShapeVersion identifies the canonical structural classifier
	// used when this bucket authorizes production-wide selection.
	StructuralShapeVersion string `json:"structural_shape_version,omitempty"`
	// StructuralFamily identifies the shortest-path family bound by the
	// structural classifier.
	StructuralFamily string `json:"structural_family,omitempty"`
	// StructuralShapeSHA256 binds the classifier output without retaining the
	// source query text.
	StructuralShapeSHA256 string `json:"structural_shape_sha256,omitempty"`
	// SQLTemplateSHA256 binds the candidate SQL template contract for a
	// structural bucket. Unlike the v2 SQL anchor, it is independent of Cypher
	// identifiers and parameters.
	SQLTemplateSHA256 string `json:"sql_template_sha256,omitempty"`
}

// traversalPromotionEvidence records independently verifiable observations for traversal promotion.
type traversalPromotionEvidence struct {
	// Path identifies the evidence document relative to its manifest.
	Path string `json:"path,omitempty"`
	// SHA256 binds the referenced  content by SHA-256 digest.
	SHA256 string `json:"sha256"`
}

// traversalPromotionManifest binds the immutable inputs authorized for traversal promotion.
type traversalPromotionManifest struct {
	// Version identifies the schema version for version.
	Version int `json:"version"`
	// Candidate identifies the execution strategy being evaluated or authorized.
	Candidate string `json:"candidate"`
	// SelectorVersion identifies the schema version for selector version.
	SelectorVersion string `json:"selector_version"`
	// ExecutionBoundary supplies the execution boundary input to the traversalPromotionManifest contract.
	ExecutionBoundary string `json:"execution_boundary"`
	// FallbackExecutor supplies the fallback executor input to the traversalPromotionManifest contract.
	FallbackExecutor string `json:"fallback_executor,omitempty"`
	// SourceCommit supplies the source commit input to the traversalPromotionManifest contract.
	SourceCommit string `json:"source_commit"`
	// SourceSHA256 binds the referenced source content by SHA-256 digest.
	SourceSHA256 string `json:"source_sha256"`
	// BinarySHA256 binds the referenced binary content by SHA-256 digest.
	BinarySHA256 string `json:"binary_sha256"`
	// CorpusSHA256 binds the referenced corpus content by SHA-256 digest.
	CorpusSHA256 string `json:"corpus_sha256"`
	// OperationalCandidateSQLSHA256 independently freezes the exact SQL used
	// by the operational candidate matrix.
	OperationalCandidateSQLSHA256 string `json:"operational_candidate_sql_sha256"`
	// TopologyEstimatorVersion binds the frozen synopsis estimator for v4.
	TopologyEstimatorVersion string `json:"topology_estimator_version,omitempty"`
	// SynopsisSchemaVersion binds the published synopsis schema required by v4.
	SynopsisSchemaVersion string `json:"synopsis_schema_version,omitempty"`
	// RouteCacheProtocol binds the transaction-local v4 route-cache contract.
	RouteCacheProtocol string `json:"route_cache_protocol,omitempty"`
	// TopologyThresholds bind the immutable topology estimator thresholds for
	// a manifest-v4 candidate.
	TopologyThresholds map[string]int64 `json:"topology_thresholds,omitempty"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps map[string]int64 `json:"caps"`
	// Buckets supplies the buckets input to the traversalPromotionManifest contract.
	Buckets []traversalPromotionBucket `json:"buckets"`
	// Evidence supplies the evidence input to the traversalPromotionManifest contract.
	Evidence map[string]traversalPromotionEvidence `json:"evidence"`
}

// decodeTraversalPromotionManifest coordinates PostgreSQL driver behavior for decode traversal promotion manifest.
func decodeTraversalPromotionManifest(raw []byte) (traversalPromotionManifest, error) {
	var manifest traversalPromotionManifest
	if len(raw) == 0 {
		return manifest, fmt.Errorf("enabled traversal policy requires the verified promotion manifest JSON")
	}
	if err := rejectDuplicateTraversalManifestKeys(raw); err != nil {
		return manifest, fmt.Errorf("decode promotion manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, fmt.Errorf("decode promotion manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return manifest, fmt.Errorf("decode promotion manifest: trailing JSON data")
		}
		return manifest, fmt.Errorf("decode promotion manifest trailing data: %w", err)
	}
	return manifest, nil
}

// rejectDuplicateTraversalManifestKeys rejects duplicate object keys at every
// depth. The standard decoder otherwise accepts the final duplicate value,
// which makes authorization documents ambiguous across implementations.
func rejectDuplicateTraversalManifestKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := rejectDuplicateTraversalJSONValue(decoder); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON data after %v", token)
		}
		return err
	}
	return nil
}

func rejectDuplicateTraversalJSONValue(decoder *json.Decoder) error {
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
			if err := rejectDuplicateTraversalJSONValue(decoder); err != nil {
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
			if err := rejectDuplicateTraversalJSONValue(decoder); err != nil {
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

// validate validates .
func (s TraversalPolicy) validate() error {
	if !s.enabled() {
		return nil
	}
	if s.Generation == 0 {
		return fmt.Errorf("enabled traversal policy requires a nonzero generation")
	}
	candidateFamilies := 0
	for _, enabled := range []bool{s.ShortestPathExecutor != "", s.EnableExpansionOrientation, s.EnableTopologyFixedSuffix, s.EnableTopologyFixedSuffixFirstUse} {
		if enabled {
			candidateFamilies++
		}
	}
	if candidateFamilies > 1 {
		return fmt.Errorf("one traversal policy generation may enable only one candidate family")
	}
	if s.manifestCandidateEnabled() && s.rollbackActive() && !s.matchingCandidateRollbackActive() {
		return fmt.Errorf("a manifest-backed traversal candidate may be combined only with its single matching emergency rollback switch")
	}
	if !s.manifestCandidateEnabled() && s.rollbackActive() {
		if s.PromotionManifestSHA256 != "" || len(s.PromotionManifestJSON) != 0 || len(s.QuerySHA256Allowlist) != 0 {
			return fmt.Errorf("a standalone traversal rollback policy must not carry promotion manifest or query authorization fields")
		}
		return nil
	}
	if !lowerHexSHA256(s.PromotionManifestSHA256) {
		return fmt.Errorf("enabled traversal policy requires a lowercase promotion manifest SHA-256 digest")
	}
	manifest, err := decodeTraversalPromotionManifest(s.PromotionManifestJSON)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(s.PromotionManifestJSON)
	if hex.EncodeToString(digest[:]) != s.PromotionManifestSHA256 {
		return fmt.Errorf("promotion manifest content does not match its SHA-256 digest")
	}
	if (manifest.Version != 2 && manifest.Version != 3 && manifest.Version != 4 && manifest.Version != 5) || strings.TrimSpace(manifest.SelectorVersion) == "" {
		return fmt.Errorf("promotion manifest requires version 2, 3, 4, or 5 and a selector version")
	}
	if strings.TrimSpace(manifest.SourceCommit) == "" || !lowerHexSHA256(manifest.SourceSHA256) || !lowerHexSHA256(manifest.BinarySHA256) || !lowerHexSHA256(manifest.CorpusSHA256) {
		return fmt.Errorf("promotion manifest requires source commit and lowercase source, binary, and corpus SHA-256 digests")
	}
	if !lowerHexSHA256(manifest.OperationalCandidateSQLSHA256) {
		return fmt.Errorf("promotion manifest requires a lowercase operational candidate SQL SHA-256 digest")
	}
	expectedCandidate := string(s.ShortestPathExecutor)
	if s.EnableExpansionOrientation {
		policy := optimize.ExpansionSearchPolicy(manifest.SelectorVersion)
		if policy != optimize.ExpansionSearchPolicyOrientationProbeV1 && policy != optimize.ExpansionSearchPolicyOrientationProbeV2 {
			return fmt.Errorf("unsupported production orientation selector %q", manifest.SelectorVersion)
		}
		expectedCandidate = string(policy)
	}
	if s.EnableTopologyFixedSuffix {
		expectedCandidate = string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1)
	}
	if s.EnableTopologyFixedSuffixFirstUse {
		expectedCandidate = string(optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1)
	}
	if manifest.Candidate != expectedCandidate {
		return fmt.Errorf("promotion manifest candidate %q does not authorize %q", manifest.Candidate, expectedCandidate)
	}
	expectedBoundary := "inline_statement"
	if s.EnableExpansionOrientation {
		expectedBoundary = "guarded_dual_arm"
	} else if s.EnableTopologyFixedSuffix {
		expectedBoundary = "transaction_retry"
	} else if s.EnableTopologyFixedSuffixFirstUse {
		expectedBoundary = "first_use_transaction_retry"
	} else if s.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG || s.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness || s.ShortestPathExecutor == optimize.ShortestPathExecutorI2GuardedDistance {
		expectedBoundary = "guarded_dual_arm"
	}
	if manifest.ExecutionBoundary != expectedBoundary {
		return fmt.Errorf("promotion manifest execution boundary %q does not authorize %q", manifest.ExecutionBoundary, expectedBoundary)
	}
	if len(manifest.Caps) == 0 || len(manifest.Buckets) == 0 {
		return fmt.Errorf("promotion manifest requires immutable caps and authorized buckets")
	}
	if s.EnableExpansionOrientation {
		expectedCaps := map[string]int64{
			"root_row_limit":               optimize.ExpansionSearchOrientationRootRowLimit,
			"reverse_seed_row_limit":       optimize.ExpansionSearchOrientationReverseSeedRowLimit,
			"directional_degree_row_limit": optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit,
			"state_limit":                  optimize.ExpansionSearchOrientationStateLimit,
		}
		if len(manifest.Caps) != len(expectedCaps) {
			return fmt.Errorf("%s promotion manifest requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps", manifest.SelectorVersion)
		}
		for name, expected := range expectedCaps {
			if actual, found := manifest.Caps[name]; !found || actual != expected {
				return fmt.Errorf("%s promotion manifest requires %s=%d", manifest.SelectorVersion, name, expected)
			}
		}
		if manifest.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) {
			return fmt.Errorf("%s promotion manifest requires fallback %q", manifest.SelectorVersion, optimize.ExpansionSearchStepwiseForward)
		}
	}
	if s.EnableTopologyFixedSuffix || s.EnableTopologyFixedSuffixFirstUse {
		expectedCaps := map[string]int64{
			"suffix_row_limit":   optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit,
			"state_limit":        optimize.ExpansionSearchSuffixReverseGuardStateLimit,
			"output_row_limit":   optimize.ExpansionSearchSuffixReverseRetryOutputRowLimit,
			"output_bytes_limit": optimize.ExpansionSearchSuffixReverseRetryOutputBytesLimit,
		}
		if len(manifest.Caps) != len(expectedCaps) {
			return fmt.Errorf("topology fixed-suffix promotion manifest requires exactly suffix, state, output-row, and output-byte caps")
		}
		for name, expected := range expectedCaps {
			if actual, found := manifest.Caps[name]; !found || actual != expected {
				return fmt.Errorf("topology fixed-suffix promotion manifest requires %s=%d", name, expected)
			}
		}
		expectedVersion, expectedSelector, expectedProtocol := 4, string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1), "topology-selected-routing-v1"
		if s.EnableTopologyFixedSuffixFirstUse {
			expectedVersion, expectedSelector, expectedProtocol = 5, string(optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1), "topology-selected-first-use-routing-v1"
		}
		if manifest.Version != expectedVersion || manifest.SelectorVersion != expectedSelector || manifest.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) || manifest.TopologyEstimatorVersion != "topology-fixed-suffix-counts-v1" || manifest.SynopsisSchemaVersion != "topology-synopsis-schema-v2" || manifest.RouteCacheProtocol != expectedProtocol {
			return fmt.Errorf("topology fixed-suffix promotion manifest requires versioned selector, fallback, estimator, synopsis schema, and route-cache protocol bindings")
		}
		if !slices.EqualFunc(sortedTopologyThresholds(manifest.TopologyThresholds), []topologyThreshold{{Name: "maximum_edge_to_node_ratio_per_mille", Value: 1000}}, func(left, right topologyThreshold) bool {
			return left == right
		}) {
			return fmt.Errorf("topology fixed-suffix promotion manifest requires maximum_edge_to_node_ratio_per_mille=1000")
		}
	}
	if s.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG {
		expectedCaps := map[string]struct{}{
			"state_limit": {}, "predecessor_limit": {}, "enumeration_limit": {}, "output_bytes_limit": {},
		}
		if len(manifest.Caps) != len(expectedCaps) {
			return fmt.Errorf("ASP-I1 promotion manifest requires exactly state, predecessor, enumeration, and output-byte caps")
		}
		for name := range expectedCaps {
			if manifest.Caps[name] <= 0 {
				return fmt.Errorf("ASP-I1 promotion manifest requires positive %s", name)
			}
		}
		if manifest.FallbackExecutor != string(optimize.ShortestPathExecutorASPA1DAG) {
			return fmt.Errorf("ASP-I1 promotion manifest requires fallback %q", optimize.ShortestPathExecutorASPA1DAG)
		}
		for _, bucket := range manifest.Buckets {
			if (bucket.Direction != "outbound" && bucket.Direction != "inbound") || bucket.ObservationMode != "all_paths" || bucket.MinimumDepth != 1 || bucket.MaximumDepth < 1 || bucket.MaximumDepth > 64 || bucket.RelationshipKindCount < 0 {
				return fmt.Errorf("ASP-I1 promotion bucket does not match the supported directed all-paths depth envelope")
			}
			if bucket.UntypedRelationship != (bucket.RelationshipKindCount == 0) {
				return fmt.Errorf("ASP-I1 promotion bucket relationship kind metadata is inconsistent")
			}
		}
	}
	if s.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
		expectedCaps := map[string]struct{}{
			"state_limit": {}, "predecessor_limit": {}, "enumeration_limit": {}, "output_bytes_limit": {},
		}
		if len(manifest.Caps) != len(expectedCaps) {
			return fmt.Errorf("SP-I1 canonical promotion manifest requires exactly state, predecessor, enumeration, and output-byte caps")
		}
		for name := range expectedCaps {
			if manifest.Caps[name] <= 0 {
				return fmt.Errorf("SP-I1 canonical promotion manifest requires positive %s", name)
			}
		}
		if manifest.FallbackExecutor != string(optimize.ShortestPathExecutorS4CanonicalWitness) {
			return fmt.Errorf("SP-I1 canonical promotion manifest requires fallback %q", optimize.ShortestPathExecutorS4CanonicalWitness)
		}
		if manifest.SelectorVersion != optimize.ShortestPathSelectorStaticV6 {
			return fmt.Errorf("SP-I1 canonical promotion manifest requires selector %q", optimize.ShortestPathSelectorStaticV6)
		}
		for _, bucket := range manifest.Buckets {
			if bucket.Direction != "inbound" || bucket.ObservationMode != string(optimize.ShortestPathObservationOnePath) ||
				bucket.MinimumDepth != 1 || bucket.MaximumDepth != 64 || bucket.RelationshipKindCount != 1 || bucket.UntypedRelationship {
				return fmt.Errorf("SP-I1 canonical promotion bucket must match the qualified inbound typed single-kind one-path depth 1..64 envelope")
			}
		}
	}
	if s.ShortestPathExecutor == optimize.ShortestPathExecutorI2GuardedDistance {
		expectedCaps := map[string]int64{
			"state_limit":    optimize.ShortestPathI2QualifiedStateLimit,
			"frontier_limit": optimize.ShortestPathI2QualifiedFrontierLimit,
		}
		if len(manifest.Caps) != len(expectedCaps) {
			return fmt.Errorf("SP-I2 distance promotion manifest requires exactly state and frontier caps")
		}
		for name, expected := range expectedCaps {
			if actual, found := manifest.Caps[name]; !found || actual != expected {
				return fmt.Errorf("SP-I2 distance promotion manifest requires %s=%d", name, expected)
			}
		}
		if manifest.FallbackExecutor != string(optimize.ShortestPathExecutorS4CanonicalDistance) {
			return fmt.Errorf("SP-I2 distance promotion manifest requires fallback %q", optimize.ShortestPathExecutorS4CanonicalDistance)
		}
		if manifest.SelectorVersion != optimize.ShortestPathSelectorStaticV8HiddenFanIn {
			return fmt.Errorf("SP-I2 distance promotion manifest requires selector %q", optimize.ShortestPathSelectorStaticV8HiddenFanIn)
		}
		for _, bucket := range manifest.Buckets {
			if bucket.Direction != "inbound" || bucket.ObservationMode != string(optimize.ShortestPathObservationDistance) || bucket.MinimumDepth != 1 || bucket.MaximumDepth < 1 || bucket.MaximumDepth > 64 || bucket.RelationshipKindCount != 1 || bucket.UntypedRelationship {
				return fmt.Errorf("SP-I2 distance promotion bucket must be inbound, typed single-kind, distance-only, and depth-bounded")
			}
		}
	}
	manifestQueries := make([]string, 0)
	seenBucketNames := map[string]struct{}{}
	seenManifestQueries := map[string]string{}
	for _, bucket := range manifest.Buckets {
		if strings.TrimSpace(bucket.Name) == "" {
			return fmt.Errorf("each promotion bucket requires a nonempty unique name")
		}
		if _, duplicate := seenBucketNames[bucket.Name]; duplicate {
			return fmt.Errorf("promotion bucket %q is duplicated", bucket.Name)
		}
		seenBucketNames[bucket.Name] = struct{}{}
		if !slices.Equal(bucket.QualificationSplit, []string{"training", "holdout"}) {
			return fmt.Errorf("each promotion bucket requires exactly one training and one holdout qualification split in canonical order")
		}
		if manifest.Version == 3 || manifest.Version == 4 {
			shape := TraversalShape{
				Version:               bucket.StructuralShapeVersion,
				Family:                bucket.StructuralFamily,
				Direction:             bucket.Direction,
				ObservationMode:       bucket.ObservationMode,
				MinimumDepth:          bucket.MinimumDepth,
				MaximumDepth:          bucket.MaximumDepth,
				RelationshipKindCount: bucket.RelationshipKindCount,
				UntypedRelationship:   bucket.UntypedRelationship,
				SuffixLength:          bucket.SuffixLength,
				CandidateStrategy:     bucket.CandidateStrategy,
			}
			expectedShapeVersion := TraversalShapeVersion
			if manifest.Version == 4 {
				expectedShapeVersion = TraversalFixedSuffixShapeVersion
			}
			if shape.Version != expectedShapeVersion || !lowerHexSHA256(bucket.StructuralShapeSHA256) || bucket.StructuralShapeSHA256 != TraversalShapeFingerprint(shape) {
				return fmt.Errorf("promotion bucket %q has an invalid structural shape binding", bucket.Name)
			}
			if !lowerHexSHA256(bucket.SQLTemplateSHA256) || bucket.SQLTemplateSHA256 != structuralSQLTemplateSHA256(manifest, bucket) {
				return fmt.Errorf("promotion bucket %q has an invalid structural SQL template binding", bucket.Name)
			}
			if manifest.Version == 4 && (bucket.Direction != "outbound" || bucket.ObservationMode != string(optimize.ExpansionSearchObservationFullPath) || bucket.MinimumDepth != 0 || bucket.MaximumDepth != 16 || bucket.SuffixLength != 3 || bucket.CandidateStrategy != string(optimize.ExpansionSearchSuffixSeededReverse)) {
				return fmt.Errorf("topology fixed-suffix promotion bucket %q does not match the qualified outbound full-path fixed-suffix envelope", bucket.Name)
			}
		}
		if len(bucket.QuerySHA256) == 0 {
			return fmt.Errorf("promotion bucket %q requires a nonempty query allowlist", bucket.Name)
		}
		seenWithinBucket := map[string]struct{}{}
		for _, query := range bucket.QuerySHA256 {
			if !lowerHexSHA256(query) {
				return fmt.Errorf("promotion bucket %q contains invalid query digest %q", bucket.Name, query)
			}
			if _, duplicate := seenWithinBucket[query]; duplicate {
				return fmt.Errorf("promotion bucket %q duplicates query digest %q", bucket.Name, query)
			}
			seenWithinBucket[query] = struct{}{}
			if owner, duplicate := seenManifestQueries[query]; duplicate {
				return fmt.Errorf("promotion manifest query %q is authorized by both bucket %q and bucket %q", query, owner, bucket.Name)
			}
			seenManifestQueries[query] = bucket.Name
		}
		manifestQueries = append(manifestQueries, bucket.QuerySHA256...)
	}
	sort.Strings(manifestQueries)
	manifestQueries = slices.Compact(manifestQueries)
	if manifest.Version == 2 && len(manifestQueries) != 1 {
		return fmt.Errorf("promotion manifest operational SQL anchor requires exactly one authorized query digest")
	}
	policyQueries := append([]string(nil), s.QuerySHA256Allowlist...)
	sort.Strings(policyQueries)
	compactedPolicyQueries := slices.Compact(policyQueries)
	if len(compactedPolicyQueries) != len(s.QuerySHA256Allowlist) {
		return fmt.Errorf("query allowlist must not contain duplicate digests")
	}
	policyQueries = compactedPolicyQueries
	if !slices.Equal(manifestQueries, policyQueries) {
		return fmt.Errorf("query allowlist must exactly match the promotion manifest buckets")
	}
	requiredEvidenceRoles := []string{"aa", "confirmation", "performance", "resource", "reference_closure", "operational"}
	if len(manifest.Evidence) != len(requiredEvidenceRoles) {
		return fmt.Errorf("promotion manifest requires exactly the six supported evidence roles")
	}
	for _, role := range requiredEvidenceRoles {
		evidence, found := manifest.Evidence[role]
		if !found || !lowerHexSHA256(evidence.SHA256) {
			return fmt.Errorf("promotion manifest requires digest-bound %s evidence", role)
		}
		clean := filepath.Clean(evidence.Path)
		if evidence.Path == "" || filepath.IsAbs(evidence.Path) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("promotion manifest %s evidence requires a contained relative path", role)
		}
	}
	if len(s.QuerySHA256Allowlist) == 0 {
		return fmt.Errorf("enabled traversal policy requires a nonempty query SHA-256 allowlist")
	}
	if s.ShortestPathExecutor != "" && !productionCanaryExecutor(s.ShortestPathExecutor) {
		return fmt.Errorf("shortest-path executor %q is not production-canary eligible", s.ShortestPathExecutor)
	}
	for _, value := range s.QuerySHA256Allowlist {
		if !lowerHexSHA256(value) {
			return fmt.Errorf("query allowlist entry %q is not a SHA-256 digest", value)
		}
	}
	return nil
}

type topologyThreshold struct {
	Name  string
	Value int64
}

func sortedTopologyThresholds(input map[string]int64) []topologyThreshold {
	thresholds := make([]topologyThreshold, 0, len(input))
	for name, value := range input {
		thresholds = append(thresholds, topologyThreshold{Name: name, Value: value})
	}
	slices.SortFunc(thresholds, func(left, right topologyThreshold) int {
		return strings.Compare(left.Name, right.Name)
	})
	return thresholds
}

// lowerHexSHA256 coordinates PostgreSQL driver behavior for lower hex sha256.
func lowerHexSHA256(value string) bool {
	if value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

// validateTraversalPromotionSQLAnchor binds the SQL rendered on a production
// cache miss to the independent manifest identity before it can be executed or
// cached. The exact formatter output is hashed without normalization, matching
// GraphBench's sql_fingerprint contract.
func validateTraversalPromotionSQLAnchor(manifest traversalPromotionManifest, sqlQuery string) error {
	// Emergency rollback policies intentionally carry no promotion manifest.
	// Active candidates cannot reach this helper with an empty anchor because
	// TraversalPolicy.validate requires one before installation.
	if manifest.OperationalCandidateSQLSHA256 == "" {
		return nil
	}
	digest := sha256.Sum256([]byte(sqlQuery))
	actual := hex.EncodeToString(digest[:])
	if actual != manifest.OperationalCandidateSQLSHA256 {
		return fmt.Errorf(
			"production traversal SQL SHA-256 %s does not match promotion manifest anchor %s",
			actual,
			manifest.OperationalCandidateSQLSHA256,
		)
	}
	return nil
}

// productionCanaryExecutor coordinates PostgreSQL driver behavior for production canary executor.
func productionCanaryExecutor(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		optimize.ShortestPathExecutorASPI1DAG,
		optimize.ShortestPathExecutorI2GuardedDistance:
		return true
	default:
		return false
	}
}

// TraversalPolicyQuerySHA256 returns the stable digest used by policy
// allowlists. Only surrounding whitespace is normalized. Collapsing interior
// whitespace is unsafe because whitespace inside string literals and escaped
// identifiers is semantically significant.
func TraversalPolicyQuerySHA256(query string) string {
	normalized := strings.TrimSpace(query)
	digest := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(digest[:])
}

// SetTraversalPolicy atomically replaces production canary selection. The
// zero value disables all candidates; old cached SQL becomes unreachable
// because the effective policy identity changes immediately.
func (s *Driver) SetTraversalPolicy(policy TraversalPolicy) error {
	if s == nil || s.SchemaManager == nil {
		return fmt.Errorf("PostgreSQL driver is not initialized")
	}
	if err := policy.validate(); err != nil {
		return err
	}
	policy.QuerySHA256Allowlist = append([]string(nil), policy.QuerySHA256Allowlist...)
	policy.PromotionManifestJSON = append(json.RawMessage(nil), policy.PromotionManifestJSON...)
	sort.Strings(policy.QuerySHA256Allowlist)
	policy.QuerySHA256Allowlist = slices.Compact(policy.QuerySHA256Allowlist)
	policy.compiledBuckets = map[string]traversalPromotionBucket{}
	if len(policy.PromotionManifestJSON) > 0 {
		manifest, err := decodeTraversalPromotionManifest(policy.PromotionManifestJSON)
		if err != nil {
			return err
		}
		policy.compiledManifest = manifest
		for _, bucket := range manifest.Buckets {
			for _, queryDigest := range bucket.QuerySHA256 {
				if _, duplicate := policy.compiledBuckets[queryDigest]; duplicate {
					return fmt.Errorf("promotion manifest query %q is authorized by more than one bucket", queryDigest)
				}
				policy.compiledBuckets[queryDigest] = bucket
			}
		}
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("serialize traversal policy identity: %w", err)
	}
	digest := sha256.Sum256(raw)
	policy.compiledIdentity = "production-policy-" + hex.EncodeToString(digest[:])
	s.traversalPolicyLock.Lock()
	s.traversalPolicy = policy
	s.traversalPolicyLock.Unlock()
	return nil
}

// TraversalPolicy returns an immutable snapshot of the active policy.
func (s *Driver) TraversalPolicy() TraversalPolicy {
	if s == nil || s.SchemaManager == nil {
		return TraversalPolicy{}
	}
	s.traversalPolicyLock.RLock()
	defer s.traversalPolicyLock.RUnlock()
	policy := s.traversalPolicy
	policy.QuerySHA256Allowlist = append([]string(nil), policy.QuerySHA256Allowlist...)
	policy.PromotionManifestJSON = append(json.RawMessage(nil), policy.PromotionManifestJSON...)
	return policy
}

// effectiveTraversalPolicy coordinates PostgreSQL driver behavior for effective traversal policy.
func (s *SchemaManager) effectiveTraversalPolicy(query string, isolation pgx.TxIsoLevel) (TraversalPolicy, string) {
	return s.effectiveTraversalPolicyForShape(query, TraversalShape{}, isolation)
}

// effectiveTraversalPolicyForShape returns the active policy for an exact
// canary query or a v3 structurally authorized query. A zero shape preserves
// the historic exact-query behavior.
func (s *SchemaManager) effectiveTraversalPolicyForShape(query string, shape TraversalShape, isolation pgx.TxIsoLevel) (TraversalPolicy, string) {
	s.traversalPolicyLock.RLock()
	policy := s.traversalPolicy
	s.traversalPolicyLock.RUnlock()
	candidateRollback := policy.matchingCandidateRollbackActive()
	standaloneRollback := !policy.manifestCandidateEnabled() && policy.rollbackActive()
	if candidateRollback {
		policy = policy.withoutManifestCandidate()
	}
	if candidateRollback || standaloneRollback {
		policy.compiledManifest.OperationalCandidateSQLSHA256 = ""
	}
	// Manifest v4 has a separate, snapshot-owned selection path. An exact
	// evidence query must not make it eligible through the ordinary translation
	// cache path, otherwise a route-cache miss could silently execute it.
	if policy.EnableTopologyFixedSuffix || policy.EnableTopologyFixedSuffixFirstUse || (candidateRollback && (policy.compiledManifest.Version == 4 || policy.compiledManifest.Version == 5)) {
		if candidateRollback {
			return policy, policy.compiledIdentity
		}
		return TraversalPolicy{}, "production-incumbent-v1"
	}

	_, queryAuthorized := policy.compiledBuckets[TraversalPolicyQuerySHA256(query)]
	_, structuralAuthorized := policy.authorizedStructuralBucketForShape(shape)
	effective := policy.enabled() && (candidateRollback || standaloneRollback || queryAuthorized || structuralAuthorized)
	if shortestPathExecutorRequiresStableSnapshot(policy.ShortestPathExecutor) && isolation != pgx.RepeatableRead && isolation != pgx.Serializable {
		effective = false
	}
	if !effective {
		return TraversalPolicy{}, "production-incumbent-v1"
	}
	return policy, policy.compiledIdentity
}

func (s *SchemaManager) hasStructuralTraversalPolicy() bool {
	s.traversalPolicyLock.RLock()
	defer s.traversalPolicyLock.RUnlock()
	return s.traversalPolicy.manifestCandidateEnabled() && (s.traversalPolicy.compiledManifest.Version == 3 || s.traversalPolicy.compiledManifest.Version == 4 || s.traversalPolicy.compiledManifest.Version == 5) && !s.traversalPolicy.rollbackActive()
}

// topologyFixedSuffixPolicyForShape returns a validated v4 or v5 policy only for
// the narrow structural bucket it authorizes. The caller must still own a
// stable transaction snapshot and receive a route-cache hit before it can
// execute the returned candidate.
func (s *SchemaManager) topologyFixedSuffixPolicyForShape(shape TraversalShape, isolation pgx.TxIsoLevel) (TraversalPolicy, string) {
	if shape.Version != TraversalFixedSuffixShapeVersion || !stableSnapshotIsolation(isolation) {
		return TraversalPolicy{}, ""
	}
	s.traversalPolicyLock.RLock()
	policy := s.traversalPolicy
	s.traversalPolicyLock.RUnlock()
	if (!policy.EnableTopologyFixedSuffix && !policy.EnableTopologyFixedSuffixFirstUse) || policy.rollbackActive() || (policy.compiledManifest.Version != 4 && policy.compiledManifest.Version != 5) {
		return TraversalPolicy{}, ""
	}
	if _, authorized := policy.authorizedStructuralBucketForShape(shape); !authorized {
		return TraversalPolicy{}, ""
	}
	if policy.EnableTopologyFixedSuffixFirstUse {
		return policy, policy.compiledIdentity + "-topology-fixed-suffix-first-use-candidate"
	}
	return policy, policy.compiledIdentity + "-topology-fixed-suffix-candidate"
}

// shortestPathExecutorRequiresStableSnapshot coordinates PostgreSQL driver behavior for shortest path executor requires stable snapshot.
func shortestPathExecutorRequiresStableSnapshot(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorB1AlternatingNodeDistance,
		optimize.ShortestPathExecutorB1AlternatingNodeWitness,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness,
		optimize.ShortestPathExecutorASPB1AlternatingNodeDAG,
		optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG,
		optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		optimize.ShortestPathExecutorASPI1DAG,
		optimize.ShortestPathExecutorI2GuardedDistance:
		return true
	default:
		return false
	}
}
