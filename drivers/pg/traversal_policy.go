package pg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	Generation              uint64 `json:"generation"`
	PromotionManifestSHA256 string `json:"promotion_manifest_sha256"`
	// PromotionManifestJSON is the exact verified authorization document. It
	// is intentionally excluded from policy serialization; its digest and
	// content-derived fields form the cache identity.
	PromotionManifestJSON        json.RawMessage               `json:"-"`
	QuerySHA256Allowlist         []string                      `json:"query_sha256_allowlist"`
	ShortestPathExecutor         optimize.ShortestPathExecutor `json:"shortest_path_executor,omitempty"`
	EnableExpansionOrientation   bool                          `json:"enable_expansion_orientation,omitempty"`
	DisableEndpointSeededReverse bool                          `json:"disable_endpoint_seeded_reverse,omitempty"`
	DisableInlineASPDAG          bool                          `json:"disable_inline_asp_dag,omitempty"`
	DisableInlineSPWitness       bool                          `json:"disable_inline_sp_witness,omitempty"`
	compiledManifest             traversalPromotionManifest
	compiledBuckets              map[string]traversalPromotionBucket
	compiledIdentity             string
}

func (s TraversalPolicy) enabled() bool {
	return s.ShortestPathExecutor != "" || s.EnableExpansionOrientation || s.DisableEndpointSeededReverse || s.DisableInlineASPDAG || s.DisableInlineSPWitness
}

func (s TraversalPolicy) productionOptions(query string) translate.ProductionOptions {
	manifest := s.compiledManifest
	if manifest.SelectorVersion == "" && len(s.PromotionManifestJSON) > 0 {
		manifest, _ = decodeTraversalPromotionManifest(s.PromotionManifestJSON)
	}
	selectorVersion := manifest.SelectorVersion
	if selectorVersion == "" {
		selectorVersion = fmt.Sprintf("traversal-kill-switch-g%d", s.Generation)
		if s.DisableEndpointSeededReverse && !s.DisableInlineASPDAG {
			selectorVersion = fmt.Sprintf("endpoint-seeded-kill-switch-g%d", s.Generation)
		} else if s.DisableInlineASPDAG && !s.DisableEndpointSeededReverse {
			selectorVersion = fmt.Sprintf("inline-asp-kill-switch-g%d", s.Generation)
		}
	}
	options := translate.ProductionOptions{
		ShortestPathExecutor: s.ShortestPathExecutor, EnableExpansionOrientation: s.EnableExpansionOrientation,
		DisableEndpointSeededReverse: s.DisableEndpointSeededReverse,
		DisableInlineASPDAG:          s.DisableInlineASPDAG,
		DisableInlineSPWitness:       s.DisableInlineSPWitness,
		SelectorVersion:              selectorVersion,
	}
	if s.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG || s.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
		options.ShortestPathCaps = &translate.ProductionShortestPathCaps{
			StateLimit:       manifest.Caps["state_limit"],
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
		} else {
			for _, bucket := range manifest.Buckets {
				if !slices.Contains(bucket.QuerySHA256, queryDigest) {
					continue
				}
				options.AuthorizedBucket = &translate.ProductionTraversalBucket{
					Direction: bucket.Direction, ObservationMode: bucket.ObservationMode,
					MinimumDepth: bucket.MinimumDepth, MaximumDepth: bucket.MaximumDepth,
					RelationshipKindCount: bucket.RelationshipKindCount, UntypedRelationship: bucket.UntypedRelationship,
				}
				break
			}
		}
	}
	return options
}

type traversalPromotionBucket struct {
	QuerySHA256           []string `json:"query_sha256"`
	QualificationSplit    []string `json:"qualification_split"`
	Direction             string   `json:"direction,omitempty"`
	ObservationMode       string   `json:"observation_mode,omitempty"`
	MinimumDepth          int64    `json:"minimum_depth,omitempty"`
	MaximumDepth          int64    `json:"maximum_depth,omitempty"`
	RelationshipKindCount int      `json:"relationship_kind_count,omitempty"`
	UntypedRelationship   bool     `json:"untyped_relationship,omitempty"`
}

type traversalPromotionEvidence struct {
	SHA256 string `json:"sha256"`
}

type traversalPromotionManifest struct {
	Version           int                                   `json:"version"`
	Candidate         string                                `json:"candidate"`
	SelectorVersion   string                                `json:"selector_version"`
	ExecutionBoundary string                                `json:"execution_boundary"`
	FallbackExecutor  string                                `json:"fallback_executor,omitempty"`
	SourceCommit      string                                `json:"source_commit"`
	SourceSHA256      string                                `json:"source_sha256"`
	BinarySHA256      string                                `json:"binary_sha256"`
	CorpusSHA256      string                                `json:"corpus_sha256"`
	Caps              map[string]int64                      `json:"caps"`
	Buckets           []traversalPromotionBucket            `json:"buckets"`
	Evidence          map[string]traversalPromotionEvidence `json:"evidence"`
}

func decodeTraversalPromotionManifest(raw []byte) (traversalPromotionManifest, error) {
	var manifest traversalPromotionManifest
	if len(raw) == 0 {
		return manifest, fmt.Errorf("enabled traversal policy requires the verified promotion manifest JSON")
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return manifest, fmt.Errorf("decode promotion manifest: %w", err)
	}
	return manifest, nil
}

func (s TraversalPolicy) validate() error {
	if !s.enabled() {
		return nil
	}
	if s.Generation == 0 {
		return fmt.Errorf("enabled traversal policy requires a nonzero generation")
	}
	if s.ShortestPathExecutor == "" && !s.EnableExpansionOrientation && (s.DisableEndpointSeededReverse || s.DisableInlineASPDAG || s.DisableInlineSPWitness) {
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
	if manifest.Version != 2 || strings.TrimSpace(manifest.SelectorVersion) == "" {
		return fmt.Errorf("promotion manifest requires version 2 and a selector version")
	}
	if strings.TrimSpace(manifest.SourceCommit) == "" || !lowerHexSHA256(manifest.SourceSHA256) || !lowerHexSHA256(manifest.BinarySHA256) || !lowerHexSHA256(manifest.CorpusSHA256) {
		return fmt.Errorf("promotion manifest requires source commit and lowercase source, binary, and corpus SHA-256 digests")
	}
	expectedCandidate := string(s.ShortestPathExecutor)
	if s.EnableExpansionOrientation {
		expectedCandidate = "orientation-probe-v1"
	}
	if manifest.Candidate != expectedCandidate {
		return fmt.Errorf("promotion manifest candidate %q does not authorize %q", manifest.Candidate, expectedCandidate)
	}
	expectedBoundary := "inline_statement"
	if s.EnableExpansionOrientation {
		expectedBoundary = "guarded_dual_arm"
	} else if s.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG || s.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
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
			return fmt.Errorf("orientation-probe-v1 promotion manifest requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps")
		}
		for name, expected := range expectedCaps {
			if actual, found := manifest.Caps[name]; !found || actual != expected {
				return fmt.Errorf("orientation-probe-v1 promotion manifest requires %s=%d", name, expected)
			}
		}
		if manifest.FallbackExecutor != string(optimize.ExpansionSearchStepwiseForward) {
			return fmt.Errorf("orientation-probe-v1 promotion manifest requires fallback %q", optimize.ExpansionSearchStepwiseForward)
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
	manifestQueries := make([]string, 0)
	for _, bucket := range manifest.Buckets {
		if !slices.Contains(bucket.QualificationSplit, "training") || !slices.Contains(bucket.QualificationSplit, "holdout") {
			return fmt.Errorf("each promotion bucket requires training and holdout qualification")
		}
		manifestQueries = append(manifestQueries, bucket.QuerySHA256...)
	}
	sort.Strings(manifestQueries)
	manifestQueries = slices.Compact(manifestQueries)
	policyQueries := append([]string(nil), s.QuerySHA256Allowlist...)
	sort.Strings(policyQueries)
	policyQueries = slices.Compact(policyQueries)
	if !slices.Equal(manifestQueries, policyQueries) {
		return fmt.Errorf("query allowlist must exactly match the promotion manifest buckets")
	}
	for _, role := range []string{"aa", "confirmation", "performance", "resource", "reference_closure", "operational"} {
		if evidence, found := manifest.Evidence[role]; !found || !lowerHexSHA256(evidence.SHA256) {
			return fmt.Errorf("promotion manifest requires digest-bound %s evidence", role)
		}
	}
	if len(s.QuerySHA256Allowlist) == 0 {
		return fmt.Errorf("enabled traversal policy requires a nonempty query SHA-256 allowlist")
	}
	if s.ShortestPathExecutor != "" && s.EnableExpansionOrientation {
		return fmt.Errorf("one traversal policy generation may enable only one candidate family")
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

func lowerHexSHA256(value string) bool {
	if value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func productionCanaryExecutor(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		optimize.ShortestPathExecutorASPI1DAG:
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
	raw, _ := json.Marshal(policy)
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

func (s *SchemaManager) effectiveTraversalPolicy(query string, isolation pgx.TxIsoLevel) (TraversalPolicy, string) {
	s.traversalPolicyLock.RLock()
	policy := s.traversalPolicy
	s.traversalPolicyLock.RUnlock()
	if policy.DisableInlineASPDAG && policy.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG {
		policy.ShortestPathExecutor = ""
	}
	if policy.DisableInlineSPWitness && policy.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
		policy.ShortestPathExecutor = ""
	}

	_, queryAuthorized := policy.compiledBuckets[TraversalPolicyQuerySHA256(query)]
	effective := policy.enabled() && (policy.DisableEndpointSeededReverse || policy.DisableInlineASPDAG || policy.DisableInlineSPWitness || queryAuthorized)
	if shortestPathExecutorRequiresStableSnapshot(policy.ShortestPathExecutor) && isolation != pgx.RepeatableRead && isolation != pgx.Serializable {
		effective = false
	}
	if !effective {
		return TraversalPolicy{}, "production-incumbent-v1"
	}
	return policy, policy.compiledIdentity
}

func shortestPathExecutorRequiresStableSnapshot(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorB1AlternatingNodeDistance,
		optimize.ShortestPathExecutorB1AlternatingNodeWitness,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness,
		optimize.ShortestPathExecutorASPB1AlternatingNodeDAG,
		optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG,
		optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		optimize.ShortestPathExecutorASPI1DAG:
		return true
	default:
		return false
	}
}
