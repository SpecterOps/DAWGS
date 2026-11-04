package pg

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

// TraversalShapeVersion identifies the stable structural identity used by
// production-wide selection. It deliberately excludes query text, identifiers,
// caller values, graph contents, and transaction state.
const TraversalShapeVersion = "traversal-shape-v1"

// TraversalFixedSuffixShapeVersion identifies the independently versioned
// structural identity for a variable expansion with a fixed terminal suffix.
// It intentionally does not reinterpret existing shortest-path v1 hashes.
const TraversalFixedSuffixShapeVersion = "fixed-suffix-shape-v1"

// TraversalShape describes one statically classified shortest-path target.
// A zero value means the query is not in the initial structural selector scope.
type TraversalShape struct {
	// Version identifies the structural-classification schema used for this shape.
	Version string `json:"version"`

	// Family identifies the eligible traversal family.
	Family string `json:"family"`

	// Direction records the traversal's logical direction.
	Direction string `json:"direction"`

	// ObservationMode identifies the semantic observation mode used by the plan.
	ObservationMode string `json:"observation_mode"`

	// MinimumDepth is the inclusive lower bound of the variable expansion.
	MinimumDepth int64 `json:"minimum_depth"`

	// MaximumDepth is the inclusive upper bound of the variable expansion.
	MaximumDepth int64 `json:"maximum_depth"`

	// RelationshipKindCount is the number of relationship kinds constrained by the query.
	RelationshipKindCount int `json:"relationship_kind_count"`

	// UntypedRelationship reports whether the expansion permits any relationship kind.
	UntypedRelationship bool `json:"untyped_relationship"`

	// SuffixLength is the fixed terminal suffix length for fixed-suffix expansions.
	SuffixLength int `json:"suffix_length,omitempty"`

	// CandidateStrategy is the eligible optimizer strategy for this shape.
	CandidateStrategy string `json:"candidate_strategy,omitempty"`

	// Fingerprint is the stable digest of the fields that define this shape.
	Fingerprint string `json:"fingerprint"`
}

// Available reports whether the classifier found exactly one initial-scope
// traversal target. Multiple targets remain on the incumbent until a later
// selector version defines their joint semantics.
func (s TraversalShape) Available() bool {
	return s.Version != "" && s.Family != "" && s.Fingerprint != ""
}

// TraversalStrategySelection is query-text-free selection telemetry. It
// observes policy routing without retaining a decision outside the query.
type TraversalStrategySelection struct {
	// Shape is the classified traversal structure considered for selection.
	Shape TraversalShape `json:"shape"`

	// PolicyGeneration identifies the active traversal-policy generation.
	PolicyGeneration uint64 `json:"policy_generation"`

	// SelectorVersion identifies the selection protocol evaluated for the query.
	SelectorVersion string `json:"selector_version"`

	// Candidate names the strategy that could replace the incumbent.
	Candidate string `json:"candidate"`

	// SelectedArm identifies the candidate or incumbent path that was selected.
	SelectedArm string `json:"selected_arm"`

	// Fallback identifies the incumbent strategy used when the candidate is not selected.
	Fallback string `json:"fallback"`

	// Bucket identifies the manifest bucket that authorized the selection, when any.
	Bucket string `json:"bucket,omitempty"`

	// TemplateSHA256 identifies the approved SQL template used by the selected bucket.
	TemplateSHA256 string `json:"template_sha256,omitempty"`

	// Mode identifies the selection mechanism that produced the decision.
	Mode string `json:"mode"`

	// Reason records the query-text-free rationale for the decision.
	Reason string `json:"reason"`
}

// TraversalStrategySelectionCollector is an optional diagnostic seam. It is
// intentionally separate from translation caching and cannot alter routing.
type TraversalStrategySelectionCollector interface {
	// RecordTraversalStrategySelection receives query-text-free routing telemetry.
	RecordTraversalStrategySelection(TraversalStrategySelection)
}

// TraversalShapeCacheProvider optionally retains bounded, query-text-free
// structural classifications. It must invalidate every entry with schema
// generation changes and may always bypass retention safely.
type TraversalShapeCacheProvider interface {
	// TraversalShapeFor returns a cached classification or calls classify to produce one.
	TraversalShapeFor(query string, classify func() (TraversalShape, error)) (TraversalShape, error)
}

// traversalShapeForQuery optimizes query and classifies its sole eligible traversal target.
func traversalShapeForQuery(query *cypher.RegularQuery) (TraversalShape, error) {
	plan, err := optimize.Optimize(query)
	if err != nil {
		return TraversalShape{}, err
	}
	return traversalShapeForPlan(plan), nil
}

// traversalShapeForPlan derives a cacheable structural shape from a lowered plan.
func traversalShapeForPlan(plan optimize.Plan) TraversalShape {
	if len(plan.LoweringPlan.ShortestPathExecutor) != 1 {
		if len(plan.LoweringPlan.ExpansionSearchStrategy) != 1 {
			return TraversalShape{}
		}
		decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
		if decision.Family != "fixed_suffix_expansion" || !decision.StructurallyEligible || decision.SuffixLength != 3 {
			return TraversalShape{}
		}
		shape := TraversalShape{
			Version:           TraversalFixedSuffixShapeVersion,
			Family:            decision.Family,
			Direction:         decision.LogicalDirection,
			ObservationMode:   string(decision.ObservationMode),
			MinimumDepth:      decision.MinimumDepth,
			MaximumDepth:      decision.MaximumDepth,
			SuffixLength:      decision.SuffixLength,
			CandidateStrategy: string(decision.CandidateStrategy),
		}
		shape.Fingerprint = TraversalShapeFingerprint(shape)
		return shape
	}
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	if !decision.StructurallyEligible {
		return TraversalShape{}
	}
	shape := TraversalShape{
		Version:               TraversalShapeVersion,
		Family:                decision.Family,
		Direction:             decision.Direction.String(),
		ObservationMode:       string(decision.ObservationMode),
		MinimumDepth:          decision.MinimumDepth,
		MaximumDepth:          decision.MaximumDepth,
		RelationshipKindCount: decision.RelationshipKindCount,
		UntypedRelationship:   decision.UntypedRelationship,
	}
	shape.Fingerprint = TraversalShapeFingerprint(shape)
	return shape
}

// TraversalShapeFingerprint returns the immutable digest that a structural
// promotion bucket must bind. It never includes query text or runtime values.
func TraversalShapeFingerprint(shape TraversalShape) string {
	if shape.Version == TraversalFixedSuffixShapeVersion {
		canonical := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d|%s", shape.Version, shape.Family, shape.Direction, shape.ObservationMode, shape.MinimumDepth, shape.MaximumDepth, shape.SuffixLength, shape.CandidateStrategy)
		digest := sha256.Sum256([]byte(canonical))
		return hex.EncodeToString(digest[:])
	}
	canonical := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d|%t", shape.Version, shape.Family, shape.Direction, shape.ObservationMode, shape.MinimumDepth, shape.MaximumDepth, shape.RelationshipKindCount, shape.UntypedRelationship)
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:])
}

// shouldClassifyTraversal reports whether policy routing or telemetry needs a traversal shape.
func (s *SchemaManager) shouldClassifyTraversal() bool {
	if _, observed := s.translationCacheProvider.(TraversalStrategySelectionCollector); observed {
		return true
	}
	return s.hasStructuralTraversalPolicy()
}

// classifyTraversalShape classifies parsed and uses the provider's bounded cache when available.
func (s *SchemaManager) classifyTraversalShape(query string, parsed *cypher.RegularQuery) (TraversalShape, error) {
	classify := func() (TraversalShape, error) {
		return traversalShapeForQuery(parsed)
	}
	if cache, found := s.translationCacheProvider.(TraversalShapeCacheProvider); found {
		return cache.TraversalShapeFor(query, classify)
	}
	return classify()
}

// observeTraversalStrategySelection emits query-text-free routing telemetry to an optional collector.
func (s *SchemaManager) observeTraversalStrategySelection(query string, shape TraversalShape, policy TraversalPolicy) {
	collector, ok := s.translationCacheProvider.(TraversalStrategySelectionCollector)
	if !ok {
		return
	}
	selection := TraversalStrategySelection{Shape: shape, SelectedArm: "incumbent", Fallback: "incumbent", Mode: "incumbent", Reason: "policy_inactive"}
	if !shape.Available() {
		selection.Reason = "shape_unavailable"
	} else if policy.enabled() {
		selection.PolicyGeneration = policy.Generation
		selection.SelectorVersion = policy.compiledManifest.SelectorVersion
		selection.Candidate = string(policy.ShortestPathExecutor)
		selection.Fallback = policy.compiledManifest.FallbackExecutor
		if policy.EnableTopologyFixedSuffix || policy.EnableTopologyFixedSuffixFirstUse {
			selection.Candidate = string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1)
			if policy.EnableTopologyFixedSuffixFirstUse {
				selection.Candidate = string(optimize.ExpansionSearchPolicyTopologyFixedSuffixFirstUseV1)
			}
			if bucket, authorized := policy.authorizedStructuralBucketForShape(shape); authorized {
				selection.SelectedArm = "candidate"
				selection.Bucket = bucket.Name
				selection.TemplateSHA256 = bucket.SQLTemplateSHA256
				selection.Mode = "topology_selected"
				selection.Reason = "topology_route_candidate_hit"
			}
		}
		selectCandidate := func(bucket traversalPromotionBucket, mode, reason string) {
			selection.SelectedArm = "candidate"
			selection.Bucket = bucket.Name
			selection.TemplateSHA256 = bucket.SQLTemplateSHA256
			selection.Mode = mode
			selection.Reason = reason
		}
		if policy.EnableTopologyFixedSuffix || policy.EnableTopologyFixedSuffixFirstUse {
			// The topology branch above is selected only by a transaction-local
			// route-cache hit, never by an evidence-query allowlist.
		} else if _, authorized := policy.compiledBuckets[TraversalPolicyQuerySHA256(strings.TrimSpace(query))]; authorized {
			selectCandidate(policy.compiledBuckets[TraversalPolicyQuerySHA256(strings.TrimSpace(query))], "exact_query_canary", "exact_query_authorized")
		} else if bucket, authorized := policy.authorizedStructuralBucketForShape(shape); authorized {
			selectCandidate(bucket, "structural_authorized", "structural_bucket_"+bucket.Name)
		} else if bucket, matched := policy.structuralBucketForShape(shape); matched {
			selection.Mode = "structural_shadow"
			selection.Reason = "structural_bucket_" + bucket.Name
		} else {
			selection.Reason = "exact_query_not_authorized"
		}
	}
	collector.RecordTraversalStrategySelection(selection)
}
