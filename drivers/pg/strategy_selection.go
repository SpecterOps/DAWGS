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
	Version               string `json:"version"`
	Family                string `json:"family"`
	Direction             string `json:"direction"`
	ObservationMode       string `json:"observation_mode"`
	MinimumDepth          int64  `json:"minimum_depth"`
	MaximumDepth          int64  `json:"maximum_depth"`
	RelationshipKindCount int    `json:"relationship_kind_count"`
	UntypedRelationship   bool   `json:"untyped_relationship"`
	SuffixLength          int    `json:"suffix_length,omitempty"`
	CandidateStrategy     string `json:"candidate_strategy,omitempty"`
	Fingerprint           string `json:"fingerprint"`
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
	Shape            TraversalShape `json:"shape"`
	PolicyGeneration uint64         `json:"policy_generation"`
	SelectorVersion  string         `json:"selector_version"`
	Candidate        string         `json:"candidate"`
	SelectedArm      string         `json:"selected_arm"`
	Fallback         string         `json:"fallback"`
	Bucket           string         `json:"bucket,omitempty"`
	TemplateSHA256   string         `json:"template_sha256,omitempty"`
	Mode             string         `json:"mode"`
	Reason           string         `json:"reason"`
}

// TraversalStrategySelectionCollector is an optional diagnostic seam. It is
// intentionally separate from translation caching and cannot alter routing.
type TraversalStrategySelectionCollector interface {
	RecordTraversalStrategySelection(TraversalStrategySelection)
}

// TraversalShapeCacheProvider optionally retains bounded, query-text-free
// structural classifications. It must invalidate every entry with schema
// generation changes and may always bypass retention safely.
type TraversalShapeCacheProvider interface {
	TraversalShapeFor(query string, classify func() (TraversalShape, error)) (TraversalShape, error)
}

func traversalShapeForQuery(query *cypher.RegularQuery) (TraversalShape, error) {
	plan, err := optimize.Optimize(query)
	if err != nil {
		return TraversalShape{}, err
	}
	return traversalShapeForPlan(plan), nil
}

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

func (s *SchemaManager) shouldClassifyTraversal() bool {
	if _, observed := s.translationCacheProvider.(TraversalStrategySelectionCollector); observed {
		return true
	}
	return s.hasStructuralTraversalPolicy()
}

func (s *SchemaManager) classifyTraversalShape(query string, parsed *cypher.RegularQuery) (TraversalShape, error) {
	classify := func() (TraversalShape, error) {
		return traversalShapeForQuery(parsed)
	}
	if cache, found := s.translationCacheProvider.(TraversalShapeCacheProvider); found {
		return cache.TraversalShapeFor(query, classify)
	}
	return classify()
}

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
		selectCandidate := func(bucket traversalPromotionBucket, mode, reason string) {
			selection.SelectedArm = "candidate"
			selection.Bucket = bucket.Name
			selection.TemplateSHA256 = bucket.SQLTemplateSHA256
			selection.Mode = mode
			selection.Reason = reason
		}
		if _, authorized := policy.compiledBuckets[TraversalPolicyQuerySHA256(strings.TrimSpace(query))]; authorized {
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
