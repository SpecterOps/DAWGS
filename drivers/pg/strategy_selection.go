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
	Mode             string         `json:"mode"`
	Reason           string         `json:"reason"`
}

// TraversalStrategySelectionCollector is an optional diagnostic seam. It is
// intentionally separate from translation caching and cannot alter routing.
type TraversalStrategySelectionCollector interface {
	RecordTraversalStrategySelection(TraversalStrategySelection)
}

func traversalShapeForQuery(query *cypher.RegularQuery) (TraversalShape, error) {
	plan, err := optimize.Optimize(query)
	if err != nil {
		return TraversalShape{}, err
	}
	if len(plan.LoweringPlan.ShortestPathExecutor) != 1 {
		return TraversalShape{}, nil
	}
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	if !decision.StructurallyEligible {
		return TraversalShape{}, nil
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
	canonical := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d|%t", shape.Version, shape.Family, shape.Direction, shape.ObservationMode, shape.MinimumDepth, shape.MaximumDepth, shape.RelationshipKindCount, shape.UntypedRelationship)
	digest := sha256.Sum256([]byte(canonical))
	shape.Fingerprint = hex.EncodeToString(digest[:])
	return shape, nil
}

func (s *SchemaManager) observeTraversalStrategySelection(query string, parsed *cypher.RegularQuery, policy TraversalPolicy) {
	collector, ok := s.translationCacheProvider.(TraversalStrategySelectionCollector)
	if !ok {
		return
	}
	shape, _ := traversalShapeForQuery(parsed)
	selection := TraversalStrategySelection{Shape: shape, Mode: "incumbent", Reason: "policy_inactive"}
	if !shape.Available() {
		selection.Reason = "shape_unavailable"
	} else if policy.enabled() {
		selection.PolicyGeneration = policy.Generation
		selection.SelectorVersion = policy.compiledManifest.SelectorVersion
		selection.Candidate = string(policy.ShortestPathExecutor)
		if _, authorized := policy.compiledBuckets[TraversalPolicyQuerySHA256(strings.TrimSpace(query))]; authorized {
			selection.Mode = "exact_query_canary"
			selection.Reason = "exact_query_authorized"
		} else {
			selection.Reason = "exact_query_not_authorized"
		}
	}
	collector.RecordTraversalStrategySelection(selection)
}
