package optimize

// Endpoint-resolution limits are immutable analysis metadata for the first
// bounded-resolution envelope. Each runtime limit has an explicit cap+1
// sentinel; this slice records the contract without changing execution.
const (
	EndpointResolutionSingletonLimit    int64 = 1
	EndpointResolutionSingletonSentinel int64 = 2
	EndpointResolutionSmallSetLimit     int64 = 32
	EndpointResolutionSmallSetSentinel  int64 = 33
)

// EndpointResolutionClass identifies how one traversal endpoint, or a
// correlated endpoint pair, could be resolved before traversal.
type EndpointResolutionClass string

const (
	EndpointResolutionClassIDEquality                EndpointResolutionClass = "id_equality"
	EndpointResolutionClassUniquePropertyEquality    EndpointResolutionClass = "unique_property_equality"
	EndpointResolutionClassNonUniquePropertyEquality EndpointResolutionClass = "nonunique_property_equality"
	EndpointResolutionClassExplicitSmallSet          EndpointResolutionClass = "explicit_small_set"
	EndpointResolutionClassCorrelatedPair            EndpointResolutionClass = "correlated_pair"
	EndpointResolutionClassUnsupported               EndpointResolutionClass = "unsupported"
)

// EndpointResolutionPlan identifies the exact incumbent and the planned-only
// bounded resolver independently of any shortest-path executor.
type EndpointResolutionPlan string

const (
	EndpointResolutionPlanIncumbent EndpointResolutionPlan = "ENDPOINT-RESOLUTION-INCUMBENT"
	EndpointResolutionPlanBounded   EndpointResolutionPlan = "ENDPOINT-RESOLUTION-BOUNDED"
)

const (
	EndpointResolutionFallbackPlannedOnly      = "planned_only"
	EndpointResolutionFallbackMutation         = "mutation"
	EndpointResolutionFallbackOptionalMatch    = "optional_match"
	EndpointResolutionFallbackCorrelatedPair   = "correlated_pair"
	EndpointResolutionFallbackUnsupported      = "unsupported_endpoint_class"
	EndpointResolutionFallbackSmallSetOverflow = "explicit_small_set_overflow"
)

// EndpointResolutionCaps serializes both admitted cardinalities and their
// overflow sentinels so future SQL cannot silently reinterpret the contract.
type EndpointResolutionCaps struct {
	SingletonLimit    int64 `json:"singleton_limit"`
	SingletonSentinel int64 `json:"singleton_sentinel"`
	SmallSetLimit     int64 `json:"small_set_limit"`
	SmallSetSentinel  int64 `json:"small_set_sentinel"`
}

// EndpointResolutionInput records one endpoint's statically recognizable
// resolution shape. Cardinality remains runtime evidence.
type EndpointResolutionInput struct {
	Symbol           string                  `json:"symbol"`
	Class            EndpointResolutionClass `json:"class"`
	Property         string                  `json:"property,omitempty"`
	StaticValueCount int                     `json:"static_value_count,omitempty"`
	ParameterizedSet bool                    `json:"parameterized_set,omitempty"`
	Limit            int64                   `json:"limit,omitempty"`
	Sentinel         int64                   `json:"sentinel,omitempty"`
}

// EndpointResolutionEligibilityFact records one conservative qualification
// check for bounded endpoint materialization.
type EndpointResolutionEligibilityFact struct {
	Name     string `json:"name"`
	Eligible bool   `json:"eligible"`
}

// EndpointResolutionDecision is analysis-only metadata for one SP/ASP
// traversal. The exact existing resolver remains selected in this milestone.
type EndpointResolutionDecision struct {
	Target               TraversalStepTarget                 `json:"target"`
	Family               string                              `json:"family"`
	Root                 EndpointResolutionInput             `json:"root"`
	Terminal             EndpointResolutionInput             `json:"terminal"`
	PairClass            EndpointResolutionClass             `json:"pair_class,omitempty"`
	PlannedClasses       []EndpointResolutionClass           `json:"planned_classes"`
	Caps                 EndpointResolutionCaps              `json:"caps"`
	PlannedCandidates    []EndpointResolutionPlan            `json:"planned_candidates"`
	CandidatePlan        EndpointResolutionPlan              `json:"candidate_plan"`
	SelectedPlan         EndpointResolutionPlan              `json:"selected_plan"`
	FallbackPlan         EndpointResolutionPlan              `json:"fallback_plan"`
	EligibilityFacts     []EndpointResolutionEligibilityFact `json:"eligibility_facts"`
	StructurallyEligible bool                                `json:"structurally_eligible"`
	StaticallyEligible   bool                                `json:"statically_eligible"`
	SelectionMode        string                              `json:"selection_mode"`
	SelectorVersion      string                              `json:"selector_version"`
	FallbackReason       string                              `json:"fallback_reason"`
}

// TraversalPredicateClass identifies the strongest safe placement property
// proven from syntax. Unsupported path forms deliberately remain conservative.
type TraversalPredicateClass string

const (
	TraversalPredicateClassStepLocalNode              TraversalPredicateClass = "step_local_node"
	TraversalPredicateClassStepLocalRelationship      TraversalPredicateClass = "step_local_relationship"
	TraversalPredicateClassUniversalAllNodes          TraversalPredicateClass = "universal_all_nodes"
	TraversalPredicateClassUniversalNoneNodes         TraversalPredicateClass = "universal_none_nodes"
	TraversalPredicateClassUniversalAllRelationships  TraversalPredicateClass = "universal_all_relationships"
	TraversalPredicateClassUniversalNoneRelationships TraversalPredicateClass = "universal_none_relationships"
	TraversalPredicateClassWholePath                  TraversalPredicateClass = "whole_path"
	TraversalPredicateClassUnsupported                TraversalPredicateClass = "unsupported"
)

// TraversalPredicatePlan separates planned step evaluation from the exact
// incumbent predicate placement that remains selected.
type TraversalPredicatePlan string

const (
	TraversalPredicatePlanIncumbent TraversalPredicatePlan = "TRAVERSAL-PREDICATE-INCUMBENT"
	TraversalPredicatePlanStep      TraversalPredicatePlan = "TRAVERSAL-PREDICATE-STEP"
)

const (
	TraversalPredicateFallbackPlannedOnly = "planned_only"
	TraversalPredicateFallbackMutation    = "mutation"
	TraversalPredicateFallbackOptional    = "optional_match"
	TraversalPredicateFallbackCorrelation = "correlated_predicate"
	TraversalPredicateFallbackWholePath   = "whole_path"
	TraversalPredicateFallbackUnsupported = "unsupported_predicate"
)

// TraversalPredicateEligibilityFact records one conservative classification
// or placement qualification.
type TraversalPredicateEligibilityFact struct {
	Name     string `json:"name"`
	Eligible bool   `json:"eligible"`
}

// TraversalPredicateDecision records one predicate relevant to a variable
// traversal. It never authorizes placement by itself.
type TraversalPredicateDecision struct {
	Target               TraversalStepTarget                 `json:"target"`
	PredicateIndex       int                                 `json:"predicate_index"`
	Source               string                              `json:"source"`
	Class                TraversalPredicateClass             `json:"class"`
	PathSymbol           string                              `json:"path_symbol,omitempty"`
	BindingSymbol        string                              `json:"binding_symbol,omitempty"`
	ReferencedSymbols    []string                            `json:"referenced_symbols,omitempty"`
	PlannedCandidates    []TraversalPredicatePlan            `json:"planned_candidates"`
	CandidatePlan        TraversalPredicatePlan              `json:"candidate_plan,omitempty"`
	SelectedPlan         TraversalPredicatePlan              `json:"selected_plan"`
	FallbackPlan         TraversalPredicatePlan              `json:"fallback_plan"`
	EligibilityFacts     []TraversalPredicateEligibilityFact `json:"eligibility_facts"`
	StructurallyEligible bool                                `json:"structurally_eligible"`
	StaticallyEligible   bool                                `json:"statically_eligible"`
	SelectionMode        string                              `json:"selection_mode"`
	ClassifierVersion    string                              `json:"classifier_version"`
	FallbackReason       string                              `json:"fallback_reason"`
}
