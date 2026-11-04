package optimize

// Endpoint-resolution limits are immutable analysis metadata for the first
// bounded-resolution envelope. Each runtime limit has an explicit cap+1
// sentinel; this slice records the contract without changing execution.
const (
	// EndpointResolutionSingletonLimit reserves the stable protocol value used to recognize endpoint resolution singleton limit across artifacts and executions.
	EndpointResolutionSingletonLimit int64 = 1

	// EndpointResolutionSingletonSentinel reserves the stable protocol value used to recognize endpoint resolution singleton sentinel across artifacts and executions.
	EndpointResolutionSingletonSentinel int64 = 2

	// EndpointResolutionSmallSetLimit reserves the stable protocol value used to recognize endpoint resolution small set limit across artifacts and executions.
	EndpointResolutionSmallSetLimit int64 = 32

	// EndpointResolutionSmallSetSentinel reserves the stable protocol value used to recognize endpoint resolution small set sentinel across artifacts and executions.
	EndpointResolutionSmallSetSentinel int64 = 33
)

// EndpointResolutionClass identifies how one traversal endpoint, or a
// correlated endpoint pair, could be resolved before traversal.
type EndpointResolutionClass string

const (
	// EndpointResolutionClassIDEquality reserves the stable protocol value used to recognize endpoint resolution class id equality across artifacts and executions.
	EndpointResolutionClassIDEquality EndpointResolutionClass = "id_equality"

	// EndpointResolutionClassUniquePropertyEquality reserves the stable protocol value used to recognize endpoint resolution class unique property equality across artifacts and executions.
	EndpointResolutionClassUniquePropertyEquality EndpointResolutionClass = "unique_property_equality"

	// EndpointResolutionClassNonUniquePropertyEquality reserves the stable protocol value used to recognize endpoint resolution class non unique property equality across artifacts and executions.
	EndpointResolutionClassNonUniquePropertyEquality EndpointResolutionClass = "nonunique_property_equality"

	// EndpointResolutionClassExplicitSmallSet reserves the stable protocol value used to recognize endpoint resolution class explicit small set across artifacts and executions.
	EndpointResolutionClassExplicitSmallSet EndpointResolutionClass = "explicit_small_set"

	// EndpointResolutionClassCorrelatedPair reserves the stable protocol value used to recognize endpoint resolution class correlated pair across artifacts and executions.
	EndpointResolutionClassCorrelatedPair EndpointResolutionClass = "correlated_pair"

	// EndpointResolutionClassUnsupported reserves the stable protocol value used to recognize endpoint resolution class unsupported across artifacts and executions.
	EndpointResolutionClassUnsupported EndpointResolutionClass = "unsupported"
)

// EndpointResolutionPlan identifies the exact incumbent and the planned-only
// bounded resolver independently of any shortest-path executor.
type EndpointResolutionPlan string

const (
	// EndpointResolutionPlanIncumbent reserves the stable protocol value used to recognize endpoint resolution plan incumbent across artifacts and executions.
	EndpointResolutionPlanIncumbent EndpointResolutionPlan = "ENDPOINT-RESOLUTION-INCUMBENT"

	// EndpointResolutionPlanBounded reserves the stable protocol value used to recognize endpoint resolution plan bounded across artifacts and executions.
	EndpointResolutionPlanBounded EndpointResolutionPlan = "ENDPOINT-RESOLUTION-BOUNDED"
)

const (
	// EndpointResolutionFallbackPlannedOnly reserves the stable protocol value used to recognize endpoint resolution fallback planned only across artifacts and executions.
	EndpointResolutionFallbackPlannedOnly = "planned_only"

	// EndpointResolutionFallbackMutation reserves the stable protocol value used to recognize endpoint resolution fallback mutation across artifacts and executions.
	EndpointResolutionFallbackMutation = "mutation"

	// EndpointResolutionFallbackOptionalMatch reserves the stable protocol value used to recognize endpoint resolution fallback optional match across artifacts and executions.
	EndpointResolutionFallbackOptionalMatch = "optional_match"

	// EndpointResolutionFallbackCorrelatedPair reserves the stable protocol value used to recognize endpoint resolution fallback correlated pair across artifacts and executions.
	EndpointResolutionFallbackCorrelatedPair = "correlated_pair"

	// EndpointResolutionFallbackUnsupported reserves the stable protocol value used to recognize endpoint resolution fallback unsupported across artifacts and executions.
	EndpointResolutionFallbackUnsupported = "unsupported_endpoint_class"

	// EndpointResolutionFallbackSmallSetOverflow reserves the stable protocol value used to recognize endpoint resolution fallback small set overflow across artifacts and executions.
	EndpointResolutionFallbackSmallSetOverflow = "explicit_small_set_overflow"
)

// EndpointResolutionCaps serializes both admitted cardinalities and their
// overflow sentinels so future SQL cannot silently reinterpret the contract.
type EndpointResolutionCaps struct {
	// SingletonLimit supplies the singleton limit input to the EndpointResolutionCaps contract.
	SingletonLimit int64 `json:"singleton_limit"`
	// SingletonSentinel supplies the singleton sentinel input to the EndpointResolutionCaps contract.
	SingletonSentinel int64 `json:"singleton_sentinel"`
	// SmallSetLimit supplies the small set limit input to the EndpointResolutionCaps contract.
	SmallSetLimit int64 `json:"small_set_limit"`
	// SmallSetSentinel supplies the small set sentinel input to the EndpointResolutionCaps contract.
	SmallSetSentinel int64 `json:"small_set_sentinel"`
}

// EndpointResolutionInput records one endpoint's statically recognizable
// resolution shape. Cardinality remains runtime evidence.
type EndpointResolutionInput struct {
	// Symbol supplies the symbol input to the EndpointResolutionInput contract.
	Symbol string `json:"symbol"`
	// Class supplies the class input to the EndpointResolutionInput contract.
	Class EndpointResolutionClass `json:"class"`
	// Property supplies the property input to the EndpointResolutionInput contract.
	Property string `json:"property,omitempty"`
	// StaticValueCount records the number of static value count.
	StaticValueCount int `json:"static_value_count,omitempty"`
	// ParameterizedSet indicates whether parameterized set applies.
	ParameterizedSet bool `json:"parameterized_set,omitempty"`
	// Limit supplies the limit input to the EndpointResolutionInput contract.
	Limit int64 `json:"limit,omitempty"`
	// Sentinel supplies the sentinel input to the EndpointResolutionInput contract.
	Sentinel int64 `json:"sentinel,omitempty"`
}

// EndpointResolutionEligibilityFact records one conservative qualification
// check for bounded endpoint materialization.
type EndpointResolutionEligibilityFact struct {
	// Name identifies the name.
	Name string `json:"name"`
	// Eligible indicates whether eligible applies.
	Eligible bool `json:"eligible"`
}

// EndpointResolutionDecision is analysis-only metadata for one SP/ASP
// traversal. The exact existing resolver remains selected in this milestone.
type EndpointResolutionDecision struct {
	// Target supplies the target input to the EndpointResolutionDecision contract.
	Target TraversalStepTarget `json:"target"`
	// Family supplies the family input to the EndpointResolutionDecision contract.
	Family string `json:"family"`
	// Root supplies the root input to the EndpointResolutionDecision contract.
	Root EndpointResolutionInput `json:"root"`
	// Terminal supplies the terminal input to the EndpointResolutionDecision contract.
	Terminal EndpointResolutionInput `json:"terminal"`
	// PairClass supplies the pair class input to the EndpointResolutionDecision contract.
	PairClass EndpointResolutionClass `json:"pair_class,omitempty"`
	// PlannedClasses supplies the planned classes input to the EndpointResolutionDecision contract.
	PlannedClasses []EndpointResolutionClass `json:"planned_classes"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps EndpointResolutionCaps `json:"caps"`
	// PlannedCandidates supplies the planned candidates input to the EndpointResolutionDecision contract.
	PlannedCandidates []EndpointResolutionPlan `json:"planned_candidates"`
	// CandidatePlan supplies the candidate plan input to the EndpointResolutionDecision contract.
	CandidatePlan EndpointResolutionPlan `json:"candidate_plan"`
	// SelectedPlan supplies the selected plan input to the EndpointResolutionDecision contract.
	SelectedPlan EndpointResolutionPlan `json:"selected_plan"`
	// FallbackPlan supplies the fallback plan input to the EndpointResolutionDecision contract.
	FallbackPlan EndpointResolutionPlan `json:"fallback_plan"`
	// EligibilityFacts supplies the eligibility facts input to the EndpointResolutionDecision contract.
	EligibilityFacts []EndpointResolutionEligibilityFact `json:"eligibility_facts"`
	// StructurallyEligible indicates whether structurally eligible applies.
	StructurallyEligible bool `json:"structurally_eligible"`
	// StaticallyEligible indicates whether statically eligible applies.
	StaticallyEligible bool `json:"statically_eligible"`
	// SelectionMode identifies the selection mode.
	SelectionMode string `json:"selection_mode"`
	// SelectorVersion identifies the schema version for selector version.
	SelectorVersion string `json:"selector_version"`
	// FallbackReason supplies the fallback reason input to the EndpointResolutionDecision contract.
	FallbackReason string `json:"fallback_reason"`
}

// TraversalPredicateClass identifies the strongest safe placement property
// proven from syntax. Unsupported path forms deliberately remain conservative.
type TraversalPredicateClass string

const (
	// TraversalPredicateClassStepLocalNode reserves the stable protocol value used to recognize traversal predicate class step local node across artifacts and executions.
	TraversalPredicateClassStepLocalNode TraversalPredicateClass = "step_local_node"

	// TraversalPredicateClassStepLocalRelationship reserves the stable protocol value used to recognize traversal predicate class step local relationship across artifacts and executions.
	TraversalPredicateClassStepLocalRelationship TraversalPredicateClass = "step_local_relationship"

	// TraversalPredicateClassUniversalAllNodes reserves the stable protocol value used to recognize traversal predicate class universal all nodes across artifacts and executions.
	TraversalPredicateClassUniversalAllNodes TraversalPredicateClass = "universal_all_nodes"

	// TraversalPredicateClassUniversalNoneNodes reserves the stable protocol value used to recognize traversal predicate class universal none nodes across artifacts and executions.
	TraversalPredicateClassUniversalNoneNodes TraversalPredicateClass = "universal_none_nodes"

	// TraversalPredicateClassUniversalAllRelationships reserves the stable protocol value used to recognize traversal predicate class universal all relationships across artifacts and executions.
	TraversalPredicateClassUniversalAllRelationships TraversalPredicateClass = "universal_all_relationships"

	// TraversalPredicateClassUniversalNoneRelationships reserves the stable protocol value used to recognize traversal predicate class universal none relationships across artifacts and executions.
	TraversalPredicateClassUniversalNoneRelationships TraversalPredicateClass = "universal_none_relationships"

	// TraversalPredicateClassWholePath reserves the stable protocol value used to recognize traversal predicate class whole path across artifacts and executions.
	TraversalPredicateClassWholePath TraversalPredicateClass = "whole_path"

	// TraversalPredicateClassUnsupported reserves the stable protocol value used to recognize traversal predicate class unsupported across artifacts and executions.
	TraversalPredicateClassUnsupported TraversalPredicateClass = "unsupported"
)

// TraversalPredicatePlan separates planned step evaluation from the exact
// incumbent predicate placement that remains selected.
type TraversalPredicatePlan string

const (
	// TraversalPredicatePlanIncumbent reserves the stable protocol value used to recognize traversal predicate plan incumbent across artifacts and executions.
	TraversalPredicatePlanIncumbent TraversalPredicatePlan = "TRAVERSAL-PREDICATE-INCUMBENT"

	// TraversalPredicatePlanStep reserves the stable protocol value used to recognize traversal predicate plan step across artifacts and executions.
	TraversalPredicatePlanStep TraversalPredicatePlan = "TRAVERSAL-PREDICATE-STEP"
)

const (
	// TraversalPredicateFallbackPlannedOnly reserves the stable protocol value used to recognize traversal predicate fallback planned only across artifacts and executions.
	TraversalPredicateFallbackPlannedOnly = "planned_only"

	// TraversalPredicateFallbackMutation reserves the stable protocol value used to recognize traversal predicate fallback mutation across artifacts and executions.
	TraversalPredicateFallbackMutation = "mutation"

	// TraversalPredicateFallbackOptional reserves the stable protocol value used to recognize traversal predicate fallback optional across artifacts and executions.
	TraversalPredicateFallbackOptional = "optional_match"

	// TraversalPredicateFallbackCorrelation reserves the stable protocol value used to recognize traversal predicate fallback correlation across artifacts and executions.
	TraversalPredicateFallbackCorrelation = "correlated_predicate"

	// TraversalPredicateFallbackWholePath reserves the stable protocol value used to recognize traversal predicate fallback whole path across artifacts and executions.
	TraversalPredicateFallbackWholePath = "whole_path"

	// TraversalPredicateFallbackUnsupported reserves the stable protocol value used to recognize traversal predicate fallback unsupported across artifacts and executions.
	TraversalPredicateFallbackUnsupported = "unsupported_predicate"
)

// TraversalPredicateEligibilityFact records one conservative classification
// or placement qualification.
type TraversalPredicateEligibilityFact struct {
	// Name identifies the name.
	Name string `json:"name"`
	// Eligible indicates whether eligible applies.
	Eligible bool `json:"eligible"`
}

// TraversalPredicateDecision records one predicate relevant to a variable
// traversal. It never authorizes placement by itself.
type TraversalPredicateDecision struct {
	// Target supplies the target input to the TraversalPredicateDecision contract.
	Target TraversalStepTarget `json:"target"`
	// PredicateIndex supplies the predicate index input to the TraversalPredicateDecision contract.
	PredicateIndex int `json:"predicate_index"`
	// Source supplies the source input to the TraversalPredicateDecision contract.
	Source string `json:"source"`
	// Class supplies the class input to the TraversalPredicateDecision contract.
	Class TraversalPredicateClass `json:"class"`
	// PathSymbol supplies the path symbol input to the TraversalPredicateDecision contract.
	PathSymbol string `json:"path_symbol,omitempty"`
	// BindingSymbol supplies the binding symbol input to the TraversalPredicateDecision contract.
	BindingSymbol string `json:"binding_symbol,omitempty"`
	// ReferencedSymbols supplies the referenced symbols input to the TraversalPredicateDecision contract.
	ReferencedSymbols []string `json:"referenced_symbols,omitempty"`
	// PlannedCandidates supplies the planned candidates input to the TraversalPredicateDecision contract.
	PlannedCandidates []TraversalPredicatePlan `json:"planned_candidates"`
	// CandidatePlan supplies the candidate plan input to the TraversalPredicateDecision contract.
	CandidatePlan TraversalPredicatePlan `json:"candidate_plan,omitempty"`
	// SelectedPlan supplies the selected plan input to the TraversalPredicateDecision contract.
	SelectedPlan TraversalPredicatePlan `json:"selected_plan"`
	// FallbackPlan supplies the fallback plan input to the TraversalPredicateDecision contract.
	FallbackPlan TraversalPredicatePlan `json:"fallback_plan"`
	// EligibilityFacts supplies the eligibility facts input to the TraversalPredicateDecision contract.
	EligibilityFacts []TraversalPredicateEligibilityFact `json:"eligibility_facts"`
	// StructurallyEligible indicates whether structurally eligible applies.
	StructurallyEligible bool `json:"structurally_eligible"`
	// StaticallyEligible indicates whether statically eligible applies.
	StaticallyEligible bool `json:"statically_eligible"`
	// SelectionMode identifies the selection mode.
	SelectionMode string `json:"selection_mode"`
	// ClassifierVersion identifies the schema version for classifier version.
	ClassifierVersion string `json:"classifier_version"`
	// FallbackReason supplies the fallback reason input to the TraversalPredicateDecision contract.
	FallbackReason string `json:"fallback_reason"`
}
