package optimize

// contiguousExpansionOrientationCandidate contains the common typed metadata
// for one variable expansion and an adjacent fixed seed region. Prefix- and
// suffix-specific analyzers retain their own correctness facts and fallback
// reasons, then use this type to produce a consistent public decision.
type contiguousExpansionOrientationCandidate struct {
	Target             TraversalStepTarget
	Family             string
	PlannedPolicy      ExpansionSearchPolicy
	EmittedPolicy      ExpansionSearchPolicy
	PlannedCandidates  []ExpansionSearchStrategy
	EmittedCandidates  []ExpansionSearchStrategy
	CandidateStrategy  ExpansionSearchStrategy
	ProbeCaps          ExpansionSearchProbeCaps
	Admission          ExpansionSearchAdmission
	PrefixStartStep    int
	PrefixEndStep      int
	PrefixLength       int
	SuffixStartStep    int
	SuffixEndStep      int
	SuffixLength       int
	SeedPredicateClass string
	EndpointLimit      int64
}

// contiguousExpansionOrientationQualification contains analysis results that
// remain specific to the fixed-prefix or fixed-suffix correctness envelope.
type contiguousExpansionOrientationQualification struct {
	SelectedStrategy     ExpansionSearchStrategy
	StructurallyEligible bool
	StaticallyEligible   bool
	EligibilityFacts     []ExpansionSearchEligibilityFact
	HasFinalLimit        bool
	ObservationMode      ExpansionSearchObservationMode
	LogicalDirection     string
	MinimumDepth         int64
	MaximumDepth         int64
	SelectionMode        string
	SelectorVersion      string
	FallbackReason       string
}

// decision combines common orientation metadata with family-specific
// qualification without conflating a planned policy with emitted SQL.
func (s contiguousExpansionOrientationCandidate) decision(qualification contiguousExpansionOrientationQualification) ExpansionSearchStrategyDecision {
	return ExpansionSearchStrategyDecision{
		Target:               s.Target,
		Family:               s.Family,
		PlannedPolicy:        s.PlannedPolicy,
		EmittedPolicy:        s.EmittedPolicy,
		PlannedCandidates:    s.PlannedCandidates,
		EmittedCandidates:    s.EmittedCandidates,
		CandidateStrategy:    s.CandidateStrategy,
		SelectedStrategy:     qualification.SelectedStrategy,
		StructurallyEligible: qualification.StructurallyEligible,
		StaticallyEligible:   qualification.StaticallyEligible,
		EligibilityFacts:     qualification.EligibilityFacts,
		ProbeCaps:            s.ProbeCaps,
		Admission:            s.Admission,
		SuffixStartStep:      s.SuffixStartStep,
		SuffixEndStep:        s.SuffixEndStep,
		SuffixLength:         s.SuffixLength,
		PrefixStartStep:      s.PrefixStartStep,
		PrefixEndStep:        s.PrefixEndStep,
		PrefixLength:         s.PrefixLength,
		SeedPredicateClass:   s.SeedPredicateClass,
		EndpointLimit:        s.EndpointLimit,
		StateLimit:           s.Admission.StateLimit,
		HasFinalLimit:        qualification.HasFinalLimit,
		ObservationMode:      qualification.ObservationMode,
		LogicalDirection:     qualification.LogicalDirection,
		MinimumDepth:         qualification.MinimumDepth,
		MaximumDepth:         qualification.MaximumDepth,
		SelectionMode:        qualification.SelectionMode,
		SelectorVersion:      qualification.SelectorVersion,
		FallbackStrategy:     s.Admission.FallbackStrategy,
		FallbackReason:       qualification.FallbackReason,
	}
}

// setExpansionSearchExpectedEmission keeps compile-time emission metadata in
// sync after statement-wide safety and observation checks change selection.
// It describes statement shape only; execution telemetry records the arm that
// actually ran.
func setExpansionSearchExpectedEmission(decision *ExpansionSearchStrategyDecision) {
	decision.EmittedPolicy = ""
	decision.EmittedCandidates = []ExpansionSearchStrategy{decision.SelectedStrategy}
	if decision.SelectedStrategy == ExpansionSearchEndpointSeededReverse && decision.StructurallyEligible {
		decision.EmittedPolicy = ExpansionSearchPolicyEndpointGuardV1
		decision.EmittedCandidates = []ExpansionSearchStrategy{
			ExpansionSearchStepwiseForward,
			ExpansionSearchEndpointSeededReverse,
		}
	}
}
