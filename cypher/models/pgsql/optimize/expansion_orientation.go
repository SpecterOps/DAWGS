package optimize

// contiguousExpansionOrientationCandidate contains the common typed metadata
// for one variable expansion and an adjacent fixed seed region. Prefix- and
// suffix-specific analyzers retain their own correctness facts and fallback
// reasons, then use this type to produce a consistent public decision.
type contiguousExpansionOrientationCandidate struct {
	// Target supplies the target input to the contiguousExpansionOrientationCandidate contract.
	Target TraversalStepTarget
	// Family supplies the family input to the contiguousExpansionOrientationCandidate contract.
	Family string
	// PlannedPolicy identifies the planned policy.
	PlannedPolicy ExpansionSearchPolicy
	// EmittedPolicy identifies the emitted policy.
	EmittedPolicy ExpansionSearchPolicy
	// PlannedCandidates supplies the planned candidates input to the contiguousExpansionOrientationCandidate contract.
	PlannedCandidates []ExpansionSearchStrategy
	// EmittedCandidates supplies the emitted candidates input to the contiguousExpansionOrientationCandidate contract.
	EmittedCandidates []ExpansionSearchStrategy
	// CandidateStrategy supplies the candidate strategy input to the contiguousExpansionOrientationCandidate contract.
	CandidateStrategy ExpansionSearchStrategy
	// ProbeCaps supplies the probe caps input to the contiguousExpansionOrientationCandidate contract.
	ProbeCaps ExpansionSearchProbeCaps
	// Admission supplies the admission input to the contiguousExpansionOrientationCandidate contract.
	Admission ExpansionSearchAdmission
	// PrefixStartStep supplies the prefix start step input to the contiguousExpansionOrientationCandidate contract.
	PrefixStartStep int
	// PrefixEndStep supplies the prefix end step input to the contiguousExpansionOrientationCandidate contract.
	PrefixEndStep int
	// PrefixLength supplies the prefix length input to the contiguousExpansionOrientationCandidate contract.
	PrefixLength int
	// SuffixStartStep supplies the suffix start step input to the contiguousExpansionOrientationCandidate contract.
	SuffixStartStep int
	// SuffixEndStep supplies the suffix end step input to the contiguousExpansionOrientationCandidate contract.
	SuffixEndStep int
	// SuffixLength supplies the suffix length input to the contiguousExpansionOrientationCandidate contract.
	SuffixLength int
	// SeedPredicateClass supplies the seed predicate class input to the contiguousExpansionOrientationCandidate contract.
	SeedPredicateClass string
	// EndpointLimit supplies the endpoint limit input to the contiguousExpansionOrientationCandidate contract.
	EndpointLimit int64
}

// contiguousExpansionOrientationQualification contains analysis results that
// remain specific to the fixed-prefix or fixed-suffix correctness envelope.
type contiguousExpansionOrientationQualification struct {
	// SelectedStrategy supplies the selected strategy input to the contiguousExpansionOrientationQualification contract.
	SelectedStrategy ExpansionSearchStrategy
	// StructurallyEligible indicates whether structurally eligible applies.
	StructurallyEligible bool
	// StaticallyEligible indicates whether statically eligible applies.
	StaticallyEligible bool
	// EligibilityFacts supplies the eligibility facts input to the contiguousExpansionOrientationQualification contract.
	EligibilityFacts []ExpansionSearchEligibilityFact
	// HasFinalLimit indicates whether has final limit applies.
	HasFinalLimit bool
	// ObservationMode identifies the observation mode.
	ObservationMode ExpansionSearchObservationMode
	// LogicalDirection supplies the logical direction input to the contiguousExpansionOrientationQualification contract.
	LogicalDirection string
	// MinimumDepth sets the inclusive lower traversal-depth bound.
	MinimumDepth int64
	// MaximumDepth sets the inclusive upper traversal-depth bound.
	MaximumDepth int64
	// SelectionMode identifies the selection mode.
	SelectionMode string
	// SelectorVersion identifies the schema version for selector version.
	SelectorVersion string
	// FallbackReason supplies the fallback reason input to the contiguousExpansionOrientationQualification contract.
	FallbackReason string
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
	decision.ExecutionBoundary = ExpansionSearchExecutionBoundaryInlineStatement
	if decision.SelectedStrategy == ExpansionSearchEndpointSeededReverse && decision.StructurallyEligible {
		decision.EmittedPolicy = ExpansionSearchPolicyEndpointGuardV1
		decision.EmittedCandidates = []ExpansionSearchStrategy{
			ExpansionSearchStepwiseForward,
			ExpansionSearchEndpointSeededReverse,
		}
		decision.ExecutionBoundary = ExpansionSearchExecutionBoundaryGuardedDualArm
	}
}
