package optimize

import (
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

type sourceTraversalStep struct {
	LeftNode     *cypher.NodePattern
	Relationship *cypher.RelationshipPattern
	RightNode    *cypher.NodePattern
}

type boundSourceSelectivity int

const (
	traversalDirectionReasonRightBound                   = "right_bound"
	traversalDirectionReasonRightConstrained             = "right_constrained"
	traversalDirectionReasonRightPredicate               = "right_predicate"
	traversalDirectionReasonTerminalKindOnlyEstimateWide = "terminal kind-only estimate too broad"
	traversalDirectionReasonBoundSourceSelective         = "bound source estimate selective"

	shortestPathStrategyReasonBoundEndpointPairs = "bound_endpoint_pairs"
	shortestPathStrategyReasonEndpointPredicates = "endpoint_predicates"

	shortestPathFilterReasonTerminalPredicate      = "terminal_predicate"
	shortestPathFilterReasonEndpointPairPredicates = "endpoint_pair_predicates"
)

const (
	boundSourceSelectivityNone boundSourceSelectivity = iota
	boundSourceSelectivityKindOnly
	boundSourceSelectivityPredicate
	boundSourceSelectivityUnique
	boundSourceSelectivityLimited
	boundSourceSelectivityTopN
)

const (
	maxExactRangeExpansionDepth       int64 = 2
	defaultShortestPathExpansionDepth int64 = 15
	defaultShortestPathStateLimit     int64 = 100_000
)

func BuildLoweringPlan(query *cypher.RegularQuery, predicateAttachments []PredicateAttachment) (LoweringPlan, error) {
	if query == nil || query.SingleQuery == nil {
		return LoweringPlan{}, nil
	}

	var plan LoweringPlan

	if query.SingleQuery.MultiPartQuery != nil {
		var (
			carriedSymbols     = map[string]struct{}{}
			carriedSelectivity = map[string]boundSourceSelectivity{}
		)

		for queryPartIndex, part := range query.SingleQuery.MultiPartQuery.Parts {
			if part == nil {
				continue
			}

			if err := appendQueryPartLowerings(&plan, queryPartIndex, part, part.ReadingClauses, predicateAttachments, carriedSymbols, carriedSelectivity); err != nil {
				return LoweringPlan{}, err
			}

			var (
				currentSymbols     = copyStringSet(carriedSymbols)
				currentSelectivity = copyBoundSourceSelectivity(carriedSelectivity)
			)

			declareReadingClauseSymbols(currentSymbols, part.ReadingClauses)
			declareReadingClauseSelectivity(currentSelectivity, part.ReadingClauses)

			if part.With == nil {
				carriedSymbols, carriedSelectivity = currentSymbols, currentSelectivity
			} else {
				carriedSymbols, carriedSelectivity = carryProjectionSelectivity(part.With.Projection, currentSymbols, currentSelectivity)
			}
		}

		if finalPart := query.SingleQuery.MultiPartQuery.SinglePartQuery; finalPart != nil {
			if err := appendQueryPartLowerings(&plan, len(query.SingleQuery.MultiPartQuery.Parts), finalPart, finalPart.ReadingClauses, predicateAttachments, carriedSymbols, carriedSelectivity); err != nil {
				return LoweringPlan{}, err
			}
		}
	} else if singlePart := query.SingleQuery.SinglePartQuery; singlePart != nil {
		if err := appendQueryPartLowerings(&plan, 0, singlePart, singlePart.ReadingClauses, predicateAttachments, nil, nil); err != nil {
			return LoweringPlan{}, err
		}
	}

	appendPredicatePlacementDecisions(&plan, query, predicateAttachments)
	attachPredicatePlacementsToSuffixPushdowns(&plan)
	appendCountStoreFastPathDecisions(&plan, query)
	appendAggregateTraversalCountDecisions(&plan, query)
	finalizeShortestPathExecutorDecisions(&plan, query)
	finalizeExpansionSearchStrategyDecisions(&plan, query)
	return plan, nil
}

func appendQueryPartLowerings(
	plan *LoweringPlan,
	queryPartIndex int,
	queryPart cypher.SyntaxNode,
	readingClauses []*cypher.ReadingClause,
	predicateAttachments []PredicateAttachment,
	initialDeclaredSymbols map[string]struct{},
	initialSelectivity map[string]boundSourceSelectivity,
) error {
	sourceReferences, err := collectReferencedSourceIdentifiers(queryPart)
	if err != nil {
		return err
	}

	appendExactRangeExpansionDecisions(plan, queryPartIndex, readingClauses)
	appendPatternPredicateExactRangeExpansionDecisions(plan, queryPartIndex, queryPart)
	appendPathRelationshipPredicateDecisions(plan, queryPartIndex, queryPart)
	appendProjectionPruningDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendLatePathMaterializationDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendPatternPredicateProjectionLowerings(plan, queryPartIndex, queryPart, sourceReferences)
	appendPatternPredicatePlacementDecisions(plan, queryPartIndex, queryPart)
	appendExpandIntoDecisions(plan, queryPartIndex, readingClauses)
	appendTraversalDirectionDecisions(plan, queryPartIndex, readingClauses, bindingPredicateSymbols(predicateAttachments, queryPartIndex), initialDeclaredSymbols, initialSelectivity)
	shortestPathSearchSymbols := shortestPathSearchPredicateSymbols(readingClauses)
	appendShortestPathStrategyDecisions(plan, queryPartIndex, readingClauses, shortestPathSearchSymbols)
	appendShortestPathFilterDecisions(plan, queryPartIndex, readingClauses, shortestPathSearchSymbols)
	appendShortestPathExecutorDecisions(plan, queryPartIndex, queryPart, readingClauses, sourceReferences)
	appendLimitPushdownDecisions(plan, queryPartIndex, queryPart, readingClauses)
	appendExpansionSuffixPushdownDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendExpansionSearchStrategyDecisions(plan, queryPartIndex, queryPart, readingClauses, sourceReferences, initialDeclaredSymbols)
	fieldRequirements, err := collectFieldRequirements(queryPartIndex, queryPart)
	if err != nil {
		return err
	}
	plan.FieldRequirements = append(plan.FieldRequirements, fieldRequirements...)
	applyShortestPathObservationModes(plan, queryPartIndex, readingClauses, fieldRequirements)
	applyExpansionSearchObservationModes(plan, queryPartIndex, readingClauses, fieldRequirements)
	return nil
}

func appendExpansionSearchStrategyDecisions(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode, readingClauses []*cypher.ReadingClause, sourceReferences map[string]struct{}, initialDeclaredSymbols map[string]struct{}) {
	_, updatingClauses := queryPartProjection(queryPart)
	declaredSymbols := copyStringSet(initialDeclaredSymbols)
	queryPartVariableExpansions := 0
	for _, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}
		for _, patternPart := range readingClause.Match.Pattern {
			for _, step := range traversalStepsForPattern(patternPart) {
				if step.Relationship != nil && step.Relationship.Range != nil {
					queryPartVariableExpansions++
				}
			}
		}
	}
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}
		for patternIndex, patternPart := range readingClause.Match.Pattern {
			steps := traversalStepsForPattern(patternPart)
			deterministicPredicates := !syntaxContainsFunctionInvocation(patternPart) && !syntaxContainsFunctionInvocation(readingClause.Match.Where)
			pathDependentPredicate := patternPart != nil && patternPart.Variable != nil && syntaxDependsOn(readingClause.Match.Where, patternPart.Variable.Symbol)
			for stepIndex, step := range steps {
				if step.Relationship == nil || step.Relationship.Range == nil {
					continue
				}
				target := PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}.TraversalStep(stepIndex)
				limitConflict := hasLimitPushdownForTarget(plan, target)
				suffixLength := fixedSuffixLength(steps[stepIndex+1:])
				suffixEnd := stepIndex + suffixLength
				minDepth := int64(1)
				if step.Relationship.Range.StartIndex != nil {
					minDepth = *step.Relationship.Range.StartIndex
				}
				maxDepth := int64(0)
				boundedDepth := step.Relationship.Range.EndIndex != nil
				if boundedDepth {
					maxDepth = *step.Relationship.Range.EndIndex
				}
				directedExpansion := step.Relationship.Direction != graph.DirectionBoth
				directedSuffix := suffixLength > 0
				noSuffixRelationshipVariables := true
				noRelationshipPredicates := step.Relationship.Properties == nil && !syntaxDependsOn(readingClause.Match.Where, variableSymbol(step.Relationship.Variable))
				suffixSteps := steps[stepIndex+1 : stepIndex+1+suffixLength]
				uncorrelatedSuffix := true
				for _, suffixStep := range suffixSteps {
					directedSuffix = directedSuffix && suffixStep.Relationship.Direction != graph.DirectionBoth
					noSuffixRelationshipVariables = noSuffixRelationshipVariables && suffixStep.Relationship.Variable == nil
					noRelationshipPredicates = noRelationshipPredicates && suffixStep.Relationship.Properties == nil && !syntaxDependsOn(readingClause.Match.Where, variableSymbol(suffixStep.Relationship.Variable))
					uncorrelatedSuffix = uncorrelatedSuffix && !symbolDeclared(declaredSymbols, variableSymbol(suffixStep.Relationship.Variable)) && !symbolDeclared(declaredSymbols, variableSymbol(suffixStep.RightNode.Variable))
				}
				noCrossRegionPredicate := !hasCrossRegionPredicate(readingClause.Match.Where, step, suffixSteps)
				boundRoot := symbolDeclared(declaredSymbols, variableSymbol(step.LeftNode.Variable))
				observation := ExpansionSearchObservationEndpointIDs
				if patternPart != nil && patternPart.Variable != nil && referencesSourceIdentifier(sourceReferences, patternPart.Variable.Symbol) {
					observation = ExpansionSearchObservationFullPath
				}
				facts := []ExpansionSearchEligibilityFact{
					{
						Name:     "read_only",
						Eligible: updatingClauses == 0,
					},
					{
						Name:     "non_optional",
						Eligible: !readingClause.Match.Optional,
					},
					{
						Name:     "ordinary_path",
						Eligible: patternPart != nil && !patternPart.ShortestPathPattern && !patternPart.AllShortestPathsPattern,
					},
					{
						Name:     "single_variable_expansion",
						Eligible: queryPartVariableExpansions == 1,
					},
					{
						Name:     "bound_root",
						Eligible: boundRoot,
					},
					{
						Name:     "directed_expansion",
						Eligible: directedExpansion,
					},
					{
						Name:     "bounded_supported_depth",
						Eligible: boundedDepth && maxDepth >= minDepth && maxDepth <= 64,
					},
					{
						Name:     "exact_three_hop_suffix",
						Eligible: suffixLength == 3,
					},
					{
						Name:     "qualified_fixed_suffix_topology",
						Eligible: qualifiedFixedSuffixTopology(step, suffixSteps),
					},
					{
						Name:     "directed_suffix",
						Eligible: directedSuffix,
					},
					{
						Name:     "no_relationship_variable",
						Eligible: step.Relationship.Variable == nil && noSuffixRelationshipVariables,
					},
					{
						Name:     "no_relationship_predicate",
						Eligible: noRelationshipPredicates,
					},
					{
						Name:     "uncorrelated_suffix",
						Eligible: uncorrelatedSuffix,
					},
					{
						Name:     "no_cross_region_predicate",
						Eligible: noCrossRegionPredicate,
					},
					{
						Name:     "no_path_dependent_predicate",
						Eligible: !pathDependentPredicate,
					},
					{
						Name:     "deterministic_predicates",
						Eligible: deterministicPredicates,
					},
					{
						Name:     "no_limit_pushdown_conflict",
						Eligible: !limitConflict,
					},
					{
						Name:     "supported_observation",
						Eligible: observation != ExpansionSearchObservationUnsupported,
					},
				}
				eligible := true
				for _, fact := range facts {
					eligible = eligible && fact.Eligible
				}
				fallbackReason := ExpansionSearchFallbackTournamentUnqualified
				switch {
				case updatingClauses > 0:
					fallbackReason = ExpansionSearchFallbackMutation
				case readingClause.Match.Optional:
					fallbackReason = ExpansionSearchFallbackOptionalMatch
				case patternPart != nil && patternPart.AllShortestPathsPattern:
					fallbackReason = ExpansionSearchFallbackAllShortestPaths
				case patternPart != nil && patternPart.ShortestPathPattern:
					fallbackReason = ExpansionSearchFallbackShortestPath
				case queryPartVariableExpansions > 1:
					fallbackReason = ExpansionSearchFallbackMultipleVariableExpansions
				case !directedExpansion:
					fallbackReason = ExpansionSearchFallbackDirectionlessExpansion
				case !boundedDepth:
					fallbackReason = ExpansionSearchFallbackUnboundedDepth
				case maxDepth < minDepth || maxDepth > 64:
					fallbackReason = ExpansionSearchFallbackUnsupportedDepth
				case suffixLength == 0:
					fallbackReason = ExpansionSearchFallbackNoFixedSuffix
				case suffixLength < 3:
					fallbackReason = ExpansionSearchFallbackSuffixTooShort
				case suffixLength != 3:
					fallbackReason = ExpansionSearchFallbackTournamentUnqualified
				case !directedSuffix:
					fallbackReason = ExpansionSearchFallbackDirectionlessSuffix
				case !noRelationshipPredicates:
					fallbackReason = ExpansionSearchFallbackRelationshipPredicate
				case !uncorrelatedSuffix:
					fallbackReason = ExpansionSearchFallbackCorrelatedSuffix
				case !noCrossRegionPredicate:
					fallbackReason = ExpansionSearchFallbackCrossRegionPredicate
				case step.Relationship.Variable != nil || !noSuffixRelationshipVariables:
					fallbackReason = ExpansionSearchFallbackRelationshipVariable
				case pathDependentPredicate:
					fallbackReason = ExpansionSearchFallbackPathDependentPredicate
				case !deterministicPredicates:
					fallbackReason = ExpansionSearchFallbackNonDeterministicPredicate
				case limitConflict:
					fallbackReason = ExpansionSearchFallbackLimitPushdownConflict
				case !boundRoot && qualifiedFixedSuffixTopology(step, suffixSteps):
					fallbackReason = ExpansionSearchFallbackUnboundRoot
				}
				plan.ExpansionSearchStrategy = append(plan.ExpansionSearchStrategy, ExpansionSearchStrategyDecision{
					Target: target,
					Family: "fixed_suffix_expansion",
					PlannedCandidates: []ExpansionSearchStrategy{
						ExpansionSearchStepwiseForward,
						ExpansionSearchLateHydratedForward,
						ExpansionSearchFactoredSuffixForward,
						ExpansionSearchSuffixSeededReverse,
						ExpansionSearchBackwardViabilityForward,
					},
					CandidateStrategy:    ExpansionSearchSuffixSeededReverse,
					SelectedStrategy:     ExpansionSearchStepwiseForward,
					StructurallyEligible: eligible,
					StaticallyEligible:   eligible,
					EligibilityFacts:     facts,
					SuffixStartStep:      stepIndex + 1,
					SuffixEndStep:        suffixEnd,
					SuffixLength:         suffixLength,
					ObservationMode:      observation,
					LogicalDirection:     step.Relationship.Direction.String(),
					MinimumDepth:         minDepth,
					MaximumDepth:         maxDepth,
					SelectionMode:        "incumbent_default",
					SelectorVersion:      "fixed-suffix-static-v1",
					FallbackStrategy:     ExpansionSearchStepwiseForward,
					FallbackReason:       fallbackReason,
				})
			}
			declarePatternSymbols(declaredSymbols, patternPart)
		}
		declareWhereSymbols(declaredSymbols, readingClause.Match)
	}
}

func syntaxContainsFunctionInvocation(node cypher.SyntaxNode) bool {
	if node == nil {
		return false
	}
	found := false
	_ = walk.Cypher(node, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if _, isFunction := node.(*cypher.FunctionInvocation); isFunction {
			found = true
		}
	}))
	return found
}

func symbolDeclared(declared map[string]struct{}, symbol string) bool {
	if symbol == "" {
		return false
	}
	_, found := declared[symbol]
	return found
}

func hasCrossRegionPredicate(where *cypher.Where, expansion sourceTraversalStep, suffix []sourceTraversalStep) bool {
	if where == nil {
		return false
	}
	prefixSymbols := map[string]struct{}{}
	suffixSymbols := map[string]struct{}{}
	addSymbol(prefixSymbols, variableSymbol(expansion.LeftNode.Variable))
	addSymbol(prefixSymbols, variableSymbol(expansion.Relationship.Variable))
	addSymbol(prefixSymbols, variableSymbol(expansion.RightNode.Variable))
	for _, step := range suffix {
		addSymbol(suffixSymbols, variableSymbol(step.Relationship.Variable))
		addSymbol(suffixSymbols, variableSymbol(step.RightNode.Variable))
	}
	for _, expression := range where.Expressions {
		var hasPrefix, hasSuffix bool
		for _, dependency := range sortedDependencies(expression) {
			if _, found := prefixSymbols[dependency]; found {
				hasPrefix = true
			}
			if _, found := suffixSymbols[dependency]; found {
				hasSuffix = true
			}
		}
		if hasPrefix && hasSuffix {
			return true
		}
	}
	return false
}

func fixedSuffixLength(steps []sourceTraversalStep) int {
	length := 0
	for _, step := range steps {
		if step.Relationship == nil || step.Relationship.Range != nil {
			break
		}
		length++
	}
	return length
}

func hasLimitPushdownForTarget(plan *LoweringPlan, target TraversalStepTarget) bool {
	for _, decision := range plan.LimitPushdown {
		if decision.Target == target {
			return true
		}
	}
	return false
}

func qualifiedFixedSuffixTopology(expansion sourceTraversalStep, suffix []sourceTraversalStep) bool {
	if len(suffix) != 3 || expansion.Relationship == nil || len(expansion.Relationship.Kinds) != 1 || expansion.Relationship.Direction != graph.DirectionOutbound {
		return false
	}
	for _, step := range suffix {
		if step.Relationship == nil || step.RightNode == nil || step.Relationship.Direction != graph.DirectionOutbound || len(step.Relationship.Kinds) != 1 || len(step.RightNode.Kinds) != 1 {
			return false
		}
	}
	return true
}

func applyExpansionSearchObservationModes(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, requirements []FieldRequirementDecision) {
	externalFieldsBySymbol := map[string]map[FieldRequirement]struct{}{}
	for _, requirement := range requirements {
		fields := map[FieldRequirement]struct{}{}
		for _, use := range requirement.Uses {
			if use.Internal {
				continue
			}
			for _, field := range use.Fields {
				fields[field] = struct{}{}
			}
		}
		externalFieldsBySymbol[requirement.Symbol] = fields
	}
	for idx := range plan.ExpansionSearchStrategy {
		decision := &plan.ExpansionSearchStrategy[idx]
		if decision.Target.QueryPartIndex != queryPartIndex || decision.Target.Predicate || decision.Target.ClauseIndex >= len(readingClauses) {
			continue
		}
		clause := readingClauses[decision.Target.ClauseIndex]
		if clause == nil || clause.Match == nil || decision.Target.PatternIndex >= len(clause.Match.Pattern) {
			continue
		}
		pattern := clause.Match.Pattern[decision.Target.PatternIndex]
		if pattern == nil || pattern.Variable == nil {
			decision.ObservationMode = ExpansionSearchObservationEndpointIDs
			setExpansionSearchEligibilityFact(decision, "supported_observation", true)
			continue
		}
		fields := externalFieldsBySymbol[pattern.Variable.Symbol]
		switch {
		case hasFieldRequirement(fields, FieldRequirementFullPath):
			decision.ObservationMode = ExpansionSearchObservationFullPath
		case hasFieldRequirement(fields, FieldRequirementOrderedPathEdgeIDs), hasFieldRequirement(fields, FieldRequirementRelationshipIDs):
			decision.ObservationMode = ExpansionSearchObservationOrderedPathIDs
		case hasFieldRequirement(fields, FieldRequirementFullEntity):
			decision.ObservationMode = ExpansionSearchObservationFullPath
		case len(fields) == 0:
			decision.ObservationMode = ExpansionSearchObservationEndpointIDs
		default:
			decision.ObservationMode = ExpansionSearchObservationUnsupported
		}
		supported := decision.ObservationMode != ExpansionSearchObservationUnsupported
		setExpansionSearchEligibilityFact(decision, "supported_observation", supported)
		if !supported {
			decision.StructurallyEligible = false
			decision.FallbackReason = ExpansionSearchFallbackUnsupportedObservation
		}
	}
}

func hasFieldRequirement(fields map[FieldRequirement]struct{}, field FieldRequirement) bool {
	_, found := fields[field]
	return found
}

func setExpansionSearchEligibilityFact(decision *ExpansionSearchStrategyDecision, name string, eligible bool) {
	for idx := range decision.EligibilityFacts {
		if decision.EligibilityFacts[idx].Name == name {
			decision.EligibilityFacts[idx].Eligible = eligible
			return
		}
	}
}

func expansionSearchFactsEligible(facts []ExpansionSearchEligibilityFact) bool {
	for _, fact := range facts {
		if !fact.Eligible {
			return false
		}
	}
	return true
}

func applyShortestPathObservationModes(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, requirements []FieldRequirementDecision) {
	fieldsBySymbol := map[string]map[FieldRequirement]struct{}{}
	for _, requirement := range requirements {
		fields := map[FieldRequirement]struct{}{}
		for _, field := range requirement.Fields {
			fields[field] = struct{}{}
		}
		fieldsBySymbol[requirement.Symbol] = fields
	}
	for idx := range plan.ShortestPathExecutor {
		decision := &plan.ShortestPathExecutor[idx]
		if decision.Target.QueryPartIndex != queryPartIndex || decision.Target.Predicate {
			continue
		}
		if decision.Target.ClauseIndex >= len(readingClauses) {
			continue
		}
		clause := readingClauses[decision.Target.ClauseIndex]
		if clause == nil || clause.Match == nil || decision.Target.PatternIndex >= len(clause.Match.Pattern) {
			continue
		}
		pattern := clause.Match.Pattern[decision.Target.PatternIndex]
		if pattern == nil || pattern.Variable == nil {
			continue
		}
		fields := fieldsBySymbol[pattern.Variable.Symbol]
		if pattern.AllShortestPathsPattern {
			if _, fullPath := fields[FieldRequirementFullPath]; fullPath {
				decision.ObservationMode = ShortestPathObservationAllPaths
			} else if _, orderedIDs := fields[FieldRequirementOrderedPathEdgeIDs]; orderedIDs {
				decision.ObservationMode = ShortestPathObservationAllPaths
			}
		} else if _, fullPath := fields[FieldRequirementFullPath]; fullPath {
			decision.ObservationMode = ShortestPathObservationOnePath
		} else if _, orderedIDs := fields[FieldRequirementOrderedPathEdgeIDs]; orderedIDs {
			decision.ObservationMode = ShortestPathObservationDistance
		}
		setShortestPathEligibilityFact(decision, "known_observation_mode", decision.ObservationMode != ShortestPathObservationUnknown)
	}
}

func appendShortestPathExecutorDecisions(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode, readingClauses []*cypher.ReadingClause, sourceReferences map[string]struct{}) {
	var (
		shortestCalls  int
		patternSources int
		hasUnwind      bool
	)
	for _, readingClause := range readingClauses {
		if readingClause == nil {
			continue
		}
		if readingClause.Unwind != nil {
			hasUnwind = true
		}
		if readingClause.Match == nil {
			continue
		}
		patternSources += len(readingClause.Match.Pattern)
		for _, patternPart := range readingClause.Match.Pattern {
			if patternPart != nil && (patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern) {
				shortestCalls++
			}
		}
	}
	_, updatingClauses := queryPartProjection(queryPart)
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}
		for patternIndex, patternPart := range readingClause.Match.Pattern {
			if patternPart == nil || (!patternPart.ShortestPathPattern && !patternPart.AllShortestPathsPattern) {
				continue
			}
			steps := traversalStepsForPattern(patternPart)
			idEqualities := singletonIDEqualityCounts(readingClause.Match.Where)
			pathPredicate := syntaxDependsOn(readingClause.Match.Where, variableSymbol(patternPart.Variable))
			for stepIndex, step := range steps {
				if step.Relationship == nil || step.Relationship.Range == nil {
					continue
				}
				minDepth := int64(1)
				if step.Relationship.Range.StartIndex != nil {
					minDepth = *step.Relationship.Range.StartIndex
				}
				maxDepth := defaultShortestPathExpansionDepth
				boundedDepth := step.Relationship.Range.EndIndex != nil
				if boundedDepth {
					maxDepth = *step.Relationship.Range.EndIndex
				}
				supportedDepth := (boundedDepth || patternPart.AllShortestPathsPattern) && (minDepth == 0 || minDepth == 1) && maxDepth >= minDepth && maxDepth <= 64
				directionSupported := step.Relationship.Direction != graph.DirectionBoth
				relationshipVariableObserved := step.Relationship.Variable != nil && referencesSourceIdentifier(sourceReferences, step.Relationship.Variable.Symbol)
				noRelationshipVariable := step.Relationship.Variable == nil || (patternPart.AllShortestPathsPattern && !relationshipVariableObserved)
				leftIDCount := idEqualities[variableSymbol(step.LeftNode.Variable)]
				rightIDCount := idEqualities[variableSymbol(step.RightNode.Variable)]
				singletonIDs := leftIDCount == 1 && rightIDCount == 1
				uncorrelatedSource := queryPartIndex == 0 && !hasUnwind
				singleEndpointPair := patternSources == 1
				physicalExpansion := ShortestPathPhysicalExpansionStartID
				topologyClassification := ShortestPathTopologyPhysicalOutbound
				if step.Relationship.Direction == graph.DirectionInbound {
					physicalExpansion = ShortestPathPhysicalExpansionEndID
					if maxDepth <= 1 {
						topologyClassification = ShortestPathTopologyPhysicalInboundShallow
					} else {
						topologyClassification = ShortestPathTopologyPhysicalInboundDeep
					}
				} else if step.Relationship.Direction == graph.DirectionBoth {
					topologyClassification = ShortestPathTopologyDirectionless
				}
				facts := []ShortestPathEligibilityFact{
					{
						Name:     "supported_shortest_path_mode",
						Eligible: patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern,
					},
					{
						Name:     "single_three_element_traversal",
						Eligible: len(patternPart.PatternElements) == 3 && len(steps) == 1,
					},
					{
						Name:     "non_optional",
						Eligible: !readingClause.Match.Optional,
					},
					{
						Name:     "directed",
						Eligible: directionSupported,
					},
					{
						Name:     "bounded_supported_depth",
						Eligible: supportedDepth,
					},
					{
						Name:     "no_relationship_variable",
						Eligible: noRelationshipVariable,
					},
					{
						Name:     "no_relationship_predicate",
						Eligible: step.Relationship.Properties == nil,
					},
					{
						Name:     "single_path_call",
						Eligible: shortestCalls == 1,
					},
					{
						Name:     "read_only",
						Eligible: updatingClauses == 0,
					},
					{
						Name:     "one_static_id_equality_per_endpoint",
						Eligible: singletonIDs,
					},
					{
						Name:     "no_path_predicate",
						Eligible: !pathPredicate,
					},
					{
						Name:     "uncorrelated_endpoint_source",
						Eligible: uncorrelatedSource,
					},
					{
						Name:     "single_endpoint_pair",
						Eligible: singleEndpointPair,
					},
					{
						Name:     "known_observation_mode",
						Eligible: false,
					},
				}
				reason := ShortestPathFallbackTournamentUnqualified
				switch {
				case patternPart.AllShortestPathsPattern && !singletonIDs:
					reason = ShortestPathFallbackAllShortestPaths
				case readingClause.Match.Optional:
					reason = ShortestPathFallbackOptionalMatch
				case !directionSupported:
					reason = ShortestPathFallbackDirectionless
				case pathPredicate:
					reason = ShortestPathFallbackPathPredicate
				case !noRelationshipVariable:
					reason = ShortestPathFallbackRelationshipVariable
				case step.Relationship.Properties != nil:
					reason = ShortestPathFallbackRelationshipPredicate
				case !supportedDepth:
					reason = ShortestPathFallbackUnsupportedDepth
				case shortestCalls != 1:
					reason = ShortestPathFallbackMultiplePathCalls
				case updatingClauses != 0:
					reason = ShortestPathFallbackMutation
				case !uncorrelatedSource:
					reason = ShortestPathFallbackCorrelatedEndpoints
				case !singleEndpointPair:
					reason = ShortestPathFallbackMultipleEndpointPairs
				case leftIDCount > 1 || rightIDCount > 1:
					reason = ShortestPathFallbackMultipleIDEqualities
				case !singletonIDs:
					reason = ShortestPathFallbackNonSingletonID
				}
				family := "SP"
				plannedCandidates := []ShortestPathExecutor{ShortestPathExecutorIncumbentWorkspace, ShortestPathExecutorS0Direct, ShortestPathExecutorS1ArrayBFS, ShortestPathExecutorS2TraceRelation, ShortestPathExecutorS3Unidirectional, ShortestPathExecutorS3EdgeM0, ShortestPathExecutorS4CanonicalDistance, ShortestPathExecutorS4CanonicalWitness}
				if patternPart.AllShortestPathsPattern {
					family = "ASP"
					plannedCandidates = []ShortestPathExecutor{ShortestPathExecutorIncumbentWorkspace, ShortestPathExecutorASPA1DAG}
				}
				plan.ShortestPathExecutor = append(plan.ShortestPathExecutor, ShortestPathExecutorDecision{
					Target: PatternTarget{
						QueryPartIndex: queryPartIndex,
						ClauseIndex:    clauseIndex,
						PatternIndex:   patternIndex,
					}.TraversalStep(stepIndex),
					Family:                 family,
					PlannedCandidates:      plannedCandidates,
					SelectedExecutor:       ShortestPathExecutorIncumbentWorkspace,
					ObservationMode:        ShortestPathObservationUnknown,
					Direction:              step.Relationship.Direction,
					PhysicalExpansion:      physicalExpansion,
					RelationshipKindCount:  len(step.Relationship.Kinds),
					UntypedRelationship:    len(step.Relationship.Kinds) == 0,
					TopologyClassification: topologyClassification,
					Eligibility:            facts,
					StructurallyEligible:   shortestPathFactsEligible(facts),
					StaticallyEligible:     false,
					MinimumDepth:           minDepth,
					MaximumDepth:           maxDepth,
					StateLimit:             defaultShortestPathStateLimit,
					SelectorVersion:        "sp-static-v3",
					SelectionMode:          "incumbent_default",
					FallbackExecutor:       ShortestPathExecutorIncumbentWorkspace,
					FallbackReason:         reason,
				})
			}
		}
	}
}

func shortestPathFactsEligible(facts []ShortestPathEligibilityFact) bool {
	for _, fact := range facts {
		if !fact.Eligible {
			return false
		}
	}
	return true
}

func setShortestPathEligibilityFact(decision *ShortestPathExecutorDecision, name string, eligible bool) {
	for idx := range decision.Eligibility {
		if decision.Eligibility[idx].Name == name {
			decision.Eligibility[idx].Eligible = eligible
			return
		}
	}
	decision.Eligibility = append(decision.Eligibility, ShortestPathEligibilityFact{
		Name:     name,
		Eligible: eligible,
	})
}

// finalizeShortestPathExecutorDecisions applies statement-wide safety facts
// after every query part has been analyzed. Per-part counting can otherwise
// misclassify two shortest calls separated by WITH, or a shortest read followed
// by a mutation, as eligible singleton read-only execution.
func finalizeShortestPathExecutorDecisions(plan *LoweringPlan, query *cypher.RegularQuery) {
	if plan == nil || query == nil || query.SingleQuery == nil {
		return
	}

	var (
		shortestCalls   int
		updatingClauses int
	)
	visitPart := func(part cypher.SyntaxNode, readingClauses []*cypher.ReadingClause) {
		for _, readingClause := range readingClauses {
			if readingClause == nil || readingClause.Match == nil {
				continue
			}
			for _, patternPart := range readingClause.Match.Pattern {
				if patternPart != nil && (patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern) {
					shortestCalls++
				}
			}
		}
		_, partUpdatingClauses := queryPartProjection(part)
		updatingClauses += partUpdatingClauses
	}

	if multiPart := query.SingleQuery.MultiPartQuery; multiPart != nil {
		for _, part := range multiPart.Parts {
			if part != nil {
				visitPart(part, part.ReadingClauses)
			}
		}
		if finalPart := multiPart.SinglePartQuery; finalPart != nil {
			visitPart(finalPart, finalPart.ReadingClauses)
		}
	} else if singlePart := query.SingleQuery.SinglePartQuery; singlePart != nil {
		visitPart(singlePart, singlePart.ReadingClauses)
	}

	for idx := range plan.ShortestPathExecutor {
		decision := &plan.ShortestPathExecutor[idx]
		singlePathCall := shortestCalls == 1
		readOnly := updatingClauses == 0
		setShortestPathEligibilityFact(decision, "single_path_call", singlePathCall)
		setShortestPathEligibilityFact(decision, "read_only", readOnly)
		structurallyEligible := shortestPathFactsEligible(decision.Eligibility)
		qualifiedPhysicalDepth := decision.Direction != graph.DirectionInbound || decision.MaximumDepth <= 1
		qualifiedPathKinds := decision.ObservationMode != ShortestPathObservationOnePath || (!decision.UntypedRelationship && decision.RelationshipKindCount == 1)
		setShortestPathEligibilityFact(decision, "qualified_physical_expansion_depth", qualifiedPhysicalDepth)
		setShortestPathEligibilityFact(decision, "qualified_one_path_kind_state", qualifiedPathKinds)
		decision.StructurallyEligible = structurallyEligible
		decision.StaticallyEligible = structurallyEligible && qualifiedPhysicalDepth && qualifiedPathKinds

		if !singlePathCall && (decision.FallbackReason == ShortestPathFallbackTournamentUnqualified || decision.FallbackReason == ShortestPathFallbackCorrelatedEndpoints) {
			decision.FallbackReason = ShortestPathFallbackMultiplePathCalls
		} else if !readOnly && decision.FallbackReason == ShortestPathFallbackTournamentUnqualified {
			decision.FallbackReason = ShortestPathFallbackMutation
		}

		if structurallyEligible && decision.ObservationMode == ShortestPathObservationAllPaths {
			// The compact all-shortest search is deliberately narrower than the
			// singleton witness executors. Minimum-depth zero and self-endpoint
			// searches can require cyclic relationship-simple paths, which cannot
			// use a minimum-node-depth predecessor DAG without changing semantics.
			if decision.MinimumDepth != 1 {
				decision.FallbackReason = ShortestPathFallbackUnsupportedDepth
				continue
			}
			decision.SelectedExecutor = ShortestPathExecutorASPA1DAG
			decision.StaticallyEligible = true
			decision.SelectionMode = "static"
			decision.SelectorVersion = "asp-static-v1"
			decision.FallbackReason = ""
			decision.ExperimentalWinner = true
			continue
		}

		if structurallyEligible {
			if !qualifiedPhysicalDepth {
				switch decision.ObservationMode {
				case ShortestPathObservationDistance:
					decision.SelectedExecutor = ShortestPathExecutorS4CanonicalDistance
				case ShortestPathObservationOnePath:
					decision.SelectedExecutor = ShortestPathExecutorS4CanonicalWitness
				default:
					decision.FallbackReason = ShortestPathFallbackDeepInboundUnqualified
					continue
				}
				decision.SelectionMode = "static"
				decision.SelectorVersion = "sp-static-v4"
				decision.StaticallyEligible = true
				decision.FallbackReason = ""
				decision.ExperimentalWinner = true
				continue
			}
			if !qualifiedPathKinds {
				if decision.ObservationMode == ShortestPathObservationOnePath {
					decision.SelectedExecutor = ShortestPathExecutorS4CanonicalWitness
					decision.SelectionMode = "static"
					decision.SelectorVersion = "sp-static-v4"
					decision.StaticallyEligible = true
					decision.FallbackReason = ""
					decision.ExperimentalWinner = true
					continue
				}
				decision.FallbackReason = ShortestPathFallbackNonSingleKindPathState
				continue
			}
			switch decision.ObservationMode {
			case ShortestPathObservationDistance:
				decision.SelectedExecutor = ShortestPathExecutorS3Unidirectional
			case ShortestPathObservationOnePath:
				decision.SelectedExecutor = ShortestPathExecutorS3EdgeM0
			default:
				continue
			}
			decision.SelectionMode = "static"
			decision.SelectorVersion = "sp-static-v3"
			decision.FallbackReason = ""
			decision.ExperimentalWinner = true
		}
	}
}

// finalizeExpansionSearchStrategyDecisions applies statement-wide safety
// facts after all query parts and field requirements are known. A compound
// region must never be selected from a per-clause view that misses another
// variable expansion or a later mutation across WITH boundaries.
func finalizeExpansionSearchStrategyDecisions(plan *LoweringPlan, query *cypher.RegularQuery) {
	if plan == nil || query == nil || query.SingleQuery == nil {
		return
	}
	var variableExpansions, updatingClauses int
	visitPart := func(part cypher.SyntaxNode, readingClauses []*cypher.ReadingClause) {
		for _, readingClause := range readingClauses {
			if readingClause == nil || readingClause.Match == nil {
				continue
			}
			for _, patternPart := range readingClause.Match.Pattern {
				for _, step := range traversalStepsForPattern(patternPart) {
					if step.Relationship != nil && step.Relationship.Range != nil {
						variableExpansions++
					}
				}
			}
		}
		_, partUpdatingClauses := queryPartProjection(part)
		updatingClauses += partUpdatingClauses
	}
	if multiPart := query.SingleQuery.MultiPartQuery; multiPart != nil {
		for _, part := range multiPart.Parts {
			if part != nil {
				visitPart(part, part.ReadingClauses)
			}
		}
		if finalPart := multiPart.SinglePartQuery; finalPart != nil {
			visitPart(finalPart, finalPart.ReadingClauses)
		}
	} else if singlePart := query.SingleQuery.SinglePartQuery; singlePart != nil {
		visitPart(singlePart, singlePart.ReadingClauses)
	}

	for idx := range plan.ExpansionSearchStrategy {
		decision := &plan.ExpansionSearchStrategy[idx]
		singleExpansion := variableExpansions == 1
		readOnly := updatingClauses == 0
		setExpansionSearchEligibilityFact(decision, "single_variable_expansion", singleExpansion)
		setExpansionSearchEligibilityFact(decision, "read_only", readOnly)
		decision.StructurallyEligible = expansionSearchFactsEligible(decision.EligibilityFacts)
		decision.StaticallyEligible = decision.StructurallyEligible
		if !singleExpansion && (decision.FallbackReason == ExpansionSearchFallbackTournamentUnqualified || decision.FallbackReason == ExpansionSearchFallbackMultipleVariableExpansions || decision.FallbackReason == ExpansionSearchFallbackUnboundRoot) {
			decision.FallbackReason = ExpansionSearchFallbackMultipleVariableExpansions
		} else if !readOnly && decision.FallbackReason == ExpansionSearchFallbackTournamentUnqualified {
			decision.FallbackReason = ExpansionSearchFallbackMutation
		}
	}
}

func syntaxDependsOn(node cypher.SyntaxNode, symbol string) bool {
	if symbol == "" {
		return false
	}
	for _, dependency := range sortedDependencies(node) {
		if dependency == symbol {
			return true
		}
	}
	return false
}

func singletonIDEqualityCounts(where *cypher.Where) map[string]int {
	counts := map[string]int{}
	if where == nil {
		return counts
	}
	for _, expression := range where.Expressions {
		for _, term := range cypherConjunctionTerms(expression) {
			comparison, ok := term.(*cypher.Comparison)
			if !ok || comparison == nil || len(comparison.Partials) != 1 || comparison.Partials[0].Operator != cypher.OperatorEquals {
				continue
			}
			partial := comparison.Partials[0]
			if symbol, ok := identityFunctionSymbol(comparison.Left); ok && expressionIsConstant(partial.Right) {
				counts[symbol]++
			}
			if symbol, ok := identityFunctionSymbol(partial.Right); ok && expressionIsConstant(comparison.Left) {
				counts[symbol]++
			}
		}
	}
	return counts
}

func identityFunctionSymbol(expression cypher.Expression) (string, bool) {
	function, ok := expression.(*cypher.FunctionInvocation)
	if !ok || function == nil || !strings.EqualFold(function.Name, cypher.IdentityFunction) || len(function.Arguments) != 1 {
		return "", false
	}
	variable, ok := function.Arguments[0].(*cypher.Variable)
	if !ok || variable == nil || variable.Symbol == "" {
		return "", false
	}
	return variable.Symbol, true
}

func appendExactRangeExpansionDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause) {
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Optional {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			appendPatternExactRangeExpansionDecisions(plan, PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}, patternPart)
		}
	}
}

func appendPatternPredicateExactRangeExpansionDecisions(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode) {
	for _, indexedPredicate := range indexedPatternPredicatesInQueryPart(queryPart) {
		patternPart := patternPartForPredicate(indexedPredicate.Predicate)
		appendPatternExactRangeExpansionDecisions(plan, PatternTarget{
			QueryPartIndex: queryPartIndex,
			ClauseIndex:    indexedPredicate.ClauseIndex,
			PatternIndex:   indexedPredicate.PredicateIndex,
			Predicate:      true,
			PredicateIndex: indexedPredicate.PredicateIndex,
		}, patternPart)
	}
}

func appendPatternExactRangeExpansionDecisions(plan *LoweringPlan, target PatternTarget, patternPart *cypher.PatternPart) {
	for stepIndex, step := range traversalStepsForPattern(patternPart) {
		if exactRangeExpansionCandidate(patternPart, step) {
			plan.ExactRangeExpansion = append(plan.ExactRangeExpansion, ExactRangeExpansionDecision{
				Target: target.TraversalStep(stepIndex),
				Depth:  ExactPatternRangeDepth(step.Relationship.Range),
			})
		}
	}
}

func exactRangeExpansionCandidate(patternPart *cypher.PatternPart, step sourceTraversalStep) bool {
	if patternPart == nil {
		return false
	}

	if patternPart.ShortestPathPattern ||
		patternPart.AllShortestPathsPattern ||
		step.Relationship == nil ||
		step.Relationship.Direction == graph.DirectionBoth ||
		step.Relationship.Variable != nil {
		return false
	}

	depth := ExactPatternRangeDepth(step.Relationship.Range)
	return depth >= 1 && depth <= maxExactRangeExpansionDepth
}

func hasExactRangeExpansionDecision(plan *LoweringPlan, target TraversalStepTarget) bool {
	if plan == nil {
		return false
	}

	for _, decision := range plan.ExactRangeExpansion {
		if decision.Target == target {
			return true
		}
	}

	return false
}

func ExactPatternRangeDepth(patternRange *cypher.PatternRange) int64 {
	if patternRange == nil || patternRange.StartIndex == nil || patternRange.EndIndex == nil {
		return 0
	}

	if *patternRange.StartIndex != *patternRange.EndIndex {
		return 0
	}

	return *patternRange.StartIndex
}

type indexedQuantifier struct {
	Index      int
	Quantifier *cypher.Quantifier
}

type quantifierCollector struct {
	walk.VisitorHandler
	quantifiers []indexedQuantifier
}

func (s *quantifierCollector) Enter(node cypher.SyntaxNode) {
	if quantifier, isQuantifier := node.(*cypher.Quantifier); isQuantifier {
		s.quantifiers = append(s.quantifiers, indexedQuantifier{
			Index:      len(s.quantifiers),
			Quantifier: quantifier,
		})
	}
}

func (s *quantifierCollector) Visit(cypher.SyntaxNode) {}
func (s *quantifierCollector) Exit(cypher.SyntaxNode)  {}

func indexedQuantifiersInQueryPart(queryPart cypher.SyntaxNode) []indexedQuantifier {
	if queryPart == nil {
		return nil
	}

	collector := &quantifierCollector{
		VisitorHandler: walk.NewCancelableErrorHandler(),
	}

	if err := walk.Cypher(queryPart, collector); err != nil {
		return nil
	}

	return collector.quantifiers
}

func quantifiersInSyntax(node cypher.SyntaxNode) []*cypher.Quantifier {
	if node == nil {
		return nil
	}

	var (
		quantifiers []*cypher.Quantifier
		collector   = &quantifierCollector{
			VisitorHandler: walk.NewCancelableErrorHandler(),
		}
	)

	if err := walk.Cypher(node, collector); err != nil {
		return nil
	}

	for _, indexed := range collector.quantifiers {
		quantifiers = append(quantifiers, indexed.Quantifier)
	}

	return quantifiers
}

func pathRelationshipQuantifierCandidate(quantifier *cypher.Quantifier) (string, string, bool) {
	if quantifier == nil ||
		(quantifier.Type != cypher.QuantifierTypeAny && quantifier.Type != cypher.QuantifierTypeNone) ||
		quantifier.Filter == nil ||
		quantifier.Filter.Specifier == nil ||
		quantifier.Filter.Specifier.Variable == nil {
		return "", "", false
	}

	function, isFunction := quantifier.Filter.Specifier.Expression.(*cypher.FunctionInvocation)
	if !isFunction || function == nil || function.NumArguments() != 1 || !strings.EqualFold(function.Name, cypher.RelationshipsFunction) {
		return "", "", false
	}

	pathVariable, isPathVariable := function.Arguments[0].(*cypher.Variable)
	if !isPathVariable || pathVariable == nil || pathVariable.Symbol == "" {
		return "", "", false
	}

	bindingSymbol := quantifier.Filter.Specifier.Variable.Symbol
	if bindingSymbol == "" {
		return "", "", false
	}

	return pathVariable.Symbol, bindingSymbol, true
}

func appendPathRelationshipPredicateDecisions(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode) {
	quantifierIndexes := map[*cypher.Quantifier]int{}
	for _, indexed := range indexedQuantifiersInQueryPart(queryPart) {
		quantifierIndexes[indexed.Quantifier] = indexed.Index
	}

	appendMatchWhereQuantifier := func(quantifier *cypher.Quantifier) {
		if quantifierIndex, indexed := quantifierIndexes[quantifier]; indexed {
			if pathSymbol, bindingSymbol, eligible := pathRelationshipQuantifierCandidate(quantifier); eligible {
				plan.PathRelationshipPredicate = append(plan.PathRelationshipPredicate, PathRelationshipPredicateDecision{
					Target: QuantifierTarget{
						QueryPartIndex:  queryPartIndex,
						QuantifierIndex: quantifierIndex,
					},
					PathSymbol:    pathSymbol,
					BindingSymbol: bindingSymbol,
				})
			}
		}
	}

	appendReadingClauseDecisions := func(readingClauses []*cypher.ReadingClause) {
		for _, readingClause := range readingClauses {
			if readingClause == nil || readingClause.Match == nil || readingClause.Match.Where == nil {
				continue
			}

			for _, quantifier := range quantifiersInSyntax(readingClause.Match.Where) {
				appendMatchWhereQuantifier(quantifier)
			}
		}
	}

	switch typedQueryPart := queryPart.(type) {
	case *cypher.SinglePartQuery:
		appendReadingClauseDecisions(typedQueryPart.ReadingClauses)

	case *cypher.MultiPartQueryPart:
		appendReadingClauseDecisions(typedQueryPart.ReadingClauses)
	}
}

func appendProjectionPruningDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, sourceReferences map[string]struct{}) {
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Optional {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			steps := traversalStepsForPattern(patternPart)
			if len(steps) == 0 {
				continue
			}

			appendPatternProjectionPruningDecisions(plan, PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}, patternPart, steps, sourceReferences)
		}
	}
}

func appendPatternProjectionPruningDecisions(plan *LoweringPlan, target PatternTarget, patternPart *cypher.PatternPart, steps []sourceTraversalStep, sourceReferences map[string]struct{}) {
	pathReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(patternPart.Variable))

	for stepIndex, step := range steps {
		var (
			decision = ProjectionPruningDecision{
				Target:                   target.TraversalStep(stepIndex),
				ReferencedSymbols:        sortedMapKeys(sourceReferences),
				PatternBindingReferenced: pathReferenced,
			}
			edgeReferenced = referencesSourceIdentifier(sourceReferences, variableSymbol(step.Relationship.Variable))
			hasPruning     bool
		)

		if step.Relationship.Range != nil && !hasExactRangeExpansionDecision(plan, decision.Target) {
			decision.OmitRelationship = !edgeReferenced
			decision.OmitPathBinding = !pathReferenced
			hasPruning = decision.OmitRelationship || decision.OmitPathBinding
		} else {
			var (
				leftReferenced  = referencesSourceIdentifier(sourceReferences, variableSymbol(step.LeftNode.Variable))
				rightReferenced = referencesSourceIdentifier(sourceReferences, variableSymbol(step.RightNode.Variable))
			)

			decision.OmitLeftNode = !(leftReferenced || pathReferenced)
			decision.OmitRelationship = !(edgeReferenced || pathReferenced)
			decision.OmitRightNode = !(rightReferenced || pathReferenced || stepIndex+1 < len(steps))
			hasPruning = decision.OmitLeftNode || decision.OmitRelationship || decision.OmitRightNode
		}

		if hasPruning {
			plan.ProjectionPruning = append(plan.ProjectionPruning, decision)
		}
	}
}

func appendPatternPredicateProjectionLowerings(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode, sourceReferences map[string]struct{}) {
	for _, indexedPredicate := range indexedPatternPredicatesInQueryPart(queryPart) {
		var (
			predicate   = indexedPredicate.Predicate
			patternPart = patternPartForPredicate(predicate)
			steps       = traversalStepsForPattern(patternPart)
		)

		if len(steps) == 0 {
			continue
		}

		target := PatternTarget{
			QueryPartIndex: queryPartIndex,
			ClauseIndex:    indexedPredicate.ClauseIndex,
			PatternIndex:   indexedPredicate.PredicateIndex,
			Predicate:      true,
			PredicateIndex: indexedPredicate.PredicateIndex,
		}

		appendPatternProjectionPruningDecisions(plan, target, patternPart, steps, sourceReferences)
		appendPatternLatePathMaterializationDecisions(plan, target, patternPart, steps, sourceReferences)
	}
}

func appendPatternPredicatePlacementDecisions(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode) {
	for _, indexedPredicate := range indexedPatternPredicatesInQueryPart(queryPart) {
		var (
			predicate   = indexedPredicate.Predicate
			patternPart = patternPartForPredicate(predicate)
			steps       = traversalStepsForPattern(patternPart)
		)

		if len(steps) != 1 {
			continue
		}

		step := steps[0]
		if step.Relationship == nil ||
			step.Relationship.Direction != graph.DirectionBoth ||
			relationshipPatternHasProperties(step.Relationship) ||
			nodePatternHasConstraints(step.LeftNode) ||
			nodePatternHasConstraints(step.RightNode) {
			continue
		}

		if variableSymbol(step.Relationship.Variable) != "" {
			continue
		}

		target := PatternTarget{
			QueryPartIndex: queryPartIndex,
			ClauseIndex:    indexedPredicate.ClauseIndex,
			PatternIndex:   indexedPredicate.PredicateIndex,
			Predicate:      true,
			PredicateIndex: indexedPredicate.PredicateIndex,
		}.TraversalStep(0)

		plan.PatternPredicate = append(plan.PatternPredicate, PatternPredicatePlacementDecision{
			Target: target,
			Mode:   PatternPredicatePlacementExistence,
		})
	}
}

func appendLatePathMaterializationDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, sourceReferences map[string]struct{}) {
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Optional {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			steps := traversalStepsForPattern(patternPart)
			appendPatternLatePathMaterializationDecisions(plan, PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}, patternPart, steps, sourceReferences)
		}
	}
}

func appendPatternLatePathMaterializationDecisions(plan *LoweringPlan, target PatternTarget, patternPart *cypher.PatternPart, steps []sourceTraversalStep, sourceReferences map[string]struct{}) {
	pathReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(patternPart.Variable))

	for stepIndex, step := range steps {
		stepTarget := target.TraversalStep(stepIndex)

		if step.Relationship.Range != nil && !hasExactRangeExpansionDecision(plan, stepTarget) {
			if !pathReferenced {
				continue
			}

			plan.LatePathMaterialization = append(plan.LatePathMaterialization, LatePathMaterializationDecision{
				Target: stepTarget,
				Mode:   LatePathMaterializationExpansionPath,
			})
			continue
		}

		edgeReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(step.Relationship.Variable))
		if pathReferenced {
			mode := LatePathMaterializationPathEdgeID
			if edgeReferenced {
				mode = LatePathMaterializationEdgeComposite
			}

			plan.LatePathMaterialization = append(plan.LatePathMaterialization, LatePathMaterializationDecision{
				Target: stepTarget,
				Mode:   mode,
			})
			continue
		}

		if !edgeReferenced && stepIndex+1 < len(steps) {
			plan.LatePathMaterialization = append(plan.LatePathMaterialization, LatePathMaterializationDecision{
				Target: stepTarget,
				Mode:   LatePathMaterializationPathEdgeID,
			})
		}
	}
}

func appendExpandIntoDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause) {
	declaredSymbols := map[string]struct{}{}

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		match := readingClause.Match
		if match.Optional {
			declareMatchSymbols(declaredSymbols, match)
			continue
		}

		for patternIndex, patternPart := range match.Pattern {
			var (
				steps             = traversalStepsForPattern(patternPart)
				declaredEndpoints = declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
			)

			for stepIndex, step := range steps {
				if step.Relationship.Range != nil {
					continue
				}

				var (
					leftSymbol  = variableSymbol(step.LeftNode.Variable)
					rightSymbol = variableSymbol(step.RightNode.Variable)
				)

				_, leftBound := declaredEndpoints[stepIndex].BeforeLeftNode[leftSymbol]
				_, rightBound := declaredEndpoints[stepIndex].BeforeRightNode[rightSymbol]

				if leftSymbol == "" {
					leftBound = stepIndex > 0
				}

				if rightSymbol == "" || !leftBound || !rightBound {
					continue
				}

				plan.ExpandInto = append(plan.ExpandInto, ExpandIntoDecision{
					Target: PatternTarget{
						QueryPartIndex: queryPartIndex,
						ClauseIndex:    clauseIndex,
						PatternIndex:   patternIndex,
					}.TraversalStep(stepIndex),
				})
			}

			declarePatternSymbols(declaredSymbols, patternPart)
		}

		declareWhereSymbols(declaredSymbols, match)
	}
}

type declaredStepEndpoints struct {
	BeforeLeftNode  map[string]struct{}
	BeforeRightNode map[string]struct{}
}

func declaredSymbolsBeforeStepEndpoints(initial map[string]struct{}, steps []sourceTraversalStep) []declaredStepEndpoints {
	var (
		declared  = copyStringSet(initial)
		endpoints = make([]declaredStepEndpoints, len(steps))
	)

	for idx, step := range steps {
		endpoints[idx].BeforeLeftNode = copyStringSet(declared)
		addSymbol(declared, variableSymbol(step.LeftNode.Variable))
		addSymbol(declared, variableSymbol(step.Relationship.Variable))
		endpoints[idx].BeforeRightNode = copyStringSet(declared)
		addSymbol(declared, variableSymbol(step.RightNode.Variable))
	}

	return endpoints
}

func appendTraversalDirectionDecisions(
	plan *LoweringPlan,
	queryPartIndex int,
	readingClauses []*cypher.ReadingClause,
	predicateConstrainedSymbols map[string]struct{},
	initialDeclaredSymbols map[string]struct{},
	initialSelectivity map[string]boundSourceSelectivity,
) {
	var (
		declaredSymbols           = copyStringSet(initialDeclaredSymbols)
		declaredSourceSelectivity = copyBoundSourceSelectivity(initialSelectivity)
	)

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		match := readingClause.Match
		if match.Optional {
			declareMatchSymbols(declaredSymbols, match)
			continue
		}

		for patternIndex, patternPart := range match.Pattern {
			var (
				steps             = traversalStepsForPattern(patternPart)
				declaredEndpoints = declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
				patternTarget     = PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}
			)

			for stepIndex, step := range steps {
				target := patternTarget.TraversalStep(stepIndex)
				if decision, shouldFlip := traversalDirectionDecisionForStep(
					target,
					stepIndex,
					step,
					declaredEndpoints[stepIndex],
					referencesSourceIdentifier(predicateConstrainedSymbols, variableSymbol(step.LeftNode.Variable)),
					referencesSourceIdentifier(predicateConstrainedSymbols, variableSymbol(step.RightNode.Variable)),
				); shouldFlip {
					plan.TraversalDirection = append(plan.TraversalDirection, decision)
				} else if decision, shouldFlip := boundLeftExpansionDirectionDecisionForStep(
					target,
					patternPart,
					steps,
					stepIndex,
					step,
					declaredEndpoints[stepIndex],
					referencesSourceIdentifier(predicateConstrainedSymbols, variableSymbol(step.RightNode.Variable)),
					nodePatternSelectivity(step.RightNode, referencesSourceIdentifier(predicateConstrainedSymbols, variableSymbol(step.RightNode.Variable))),
					declaredSourceSelectivity[variableSymbol(step.LeftNode.Variable)],
				); shouldFlip {
					plan.TraversalDirection = append(plan.TraversalDirection, decision)
				}
			}

			declarePatternSymbols(declaredSymbols, patternPart)
		}

		declareSelectiveMatchSymbols(declaredSourceSelectivity, match)
		declareWhereSymbols(declaredSymbols, match)
	}
}

func bindingPredicateSymbols(predicateAttachments []PredicateAttachment, queryPartIndex int) map[string]struct{} {
	symbols := map[string]struct{}{}

	for _, attachment := range predicateAttachments {
		if attachment.QueryPartIndex != queryPartIndex {
			continue
		}

		for _, symbol := range attachment.BindingSymbols {
			addSymbol(symbols, symbol)
		}
	}

	return symbols
}

func copyBoundSourceSelectivity(values map[string]boundSourceSelectivity) map[string]boundSourceSelectivity {
	copied := make(map[string]boundSourceSelectivity, len(values))
	for key, value := range values {
		copied[key] = value
	}

	return copied
}

func carryProjectionSelectivity(
	projection *cypher.Projection,
	incomingSymbols map[string]struct{},
	incomingSelectivity map[string]boundSourceSelectivity,
) (map[string]struct{}, map[string]boundSourceSelectivity) {
	var (
		carriedSymbols     = map[string]struct{}{}
		carriedSelectivity = map[string]boundSourceSelectivity{}
	)

	if projection == nil {
		return carriedSymbols, carriedSelectivity
	}

	projectionSelectivity := projectionCardinalitySelectivity(projection)
	if projectionCarriesAllSymbols(projection) {
		for symbol := range incomingSymbols {
			addSymbol(carriedSymbols, symbol)
			mergeBoundSourceSelectivity(carriedSelectivity, symbol, incomingSelectivity[symbol])
			mergeBoundSourceSelectivity(carriedSelectivity, symbol, projectionSelectivity)
		}
	}

	for _, item := range projection.Items {
		symbol, alias, ok := projectionItemVariableSymbolAndAlias(item)
		if !ok {
			continue
		}
		if symbol == cypher.TokenLiteralAsterisk {
			continue
		}

		addSymbol(carriedSymbols, alias)
		mergeBoundSourceSelectivity(carriedSelectivity, alias, incomingSelectivity[symbol])
		mergeBoundSourceSelectivity(carriedSelectivity, alias, projectionSelectivity)
	}

	return carriedSymbols, carriedSelectivity
}

func projectionCarriesAllSymbols(projection *cypher.Projection) bool {
	if projection == nil {
		return false
	}
	if projection.All || len(projection.Items) == 0 {
		return true
	}

	for _, item := range projection.Items {
		if symbol, _, ok := projectionItemVariableSymbolAndAlias(item); ok && symbol == cypher.TokenLiteralAsterisk {
			return true
		}
		if symbol, ok := expressionVariableSymbol(item); ok && symbol == cypher.TokenLiteralAsterisk {
			return true
		}
	}

	return false
}

func projectionCardinalitySelectivity(projection *cypher.Projection) boundSourceSelectivity {
	if projection == nil || projection.Limit == nil {
		return boundSourceSelectivityNone
	}

	if projection.Order != nil || projectionHasAggregate(projection) {
		return boundSourceSelectivityTopN
	}

	return boundSourceSelectivityLimited
}

func projectionHasAggregate(projection *cypher.Projection) bool {
	if projection == nil {
		return false
	}

	for _, item := range projection.Items {
		projectionItem, ok := item.(*cypher.ProjectionItem)
		if !ok || projectionItem == nil {
			continue
		}

		if expressionHasAggregate(projectionItem.Expression) {
			return true
		}
	}

	return false
}

func expressionHasAggregate(expression cypher.Expression) bool {
	switch typedExpression := expression.(type) {
	case *cypher.FunctionInvocation:
		return typedExpression != nil && strings.EqualFold(typedExpression.Name, cypher.CountFunction)
	default:
		return false
	}
}

func declareSelectiveMatchSymbols(symbols map[string]boundSourceSelectivity, match *cypher.Match) {
	if match == nil {
		return
	}

	for _, patternPart := range match.Pattern {
		for _, nodePattern := range nodePatternsForPattern(patternPart) {
			if nodePattern == nil {
				continue
			}

			symbol := variableSymbol(nodePattern.Variable)
			if symbol == "" {
				continue
			}

			mergeBoundSourceSelectivity(symbols, symbol, propertyConstraintSelectivity(nodePattern.Properties))
		}
	}

	if match.Where == nil {
		return
	}

	for _, expression := range match.Where.Expressions {
		for _, term := range cypherConjunctionTerms(expression) {
			if symbol, selectivity, ok := propertyPredicateSelectivity(term); ok {
				mergeBoundSourceSelectivity(symbols, symbol, selectivity)
			}
		}
	}
}

func declareReadingClauseSymbols(symbols map[string]struct{}, readingClauses []*cypher.ReadingClause) {
	for _, readingClause := range readingClauses {
		if readingClause != nil {
			declareMatchSymbols(symbols, readingClause.Match)
		}
	}
}

func declareReadingClauseSelectivity(symbols map[string]boundSourceSelectivity, readingClauses []*cypher.ReadingClause) {
	for _, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Optional {
			continue
		}

		declareSelectiveMatchSymbols(symbols, readingClause.Match)
	}
}

func nodePatternsForPattern(patternPart *cypher.PatternPart) []*cypher.NodePattern {
	if patternPart == nil {
		return nil
	}

	nodePatterns := make([]*cypher.NodePattern, 0, len(patternPart.PatternElements))
	for _, element := range patternPart.PatternElements {
		if nodePattern, ok := element.AsNodePattern(); ok {
			nodePatterns = append(nodePatterns, nodePattern)
		}
	}

	return nodePatterns
}

func mergeBoundSourceSelectivity(symbols map[string]boundSourceSelectivity, symbol string, selectivity boundSourceSelectivity) {
	if selectivity > symbols[symbol] {
		symbols[symbol] = selectivity
	}
}

func propertyPredicateSelectivity(expression cypher.Expression) (string, boundSourceSelectivity, bool) {
	comparison, isComparison := expression.(*cypher.Comparison)
	if !isComparison || len(comparison.Partials) != 1 {
		return "", boundSourceSelectivityNone, false
	}

	partial := comparison.Partials[0]
	if partial.Operator != cypher.OperatorEquals {
		return "", boundSourceSelectivityNone, false
	}

	if symbol, property, ok := propertyLookupSymbol(comparison.Left); ok && !expressionReferencesAnySource(partial.Right) {
		return symbol, propertySelectivity(property, partial.Right), true
	}

	if symbol, property, ok := propertyLookupSymbol(partial.Right); ok && !expressionReferencesAnySource(comparison.Left) {
		return symbol, propertySelectivity(property, comparison.Left), true
	}

	return "", boundSourceSelectivityNone, false
}

func propertyConstraintSelectivity(expression cypher.Expression) boundSourceSelectivity {
	properties, ok := expression.(*cypher.Properties)
	if !ok || properties == nil || properties.Parameter != nil {
		return boundSourceSelectivityNone
	}

	highest := boundSourceSelectivityNone
	for property, value := range properties.Map {
		if selectivity := propertySelectivity(property, value); selectivity > highest {
			highest = selectivity
		}
	}

	return highest
}

func propertySelectivity(property string, value cypher.Expression) boundSourceSelectivity {
	if strings.EqualFold(property, "objectid") && expressionIsConstant(value) {
		return boundSourceSelectivityUnique
	}

	if expressionIsConstant(value) {
		return boundSourceSelectivityPredicate
	}

	return boundSourceSelectivityNone
}

func expressionIsConstant(expression cypher.Expression) bool {
	switch typedExpression := expression.(type) {
	case *cypher.Literal:
		return typedExpression != nil && !typedExpression.Null
	case *cypher.Parameter:
		return typedExpression != nil
	default:
		return false
	}
}

func propertyLookupSymbol(expression cypher.Expression) (string, string, bool) {
	propertyLookup, isPropertyLookup := expression.(*cypher.PropertyLookup)
	if !isPropertyLookup || propertyLookup == nil {
		return "", "", false
	}

	variable, isVariable := propertyLookup.Atom.(*cypher.Variable)
	if !isVariable || variable == nil || variable.Symbol == "" || propertyLookup.Symbol == "" {
		return "", "", false
	}

	return variable.Symbol, propertyLookup.Symbol, true
}

func nodePatternHasUniquePropertyConstraint(nodePattern *cypher.NodePattern) bool {
	return nodePattern != nil && propertyConstraintSelectivity(nodePattern.Properties) == boundSourceSelectivityUnique
}

func nodePatternSelectivity(nodePattern *cypher.NodePattern, hasAttachedPredicate bool) boundSourceSelectivity {
	if nodePattern == nil {
		return boundSourceSelectivityNone
	}

	selectivity := boundSourceSelectivityNone
	if len(nodePattern.Kinds) > 0 {
		selectivity = boundSourceSelectivityKindOnly
	}

	mergeSelectivityValue(&selectivity, propertyConstraintSelectivity(nodePattern.Properties))
	if hasAttachedPredicate {
		mergeSelectivityValue(&selectivity, boundSourceSelectivityPredicate)
	}

	return selectivity
}

func mergeSelectivityValue(current *boundSourceSelectivity, next boundSourceSelectivity) {
	if next > *current {
		*current = next
	}
}

func shortestPathSearchPredicateSymbols(readingClauses []*cypher.ReadingClause) map[string]struct{} {
	symbols := map[string]struct{}{}

	for _, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Where == nil {
			continue
		}

		for _, expression := range readingClause.Match.Where.Expressions {
			addShortestPathSearchPredicateSymbols(symbols, expression)
		}
	}

	return symbols
}

func addShortestPathSearchPredicateSymbols(symbols map[string]struct{}, expression cypher.Expression) {
	for _, term := range cypherConjunctionTerms(expression) {
		if symbol, ok := shortestPathSearchPredicateSymbol(term); ok {
			addSymbol(symbols, symbol)
		}
	}
}

func cypherConjunctionTerms(expression cypher.Expression) []cypher.Expression {
	if conjunction, isConjunction := expression.(*cypher.Conjunction); isConjunction {
		var terms []cypher.Expression
		for _, subexpression := range conjunction.Expressions {
			terms = append(terms, cypherConjunctionTerms(subexpression)...)
		}

		return terms
	}

	return []cypher.Expression{expression}
}

func shortestPathSearchPredicateSymbol(expression cypher.Expression) (string, bool) {
	comparison, isComparison := expression.(*cypher.Comparison)
	if !isComparison || len(comparison.Partials) != 1 {
		return "", false
	}

	partial := comparison.Partials[0]
	if !isEndpointSearchOperator(partial.Operator) {
		return "", false
	}

	if symbol, ok := propertyLookupVariableSymbol(comparison.Left); ok && !expressionReferencesAnySource(partial.Right) {
		return symbol, true
	}

	if symbol, ok := propertyLookupVariableSymbol(partial.Right); ok && !expressionReferencesAnySource(comparison.Left) {
		return symbol, true
	}

	return "", false
}

func isEndpointSearchOperator(operator cypher.Operator) bool {
	switch operator {
	case cypher.OperatorEquals,
		cypher.OperatorRegexMatch,
		cypher.OperatorGreaterThan,
		cypher.OperatorGreaterThanOrEqualTo,
		cypher.OperatorLessThan,
		cypher.OperatorLessThanOrEqualTo,
		cypher.OperatorStartsWith,
		cypher.OperatorEndsWith,
		cypher.OperatorContains,
		cypher.OperatorIn:
		return true
	default:
		return false
	}
}

func propertyLookupVariableSymbol(expression cypher.Expression) (string, bool) {
	propertyLookup, isPropertyLookup := expression.(*cypher.PropertyLookup)
	if !isPropertyLookup || propertyLookup == nil {
		return "", false
	}

	variable, isVariable := propertyLookup.Atom.(*cypher.Variable)
	if !isVariable || variable == nil || variable.Symbol == "" {
		return "", false
	}

	return variable.Symbol, true
}

func expressionReferencesAnySource(expression cypher.Expression) bool {
	references, err := collectReferencedSourceIdentifiers(expression)
	return err != nil || len(references) > 0
}

func traversalDirectionDecisionForStep(
	target TraversalStepTarget,
	stepIndex int,
	step sourceTraversalStep,
	declaredEndpoints declaredStepEndpoints,
	leftHasAttachedPredicate bool,
	rightHasAttachedPredicate bool,
) (TraversalDirectionDecision, bool) {
	if leftEndpointBoundForStep(stepIndex, step, declaredEndpoints) {
		return TraversalDirectionDecision{}, false
	}

	var (
		rightSymbol = variableSymbol(step.RightNode.Variable)
		leftSymbol  = variableSymbol(step.LeftNode.Variable)
	)

	if rightSymbol != "" {
		if _, rightBound := declaredEndpoints.BeforeRightNode[rightSymbol]; rightBound {
			if rightSymbol == leftSymbol {
				return TraversalDirectionDecision{}, false
			}

			return TraversalDirectionDecision{
				Target: target,
				Flip:   true,
				Reason: traversalDirectionReasonRightBound,
			}, true
		}
	}

	var (
		leftConstrained  = nodePatternHasConstraints(step.LeftNode) || leftHasAttachedPredicate
		rightConstrained = nodePatternHasConstraints(step.RightNode) || rightHasAttachedPredicate
	)

	if rightConstrained && !leftConstrained {
		reason := traversalDirectionReasonRightConstrained
		if !nodePatternHasConstraints(step.RightNode) && rightHasAttachedPredicate {
			reason = traversalDirectionReasonRightPredicate
		}

		return TraversalDirectionDecision{
			Target: target,
			Flip:   true,
			Reason: reason,
		}, true
	}

	return TraversalDirectionDecision{}, false
}

func boundLeftExpansionDirectionDecisionForStep(
	target TraversalStepTarget,
	patternPart *cypher.PatternPart,
	steps []sourceTraversalStep,
	stepIndex int,
	step sourceTraversalStep,
	declaredEndpoints declaredStepEndpoints,
	rightHasAttachedPredicate bool,
	rightSelectivity boundSourceSelectivity,
	leftSourceSelectivity boundSourceSelectivity,
) (TraversalDirectionDecision, bool) {
	if patternPart == nil ||
		patternPart.Variable != nil ||
		patternPart.ShortestPathPattern ||
		patternPart.AllShortestPathsPattern ||
		len(steps) != 1 ||
		stepIndex != 0 ||
		step.Relationship == nil ||
		step.Relationship.Range == nil ||
		step.Relationship.Direction == graph.DirectionBoth ||
		step.Relationship.Variable != nil ||
		nodePatternHasConstraints(step.LeftNode) ||
		!nodePatternHasConstraints(step.RightNode) {
		return TraversalDirectionDecision{}, false
	}

	var (
		leftSymbol  = variableSymbol(step.LeftNode.Variable)
		rightSymbol = variableSymbol(step.RightNode.Variable)
	)

	if leftSymbol == "" || leftSymbol == rightSymbol {
		return TraversalDirectionDecision{}, false
	}

	if _, leftBound := declaredEndpoints.BeforeLeftNode[leftSymbol]; !leftBound {
		return TraversalDirectionDecision{}, false
	}

	if rightSymbol != "" {
		if _, rightBound := declaredEndpoints.BeforeRightNode[rightSymbol]; rightBound {
			return TraversalDirectionDecision{}, false
		}
	}

	if leftSourceSelectivity >= boundSourceSelectivityUnique && rightSelectivity < boundSourceSelectivityUnique {
		return TraversalDirectionDecision{
			Target: target,
			Reason: traversalDirectionReasonBoundSourceSelective,
		}, true
	}

	if step.RightNode.Properties == nil && !rightHasAttachedPredicate {
		return TraversalDirectionDecision{
			Target: target,
			Reason: traversalDirectionReasonTerminalKindOnlyEstimateWide,
		}, true
	}

	return TraversalDirectionDecision{
		Target: target,
		Flip:   true,
		Reason: traversalDirectionReasonRightConstrained,
	}, true
}

func appendShortestPathStrategyDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, predicateConstrainedSymbols map[string]struct{}) {
	declaredSymbols := map[string]struct{}{}

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		match := readingClause.Match
		if match.Optional {
			declareMatchSymbols(declaredSymbols, match)
			continue
		}

		for patternIndex, patternPart := range match.Pattern {
			if patternPart == nil || (!patternPart.ShortestPathPattern && !patternPart.AllShortestPathsPattern) {
				declarePatternSymbols(declaredSymbols, patternPart)
				continue
			}

			var (
				steps             = traversalStepsForPattern(patternPart)
				declaredEndpoints = declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
				patternTarget     = PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}
			)

			for stepIndex, step := range steps {
				if step.Relationship.Range == nil {
					continue
				}

				if decision, shouldPlan := shortestPathStrategyDecisionForStep(
					patternTarget.TraversalStep(stepIndex),
					step,
					declaredEndpoints[stepIndex],
					predicateConstrainedSymbols,
				); shouldPlan {
					plan.ShortestPathStrategy = append(plan.ShortestPathStrategy, decision)
				}
			}

			declarePatternSymbols(declaredSymbols, patternPart)
		}

		declareWhereSymbols(declaredSymbols, match)
	}
}

func shortestPathStrategyDecisionForStep(
	target TraversalStepTarget,
	step sourceTraversalStep,
	declaredEndpoints declaredStepEndpoints,
	predicateConstrainedSymbols map[string]struct{},
) (ShortestPathStrategyDecision, bool) {
	var (
		leftSymbol  = variableSymbol(step.LeftNode.Variable)
		rightSymbol = variableSymbol(step.RightNode.Variable)
	)

	_, rightBound := declaredEndpoints.BeforeRightNode[rightSymbol]
	if leftEndpointBoundForStep(target.StepIndex, step, declaredEndpoints) && rightSymbol != "" && rightBound {
		return ShortestPathStrategyDecision{
			Target:   target,
			Strategy: ShortestPathStrategyBidirectional,
			Reason:   shortestPathStrategyReasonBoundEndpointPairs,
		}, true
	}

	if endpointHasSearchConstraint(step.LeftNode, leftSymbol, predicateConstrainedSymbols) &&
		endpointHasSearchConstraint(step.RightNode, rightSymbol, predicateConstrainedSymbols) {
		return ShortestPathStrategyDecision{
			Target:   target,
			Strategy: ShortestPathStrategyBidirectional,
			Reason:   shortestPathStrategyReasonEndpointPredicates,
		}, true
	}

	return ShortestPathStrategyDecision{}, false
}

func endpointHasSearchConstraint(nodePattern *cypher.NodePattern, symbol string, predicateConstrainedSymbols map[string]struct{}) bool {
	if nodePattern == nil {
		return false
	}

	return nodePattern.Properties != nil || referencesSourceIdentifier(predicateConstrainedSymbols, symbol)
}

func endpointHasTerminalFilterConstraint(nodePattern *cypher.NodePattern, symbol string, predicateConstrainedSymbols map[string]struct{}) bool {
	if nodePattern == nil {
		return false
	}

	return nodePatternHasConstraints(nodePattern) || referencesSourceIdentifier(predicateConstrainedSymbols, symbol)
}

func appendShortestPathFilterDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, predicateConstrainedSymbols map[string]struct{}) {
	declaredSymbols := map[string]struct{}{}

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		match := readingClause.Match
		if match.Optional {
			declareMatchSymbols(declaredSymbols, match)
			continue
		}

		for patternIndex, patternPart := range match.Pattern {
			if patternPart == nil || (!patternPart.ShortestPathPattern && !patternPart.AllShortestPathsPattern) {
				declarePatternSymbols(declaredSymbols, patternPart)
				continue
			}

			var (
				steps             = traversalStepsForPattern(patternPart)
				declaredEndpoints = declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
				patternTarget     = PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}
			)

			for stepIndex, step := range steps {
				if step.Relationship.Range == nil {
					continue
				}

				if decision, shouldPlan := shortestPathFilterDecisionForStep(
					plan,
					patternTarget.TraversalStep(stepIndex),
					step,
					declaredEndpoints[stepIndex],
					predicateConstrainedSymbols,
				); shouldPlan {
					plan.ShortestPathFilter = append(plan.ShortestPathFilter, decision)
				}
			}

			declarePatternSymbols(declaredSymbols, patternPart)
		}

		declareWhereSymbols(declaredSymbols, match)
	}
}

func shortestPathFilterDecisionForStep(
	plan *LoweringPlan,
	target TraversalStepTarget,
	step sourceTraversalStep,
	declaredEndpoints declaredStepEndpoints,
	predicateConstrainedSymbols map[string]struct{},
) (ShortestPathFilterDecision, bool) {
	var (
		leftSymbol  = variableSymbol(step.LeftNode.Variable)
		rightSymbol = variableSymbol(step.RightNode.Variable)
	)

	if rightSymbol != "" {
		if _, rightBound := declaredEndpoints.BeforeRightNode[rightSymbol]; rightBound {
			return ShortestPathFilterDecision{}, false
		}
	}

	var (
		leftSearchConstrained  = endpointHasSearchConstraint(step.LeftNode, leftSymbol, predicateConstrainedSymbols)
		rightSearchConstrained = endpointHasSearchConstraint(step.RightNode, rightSymbol, predicateConstrainedSymbols)
	)

	if !endpointHasTerminalFilterConstraint(step.RightNode, rightSymbol, predicateConstrainedSymbols) {
		return ShortestPathFilterDecision{}, false
	}

	if hasShortestPathBidirectionalStrategy(plan, target) && leftSearchConstrained && rightSearchConstrained {
		return ShortestPathFilterDecision{
			Target: target,
			Mode:   ShortestPathFilterEndpointPair,
			Reason: shortestPathFilterReasonEndpointPairPredicates,
		}, true
	}

	return ShortestPathFilterDecision{
		Target: target,
		Mode:   ShortestPathFilterTerminal,
		Reason: shortestPathFilterReasonTerminalPredicate,
	}, true
}

func hasShortestPathBidirectionalStrategy(plan *LoweringPlan, target TraversalStepTarget) bool {
	if plan == nil {
		return false
	}

	for _, decision := range plan.ShortestPathStrategy {
		if decision.Target == target && decision.Strategy == ShortestPathStrategyBidirectional {
			return true
		}
	}

	return false
}

func appendLimitPushdownDecisions(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode, readingClauses []*cypher.ReadingClause) {
	if !queryPartAllowsLimitPushdown(queryPart, readingClauses) {
		return
	}

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Optional {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			if patternPart == nil {
				continue
			}
			if patternPart.AllShortestPathsPattern {
				continue
			}

			patternTarget := PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}

			for stepIndex, step := range traversalStepsForPattern(patternPart) {
				mode := LimitPushdownTraversalCTE
				if patternPart.ShortestPathPattern && step.Relationship.Range != nil {
					mode = LimitPushdownShortestPathHarness
				}

				plan.LimitPushdown = append(plan.LimitPushdown, LimitPushdownDecision{
					Target: patternTarget.TraversalStep(stepIndex),
					Mode:   mode,
				})
			}
		}
	}
}

func queryPartAllowsLimitPushdown(queryPart cypher.SyntaxNode, readingClauses []*cypher.ReadingClause) bool {
	projection, updatingClauseCount := queryPartProjection(queryPart)
	if projection == nil ||
		projection.Limit == nil ||
		projection.Skip != nil ||
		projection.Order != nil ||
		projection.Distinct ||
		len(readingClauses) != 1 ||
		updatingClauseCount > 0 {
		return false
	}

	return true
}

func queryPartProjection(queryPart cypher.SyntaxNode) (*cypher.Projection, int) {
	switch typedQueryPart := queryPart.(type) {
	case *cypher.SinglePartQuery:
		if typedQueryPart.Return == nil {
			return nil, len(typedQueryPart.UpdatingClauses)
		}

		return typedQueryPart.Return.Projection, len(typedQueryPart.UpdatingClauses)

	case *cypher.MultiPartQueryPart:
		if typedQueryPart.With == nil {
			return nil, len(typedQueryPart.UpdatingClauses)
		}

		return typedQueryPart.With.Projection, len(typedQueryPart.UpdatingClauses)

	default:
		return nil, 0
	}
}

func suffixBindingsObserved(patternPart *cypher.PatternPart, steps []sourceTraversalStep, references map[string]struct{}) bool {
	if patternPart != nil && patternPart.Variable != nil && referencesSourceIdentifier(references, patternPart.Variable.Symbol) {
		return true
	}
	for _, step := range steps {
		if (step.Relationship != nil && step.Relationship.Variable != nil && referencesSourceIdentifier(references, step.Relationship.Variable.Symbol)) ||
			(step.RightNode != nil && step.RightNode.Variable != nil && referencesSourceIdentifier(references, step.RightNode.Variable.Symbol)) {
			return true
		}
	}
	return false
}

func appendExpansionSuffixPushdownDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, sourceReferences map[string]struct{}) {
	declaredSymbols := map[string]struct{}{}

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		match := readingClause.Match
		if match.Optional {
			declareMatchSymbols(declaredSymbols, match)
			continue
		}

		for patternIndex, patternPart := range match.Pattern {
			var (
				steps             = traversalStepsForPattern(patternPart)
				declaredEndpoints = declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
			)

			for stepIndex, step := range steps {
				if step.Relationship.Range == nil || stepIndex+1 >= len(steps) {
					continue
				}

				target := PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}.TraversalStep(stepIndex)
				if hasExactRangeExpansionDecision(plan, target) {
					continue
				}

				if hasTraversalDirectionFlip(plan, target) || expansionStepMayFlipForConstraintBalance(stepIndex, step, declaredEndpoints[stepIndex]) {
					continue
				}

				if suffixLength := expansionSuffixPushdownLength(steps[stepIndex+1:]); suffixLength > 0 {
					suffixSteps := steps[stepIndex+1 : stepIndex+1+suffixLength]
					// Start with the measured fixed-suffix shape: an observed immediate
					// continuation of three or more fixed hops. Shorter suffixes retain
					// the established prefilter until their own decoy-density A/B exists.
					observed := suffixLength >= 3 && suffixBindingsObserved(patternPart, suffixSteps, sourceReferences)
					reason := "supplemental suffix prefilter retained for unobserved continuation"
					if observed {
						reason = "immediate observed continuation produces suffix rows"
					}
					plan.ExpansionSuffixPushdown = append(plan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
						Target:            target,
						SuffixLength:      suffixLength,
						SuffixStartStep:   stepIndex + 1,
						SuffixEndStep:     stepIndex + suffixLength,
						ApplySupplemental: !observed,
						Reason:            reason,
					})
				}
			}

			declarePatternSymbols(declaredSymbols, patternPart)
		}

		declareWhereSymbols(declaredSymbols, match)
	}
}

func expansionStepMayFlipForConstraintBalance(stepIndex int, step sourceTraversalStep, declaredEndpoints declaredStepEndpoints) bool {
	_, mayFlip := traversalDirectionDecisionForStep(TraversalStepTarget{}, stepIndex, step, declaredEndpoints, false, false)
	return mayFlip
}

func leftEndpointBoundForStep(stepIndex int, step sourceTraversalStep, declaredEndpoints declaredStepEndpoints) bool {
	leftSymbol := variableSymbol(step.LeftNode.Variable)
	if leftSymbol == "" {
		return stepIndex > 0
	}

	_, leftBound := declaredEndpoints.BeforeLeftNode[leftSymbol]
	return leftBound
}

func hasTraversalDirectionFlip(plan *LoweringPlan, target TraversalStepTarget) bool {
	if plan == nil {
		return false
	}

	for _, decision := range plan.TraversalDirection {
		if decision.Target == target && decision.Flip {
			return true
		}
	}

	return false
}

type bindingTargetKey struct {
	QueryPartIndex int
	Symbol         string
}

func appendPredicatePlacementDecisions(plan *LoweringPlan, query *cypher.RegularQuery, predicateAttachments []PredicateAttachment) {
	if len(predicateAttachments) == 0 {
		return
	}

	bindingTargets := indexBindingTargets(query)
	for _, attachment := range predicateAttachments {
		if attachment.Scope != PredicateAttachmentScopeBinding || len(attachment.BindingSymbols) != 1 {
			continue
		}

		target, hasTarget := bindingTargets[bindingTargetKey{
			QueryPartIndex: attachment.QueryPartIndex,
			Symbol:         attachment.BindingSymbols[0],
		}]
		if !hasTarget {
			continue
		}
		if target.ClauseIndex != attachment.ClauseIndex {
			continue
		}

		plan.PredicatePlacement = append(plan.PredicatePlacement, PredicatePlacementDecision{
			Target:     target,
			Attachment: attachment,
			Placement:  attachment.Scope,
		})
	}
}

func attachPredicatePlacementsToSuffixPushdowns(plan *LoweringPlan) {
	for suffixIdx := range plan.ExpansionSuffixPushdown {
		suffix := &plan.ExpansionSuffixPushdown[suffixIdx]
		for _, placement := range plan.PredicatePlacement {
			if placement.Target.QueryPartIndex != suffix.Target.QueryPartIndex ||
				placement.Target.ClauseIndex != suffix.Target.ClauseIndex ||
				placement.Target.PatternIndex != suffix.Target.PatternIndex {
				continue
			}

			if placement.Target.StepIndex > suffix.Target.StepIndex &&
				placement.Target.StepIndex <= suffix.Target.StepIndex+suffix.SuffixLength {
				suffix.PredicateAttachments = append(suffix.PredicateAttachments, placement.Attachment)
			}
		}
	}
}

func appendCountStoreFastPathDecisions(plan *LoweringPlan, query *cypher.RegularQuery) {
	if decision, ok := countStoreFastPathDecision(query); ok {
		plan.CountStoreFastPath = append(plan.CountStoreFastPath, decision)
	}
}

func appendAggregateTraversalCountDecisions(plan *LoweringPlan, query *cypher.RegularQuery) {
	if shape, ok := AggregateTraversalCountShapeForQuery(query); ok {
		plan.AggregateTraversalCount = append(plan.AggregateTraversalCount, AggregateTraversalCountDecision{
			QueryPartIndex: shape.QueryPartIndex,
			SourceSymbol:   shape.SourceSymbol,
			TerminalSymbol: shape.TerminalSymbol,
			CountAlias:     shape.CountAlias,
			Limit:          shape.Limit,
			Target:         shape.Target,
		})
	}
}

func AggregateTraversalCountShapeForQuery(query *cypher.RegularQuery) (AggregateTraversalCountShape, bool) {
	if query == nil || query.SingleQuery == nil || query.SingleQuery.MultiPartQuery == nil {
		return AggregateTraversalCountShape{}, false
	}

	multiPartQuery := query.SingleQuery.MultiPartQuery
	if len(multiPartQuery.Parts) != 1 || multiPartQuery.Parts[0] == nil || multiPartQuery.SinglePartQuery == nil {
		return AggregateTraversalCountShape{}, false
	}

	part := multiPartQuery.Parts[0]
	if len(part.UpdatingClauses) > 0 || len(part.ReadingClauses) != 2 || part.With == nil || part.With.Where != nil {
		return AggregateTraversalCountShape{}, false
	}

	sourceMatch, sourceNode, sourceSymbol, ok := aggregateTraversalSourceMatch(part.ReadingClauses[0])
	if !ok {
		return AggregateTraversalCountShape{}, false
	}

	terminalMatch, relationship, terminalNode, terminalSymbol, ok := aggregateTraversalMatch(part.ReadingClauses[1], sourceSymbol)
	if !ok {
		return AggregateTraversalCountShape{}, false
	}

	countAlias, ok := aggregateTraversalWithProjection(part.With.Projection, sourceSymbol, terminalSymbol)
	if !ok {
		return AggregateTraversalCountShape{}, false
	}

	finalProjection, ok := aggregateTraversalFinalProjection(multiPartQuery.SinglePartQuery, sourceSymbol, countAlias)
	if !ok {
		return AggregateTraversalCountShape{}, false
	}

	minDepth, maxDepth, ok := aggregateTraversalDepthBounds(relationship.Range)
	if !ok {
		return AggregateTraversalCountShape{}, false
	}

	return AggregateTraversalCountShape{
		QueryPartIndex:    0,
		SourceSymbol:      sourceSymbol,
		TerminalSymbol:    terminalSymbol,
		CountAlias:        countAlias,
		ReturnSourceAlias: finalProjection.SourceAlias,
		ReturnCountAlias:  finalProjection.CountAlias,
		ReturnCount:       finalProjection.ReturnCount,
		Limit:             finalProjection.Limit,
		SourceMatch:       sourceMatch,
		TerminalMatch:     terminalMatch,
		SourceKinds:       sourceNode.Kinds,
		TerminalKinds:     terminalNode.Kinds,
		RelationshipKinds: relationship.Kinds,
		Direction:         relationship.Direction,
		MinDepth:          minDepth,
		MaxDepth:          maxDepth,
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
	}, true
}

func aggregateTraversalSourceMatch(readingClause *cypher.ReadingClause) (*cypher.Match, *cypher.NodePattern, string, bool) {
	if readingClause == nil || readingClause.Match == nil {
		return nil, nil, "", false
	}

	match := readingClause.Match
	if match.Optional || len(match.Pattern) != 1 {
		return nil, nil, "", false
	}

	patternPart := match.Pattern[0]
	nodePattern, ok := singleNodePattern(patternPart)
	if !ok || nodePattern == nil || nodePattern.Variable == nil || nodePattern.Variable.Symbol == "" || nodePattern.Properties != nil {
		return nil, nil, "", false
	}

	for _, dependency := range sortedDependencies(match.Where) {
		if dependency != nodePattern.Variable.Symbol {
			return nil, nil, "", false
		}
	}

	return match, nodePattern, nodePattern.Variable.Symbol, true
}

func aggregateTraversalMatch(readingClause *cypher.ReadingClause, sourceSymbol string) (*cypher.Match, *cypher.RelationshipPattern, *cypher.NodePattern, string, bool) {
	if readingClause == nil || readingClause.Match == nil {
		return nil, nil, nil, "", false
	}

	match := readingClause.Match
	if match.Optional || len(match.Pattern) != 1 {
		return nil, nil, nil, "", false
	}

	patternPart := match.Pattern[0]
	if patternPart == nil || patternPart.Variable != nil || patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern || len(patternPart.PatternElements) != 3 {
		return nil, nil, nil, "", false
	}

	leftNode, leftOK := patternPart.PatternElements[0].AsNodePattern()
	relationship, relationshipOK := patternPart.PatternElements[1].AsRelationshipPattern()
	rightNode, rightOK := patternPart.PatternElements[2].AsNodePattern()
	if !leftOK || !relationshipOK || !rightOK ||
		leftNode == nil || relationship == nil || rightNode == nil ||
		variableSymbol(leftNode.Variable) != sourceSymbol ||
		leftNode.Properties != nil ||
		relationship.Variable != nil ||
		relationship.Range == nil ||
		relationship.Properties != nil ||
		relationship.Direction == graph.DirectionBoth ||
		rightNode.Properties != nil ||
		rightNode.Variable == nil ||
		rightNode.Variable.Symbol == "" {
		return nil, nil, nil, "", false
	}

	if match.Where != nil {
		for _, dependency := range sortedDependencies(match.Where) {
			if dependency != rightNode.Variable.Symbol {
				return nil, nil, nil, "", false
			}
		}
	}

	return match, relationship, rightNode, rightNode.Variable.Symbol, true
}

func aggregateTraversalWithProjection(projection *cypher.Projection, sourceSymbol, terminalSymbol string) (string, bool) {
	if projection == nil || projection.All || projection.Order != nil || projection.Skip != nil || projection.Limit != nil || len(projection.Items) != 2 {
		return "", false
	}

	if symbol, ok := projectionItemVariableSymbol(projection.Items[0]); !ok || symbol != sourceSymbol {
		return "", false
	}

	countAlias, ok := projectionItemCountAlias(projection.Items[1], terminalSymbol)
	if !ok {
		return "", false
	}

	return countAlias, true
}

type aggregateTraversalFinalProjectionShape struct {
	SourceAlias string
	CountAlias  string
	ReturnCount bool
	Limit       int64
}

func aggregateTraversalFinalProjection(queryPart *cypher.SinglePartQuery, sourceSymbol, countAlias string) (aggregateTraversalFinalProjectionShape, bool) {
	if queryPart == nil || len(queryPart.ReadingClauses) > 0 || len(queryPart.UpdatingClauses) > 0 || queryPart.Return == nil || queryPart.Return.Projection == nil {
		return aggregateTraversalFinalProjectionShape{}, false
	}

	projection := queryPart.Return.Projection
	if projection.Distinct || projection.All || projection.Skip != nil || projection.Order == nil || projection.Limit == nil || len(projection.Items) < 1 || len(projection.Items) > 2 {
		return aggregateTraversalFinalProjectionShape{}, false
	}

	var (
		finalProjection = aggregateTraversalFinalProjectionShape{
			SourceAlias: sourceSymbol,
			CountAlias:  countAlias,
		}
		sourceSeen = false
		countSeen  = false
	)

	for _, item := range projection.Items {
		symbol, alias, ok := projectionItemVariableSymbolAndAlias(item)
		if !ok {
			return aggregateTraversalFinalProjectionShape{}, false
		}

		switch symbol {
		case sourceSymbol:
			if sourceSeen {
				return aggregateTraversalFinalProjectionShape{}, false
			}
			sourceSeen = true
			finalProjection.SourceAlias = alias
		case countAlias:
			if countSeen {
				return aggregateTraversalFinalProjectionShape{}, false
			}
			countSeen = true
			finalProjection.ReturnCount = true
			finalProjection.CountAlias = alias
		default:
			return aggregateTraversalFinalProjectionShape{}, false
		}
	}
	if !sourceSeen {
		return aggregateTraversalFinalProjectionShape{}, false
	}

	if len(projection.Order.Items) != 1 || projection.Order.Items[0] == nil || projection.Order.Items[0].Ascending {
		return aggregateTraversalFinalProjectionShape{}, false
	}

	if orderSymbol, ok := expressionVariableSymbol(projection.Order.Items[0].Expression); !ok || (orderSymbol != countAlias && orderSymbol != finalProjection.CountAlias) {
		return aggregateTraversalFinalProjectionShape{}, false
	}

	limit, ok := literalInt64(projection.Limit.Value)
	if !ok {
		return aggregateTraversalFinalProjectionShape{}, false
	}
	finalProjection.Limit = limit
	return finalProjection, true
}

func aggregateTraversalDepthBounds(patternRange *cypher.PatternRange) (int64, int64, bool) {
	if patternRange == nil {
		return 0, 0, false
	}

	minDepth := int64(1)
	if patternRange.StartIndex != nil {
		minDepth = *patternRange.StartIndex
	}
	if minDepth < 1 {
		return 0, 0, false
	}

	maxDepth := int64(15)
	if patternRange.EndIndex != nil {
		maxDepth = *patternRange.EndIndex
	}
	if maxDepth < minDepth {
		return 0, 0, false
	}

	return minDepth, maxDepth, true
}

func projectionItemVariableSymbol(expression cypher.Expression) (string, bool) {
	projectionItem, ok := expression.(*cypher.ProjectionItem)
	if !ok || projectionItem == nil || projectionItem.Alias != nil {
		return "", false
	}

	return expressionVariableSymbol(projectionItem.Expression)
}

func projectionItemVariableSymbolAndAlias(expression cypher.Expression) (string, string, bool) {
	projectionItem, ok := expression.(*cypher.ProjectionItem)
	if !ok || projectionItem == nil {
		return "", "", false
	}

	symbol, ok := expressionVariableSymbol(projectionItem.Expression)
	if !ok {
		return "", "", false
	}

	alias := symbol
	if projectionItem.Alias != nil {
		if projectionItem.Alias.Symbol == "" {
			return "", "", false
		}

		alias = projectionItem.Alias.Symbol
	}

	return symbol, alias, true
}

func expressionVariableSymbol(expression cypher.Expression) (string, bool) {
	variable, ok := expression.(*cypher.Variable)
	if !ok || variable == nil || variable.Symbol == "" {
		return "", false
	}

	return variable.Symbol, true
}

func projectionItemCountAlias(expression cypher.Expression, terminalSymbol string) (string, bool) {
	projectionItem, ok := expression.(*cypher.ProjectionItem)
	if !ok || projectionItem == nil || projectionItem.Alias == nil || projectionItem.Alias.Symbol == "" {
		return "", false
	}

	function, ok := projectionItem.Expression.(*cypher.FunctionInvocation)
	if !ok || function == nil || !strings.EqualFold(function.Name, cypher.CountFunction) ||
		function.Distinct || len(function.Namespace) > 0 || len(function.Arguments) != 1 {
		return "", false
	}

	if !aggregateTraversalCountArgumentMatches(function.Arguments[0], terminalSymbol) {
		return "", false
	}

	return projectionItem.Alias.Symbol, true
}

func aggregateTraversalCountArgumentMatches(expression cypher.Expression, terminalSymbol string) bool {
	if symbol, ok := expressionVariableSymbol(expression); ok {
		return symbol == terminalSymbol
	}

	rangeQuantifier, ok := expression.(*cypher.RangeQuantifier)
	return ok && rangeQuantifier != nil && rangeQuantifier.Value == cypher.TokenLiteralAsterisk
}

func literalInt64(expression cypher.Expression) (int64, bool) {
	literal, ok := expression.(*cypher.Literal)
	if !ok || literal == nil || literal.Null {
		return 0, false
	}

	switch value := literal.Value.(type) {
	case int:
		return int64(value), value >= 0
	case int8:
		return int64(value), value >= 0
	case int16:
		return int64(value), value >= 0
	case int32:
		return int64(value), value >= 0
	case int64:
		return value, value >= 0
	default:
		return 0, false
	}
}

func countStoreFastPathDecision(query *cypher.RegularQuery) (CountStoreFastPathDecision, bool) {
	if query == nil || query.SingleQuery == nil || query.SingleQuery.SinglePartQuery == nil {
		return CountStoreFastPathDecision{}, false
	}

	queryPart := query.SingleQuery.SinglePartQuery
	if len(queryPart.UpdatingClauses) > 0 || len(queryPart.ReadingClauses) != 1 {
		return CountStoreFastPathDecision{}, false
	}

	countArgument, ok := simpleCountProjectionArgument(queryPart.Return)
	if !ok {
		return CountStoreFastPathDecision{}, false
	}

	readingClause := queryPart.ReadingClauses[0]
	if readingClause == nil || readingClause.Match == nil {
		return CountStoreFastPathDecision{}, false
	}

	match := readingClause.Match
	if match.Optional || match.Where != nil || len(match.Pattern) != 1 {
		return CountStoreFastPathDecision{}, false
	}

	patternPart := match.Pattern[0]
	if patternPart == nil || patternPart.Variable != nil || patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern {
		return CountStoreFastPathDecision{}, false
	}

	if len(patternPart.PatternElements) == 1 {
		nodePattern, ok := patternPart.PatternElements[0].AsNodePattern()
		if !ok || nodePattern == nil || nodePattern.Properties != nil {
			return CountStoreFastPathDecision{}, false
		}

		bindingSymbol := variableSymbol(nodePattern.Variable)
		if countArgument != cypher.TokenLiteralAsterisk && countArgument != bindingSymbol {
			return CountStoreFastPathDecision{}, false
		}

		return CountStoreFastPathDecision{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			BindingSymbol:  bindingSymbol,
			Target:         CountStoreFastPathNode,
			KindSymbols:    kindSymbols(nodePattern.Kinds),
		}, true
	}

	if len(patternPart.PatternElements) != 3 {
		return CountStoreFastPathDecision{}, false
	}

	leftNode, leftOK := patternPart.PatternElements[0].AsNodePattern()
	relationship, relationshipOK := patternPart.PatternElements[1].AsRelationshipPattern()
	rightNode, rightOK := patternPart.PatternElements[2].AsNodePattern()
	if !leftOK || !relationshipOK || !rightOK {
		return CountStoreFastPathDecision{}, false
	}

	if constrainedCountFastPathEndpoint(leftNode) || constrainedCountFastPathEndpoint(rightNode) ||
		relationship == nil || relationship.Range != nil || relationship.Properties != nil ||
		relationship.Direction == graph.DirectionBoth {
		return CountStoreFastPathDecision{}, false
	}

	bindingSymbol := variableSymbol(relationship.Variable)
	if countArgument != cypher.TokenLiteralAsterisk && countArgument != bindingSymbol {
		return CountStoreFastPathDecision{}, false
	}

	return CountStoreFastPathDecision{
		QueryPartIndex: 0,
		ClauseIndex:    0,
		PatternIndex:   0,
		BindingSymbol:  bindingSymbol,
		Target:         CountStoreFastPathEdge,
		KindSymbols:    kindSymbols(relationship.Kinds),
	}, true
}

func simpleCountProjectionArgument(returnClause *cypher.Return) (string, bool) {
	if returnClause == nil || returnClause.Projection == nil {
		return "", false
	}

	projection := returnClause.Projection
	if projection.Distinct || projection.All || projection.Order != nil || projection.Skip != nil || projection.Limit != nil || len(projection.Items) != 1 {
		return "", false
	}

	projectionItem, ok := projection.Items[0].(*cypher.ProjectionItem)
	if !ok || projectionItem == nil {
		return "", false
	}

	function, ok := projectionItem.Expression.(*cypher.FunctionInvocation)
	if !ok || function == nil || !strings.EqualFold(function.Name, cypher.CountFunction) ||
		function.Distinct || len(function.Namespace) > 0 || len(function.Arguments) != 1 {
		return "", false
	}

	switch argument := function.Arguments[0].(type) {
	case *cypher.Variable:
		if argument == nil {
			return "", false
		}

		return argument.Symbol, true
	case *cypher.RangeQuantifier:
		if argument != nil && argument.Value == cypher.TokenLiteralAsterisk {
			return cypher.TokenLiteralAsterisk, true
		}
	}

	return "", false
}

func constrainedCountFastPathEndpoint(nodePattern *cypher.NodePattern) bool {
	return nodePattern == nil || nodePattern.Variable != nil || len(nodePattern.Kinds) > 0 || nodePattern.Properties != nil
}

func kindSymbols(kinds graph.Kinds) []string {
	if len(kinds) == 0 {
		return nil
	}

	symbols := make([]string, len(kinds))
	for idx, kind := range kinds {
		symbols[idx] = kind.String()
	}

	return symbols
}

func indexBindingTargets(query *cypher.RegularQuery) map[bindingTargetKey]TraversalStepTarget {
	targets := map[bindingTargetKey]TraversalStepTarget{}

	if query == nil || query.SingleQuery == nil {
		return targets
	}

	if query.SingleQuery.MultiPartQuery != nil {
		for queryPartIndex, part := range query.SingleQuery.MultiPartQuery.Parts {
			if part == nil {
				continue
			}

			indexReadingClauseBindingTargets(targets, queryPartIndex, part.ReadingClauses)
		}

		if finalPart := query.SingleQuery.MultiPartQuery.SinglePartQuery; finalPart != nil {
			indexReadingClauseBindingTargets(targets, len(query.SingleQuery.MultiPartQuery.Parts), finalPart.ReadingClauses)
		}
	} else if query.SingleQuery.SinglePartQuery != nil {
		indexReadingClauseBindingTargets(targets, 0, query.SingleQuery.SinglePartQuery.ReadingClauses)
	}

	return targets
}

func indexReadingClauseBindingTargets(targets map[bindingTargetKey]TraversalStepTarget, queryPartIndex int, readingClauses []*cypher.ReadingClause) {
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			patternTarget := PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}

			for stepIndex, step := range traversalStepsForPattern(patternPart) {
				stepTarget := patternTarget.TraversalStep(stepIndex)
				setBindingTarget(targets, queryPartIndex, variableSymbol(step.LeftNode.Variable), stepTarget)
				setBindingTarget(targets, queryPartIndex, variableSymbol(step.Relationship.Variable), stepTarget)
				setBindingTarget(targets, queryPartIndex, variableSymbol(step.RightNode.Variable), stepTarget)
			}
		}
	}
}

func setBindingTarget(targets map[bindingTargetKey]TraversalStepTarget, queryPartIndex int, symbol string, target TraversalStepTarget) {
	if symbol == "" {
		return
	}

	key := bindingTargetKey{
		QueryPartIndex: queryPartIndex,
		Symbol:         symbol,
	}
	if _, exists := targets[key]; !exists {
		targets[key] = target
	}
}

func expansionSuffixPushdownLength(suffixSteps []sourceTraversalStep) int {
	var suffixLength int

	for _, step := range suffixSteps {
		if step.Relationship.Range != nil || step.Relationship.Direction == graph.DirectionBoth {
			break
		}

		suffixLength++
	}

	return suffixLength
}

func declareMatchSymbols(declared map[string]struct{}, match *cypher.Match) {
	if match == nil {
		return
	}

	for _, patternPart := range match.Pattern {
		declarePatternSymbols(declared, patternPart)
	}

	declareWhereSymbols(declared, match)
}

func declarePatternSymbols(declared map[string]struct{}, patternPart *cypher.PatternPart) {
	if patternPart == nil {
		return
	}

	addSymbol(declared, variableSymbol(patternPart.Variable))
	for _, element := range patternPart.PatternElements {
		if element == nil {
			continue
		}

		if nodePattern, isNodePattern := element.AsNodePattern(); isNodePattern {
			addSymbol(declared, variableSymbol(nodePattern.Variable))
		} else if relationshipPattern, isRelationshipPattern := element.AsRelationshipPattern(); isRelationshipPattern {
			addSymbol(declared, variableSymbol(relationshipPattern.Variable))
		}
	}
}

func declareWhereSymbols(declared map[string]struct{}, match *cypher.Match) {
	for _, dependency := range dependenciesForMatch(match) {
		addSymbol(declared, dependency)
	}
}

func nodePatternHasConstraints(nodePattern *cypher.NodePattern) bool {
	return nodePattern != nil && (len(nodePattern.Kinds) > 0 || nodePattern.Properties != nil)
}

func relationshipPatternHasProperties(relationshipPattern *cypher.RelationshipPattern) bool {
	return relationshipPattern != nil && relationshipPattern.Properties != nil
}

func addSymbol(symbols map[string]struct{}, symbol string) {
	if symbol != "" {
		symbols[symbol] = struct{}{}
	}
}

func copyStringSet(values map[string]struct{}) map[string]struct{} {
	copied := make(map[string]struct{}, len(values))
	for value := range values {
		copied[value] = struct{}{}
	}

	return copied
}

func traversalStepsForPattern(patternPart *cypher.PatternPart) []sourceTraversalStep {
	if patternPart == nil {
		return nil
	}

	var (
		steps        []sourceTraversalStep
		leftNode     *cypher.NodePattern
		relationship *cypher.RelationshipPattern
	)

	for _, element := range patternPart.PatternElements {
		if element == nil {
			continue
		}

		if nodePattern, isNodePattern := element.AsNodePattern(); isNodePattern {
			if leftNode == nil {
				leftNode = nodePattern
				continue
			}

			if relationship != nil {
				steps = append(steps, sourceTraversalStep{
					LeftNode:     leftNode,
					Relationship: relationship,
					RightNode:    nodePattern,
				})
			}

			leftNode = nodePattern
			relationship = nil
		} else if relationshipPattern, isRelationshipPattern := element.AsRelationshipPattern(); isRelationshipPattern {
			relationship = relationshipPattern
		}
	}

	return steps
}

func variableSymbol(variable *cypher.Variable) string {
	if variable == nil {
		return ""
	}

	return variable.Symbol
}
