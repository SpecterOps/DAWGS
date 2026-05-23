package optimize

import (
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

type sourceTraversalStep struct {
	LeftNode     *cypher.NodePattern
	Relationship *cypher.RelationshipPattern
	RightNode    *cypher.NodePattern
}

const (
	traversalDirectionReasonRightBound       = "right_bound"
	traversalDirectionReasonRightConstrained = "right_constrained"
	traversalDirectionReasonRightPredicate   = "right_predicate"

	shortestPathStrategyReasonBoundEndpointPairs = "bound_endpoint_pairs"
	shortestPathStrategyReasonEndpointPredicates = "endpoint_predicates"

	shortestPathFilterReasonTerminalPredicate      = "terminal_predicate"
	shortestPathFilterReasonEndpointPairPredicates = "endpoint_pair_predicates"
)

func BuildLoweringPlan(query *cypher.RegularQuery, predicateAttachments []PredicateAttachment) (LoweringPlan, error) {
	if query == nil || query.SingleQuery == nil {
		return LoweringPlan{}, nil
	}

	var plan LoweringPlan

	if query.SingleQuery.MultiPartQuery != nil {
		for queryPartIndex, part := range query.SingleQuery.MultiPartQuery.Parts {
			if part == nil {
				continue
			}

			if err := appendQueryPartLowerings(&plan, queryPartIndex, part, part.ReadingClauses, predicateAttachments); err != nil {
				return LoweringPlan{}, err
			}
		}

		if finalPart := query.SingleQuery.MultiPartQuery.SinglePartQuery; finalPart != nil {
			if err := appendQueryPartLowerings(&plan, len(query.SingleQuery.MultiPartQuery.Parts), finalPart, finalPart.ReadingClauses, predicateAttachments); err != nil {
				return LoweringPlan{}, err
			}
		}
	} else if singlePart := query.SingleQuery.SinglePartQuery; singlePart != nil {
		if err := appendQueryPartLowerings(&plan, 0, singlePart, singlePart.ReadingClauses, predicateAttachments); err != nil {
			return LoweringPlan{}, err
		}
	}

	appendPredicatePlacementDecisions(&plan, query, predicateAttachments)
	attachPredicatePlacementsToSuffixPushdowns(&plan)
	appendCountStoreFastPathDecisions(&plan, query)
	return plan, nil
}

func appendQueryPartLowerings(
	plan *LoweringPlan,
	queryPartIndex int,
	queryPart cypher.SyntaxNode,
	readingClauses []*cypher.ReadingClause,
	predicateAttachments []PredicateAttachment,
) error {
	sourceReferences, err := collectReferencedSourceIdentifiers(queryPart)
	if err != nil {
		return err
	}

	appendProjectionPruningDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendLatePathMaterializationDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendPatternPredicateProjectionLowerings(plan, queryPartIndex, queryPart, sourceReferences)
	appendPatternPredicatePlacementDecisions(plan, queryPartIndex, queryPart)
	appendExpandIntoDecisions(plan, queryPartIndex, readingClauses)
	appendTraversalDirectionDecisions(plan, queryPartIndex, readingClauses, bindingPredicateSymbols(predicateAttachments, queryPartIndex))
	appendShortestPathStrategyDecisions(plan, queryPartIndex, readingClauses, bindingPredicateSymbols(predicateAttachments, queryPartIndex))
	appendShortestPathFilterDecisions(plan, queryPartIndex, readingClauses, bindingPredicateSymbols(predicateAttachments, queryPartIndex))
	appendLimitPushdownDecisions(plan, queryPartIndex, queryPart, readingClauses)
	appendExpansionSuffixPushdownDecisions(plan, queryPartIndex, readingClauses)
	return nil
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
		decision := ProjectionPruningDecision{
			Target:                   target.TraversalStep(stepIndex),
			ReferencedSymbols:        sortedMapKeys(sourceReferences),
			PatternBindingReferenced: pathReferenced,
		}

		edgeReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(step.Relationship.Variable))
		var hasPruning bool
		if step.Relationship.Range != nil {
			decision.OmitRelationship = !edgeReferenced
			decision.OmitPathBinding = !pathReferenced
			hasPruning = decision.OmitRelationship || decision.OmitPathBinding
		} else {
			leftReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(step.LeftNode.Variable))
			rightReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(step.RightNode.Variable))

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
	for predicateIndex, predicate := range patternPredicatesInQueryPart(queryPart) {
		patternPart := patternPartForPredicate(predicate)
		steps := traversalStepsForPattern(patternPart)
		if len(steps) == 0 {
			continue
		}

		target := PatternTarget{
			QueryPartIndex: queryPartIndex,
			PatternIndex:   predicateIndex,
			Predicate:      true,
			PredicateIndex: predicateIndex,
		}

		appendPatternProjectionPruningDecisions(plan, target, patternPart, steps, sourceReferences)
		appendPatternLatePathMaterializationDecisions(plan, target, patternPart, steps, sourceReferences)
	}
}

func appendPatternPredicatePlacementDecisions(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode) {
	for predicateIndex, predicate := range patternPredicatesInQueryPart(queryPart) {
		patternPart := patternPartForPredicate(predicate)
		steps := traversalStepsForPattern(patternPart)
		if len(steps) != 1 {
			continue
		}

		step := steps[0]
		if step.Relationship == nil ||
			step.Relationship.Direction != graph.DirectionBoth ||
			relationshipPatternHasConstraints(step.Relationship) ||
			nodePatternHasConstraints(step.LeftNode) ||
			nodePatternHasConstraints(step.RightNode) {
			continue
		}

		if variableSymbol(step.Relationship.Variable) != "" || variableSymbol(step.RightNode.Variable) != "" {
			continue
		}

		target := PatternTarget{
			QueryPartIndex: queryPartIndex,
			PatternIndex:   predicateIndex,
			Predicate:      true,
			PredicateIndex: predicateIndex,
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

		if step.Relationship.Range != nil {
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
			steps := traversalStepsForPattern(patternPart)
			declaredEndpoints := declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)

			for stepIndex, step := range steps {
				if step.Relationship.Range != nil {
					continue
				}

				leftSymbol := variableSymbol(step.LeftNode.Variable)
				rightSymbol := variableSymbol(step.RightNode.Variable)
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
	declared := copyStringSet(initial)
	endpoints := make([]declaredStepEndpoints, len(steps))

	for idx, step := range steps {
		endpoints[idx].BeforeLeftNode = copyStringSet(declared)
		addSymbol(declared, variableSymbol(step.LeftNode.Variable))
		addSymbol(declared, variableSymbol(step.Relationship.Variable))
		endpoints[idx].BeforeRightNode = copyStringSet(declared)
		addSymbol(declared, variableSymbol(step.RightNode.Variable))
	}

	return endpoints
}

func appendTraversalDirectionDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, predicateConstrainedSymbols map[string]struct{}) {
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
			steps := traversalStepsForPattern(patternPart)
			declaredEndpoints := declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
			patternTarget := PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}

			for stepIndex, step := range steps {
				if decision, shouldFlip := traversalDirectionDecisionForStep(
					patternTarget.TraversalStep(stepIndex),
					stepIndex,
					step,
					declaredEndpoints[stepIndex],
					referencesSourceIdentifier(predicateConstrainedSymbols, variableSymbol(step.LeftNode.Variable)),
					referencesSourceIdentifier(predicateConstrainedSymbols, variableSymbol(step.RightNode.Variable)),
				); shouldFlip {
					plan.TraversalDirection = append(plan.TraversalDirection, decision)
				}
			}

			declarePatternSymbols(declaredSymbols, patternPart)
		}

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

	rightSymbol := variableSymbol(step.RightNode.Variable)
	if rightSymbol != "" {
		if _, rightBound := declaredEndpoints.BeforeRightNode[rightSymbol]; rightBound {
			if rightSymbol == variableSymbol(step.LeftNode.Variable) {
				return TraversalDirectionDecision{}, false
			}

			return TraversalDirectionDecision{
				Target: target,
				Flip:   true,
				Reason: traversalDirectionReasonRightBound,
			}, true
		}
	}

	leftConstrained := nodePatternHasConstraints(step.LeftNode) || leftHasAttachedPredicate
	rightConstrained := nodePatternHasConstraints(step.RightNode) || rightHasAttachedPredicate

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

			steps := traversalStepsForPattern(patternPart)
			declaredEndpoints := declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
			patternTarget := PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}

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
	leftSymbol := variableSymbol(step.LeftNode.Variable)
	rightSymbol := variableSymbol(step.RightNode.Variable)

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

			steps := traversalStepsForPattern(patternPart)
			declaredEndpoints := declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)
			patternTarget := PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}

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
	leftSymbol := variableSymbol(step.LeftNode.Variable)
	rightSymbol := variableSymbol(step.RightNode.Variable)
	if rightSymbol != "" {
		if _, rightBound := declaredEndpoints.BeforeRightNode[rightSymbol]; rightBound {
			return ShortestPathFilterDecision{}, false
		}
	}

	leftSearchConstrained := endpointHasSearchConstraint(step.LeftNode, leftSymbol, predicateConstrainedSymbols)
	rightSearchConstrained := endpointHasSearchConstraint(step.RightNode, rightSymbol, predicateConstrainedSymbols)
	if !rightSearchConstrained {
		return ShortestPathFilterDecision{}, false
	}

	if hasShortestPathBidirectionalStrategy(plan, target) && leftSearchConstrained {
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

func appendExpansionSuffixPushdownDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause) {
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
			steps := traversalStepsForPattern(patternPart)
			declaredEndpoints := declaredSymbolsBeforeStepEndpoints(declaredSymbols, steps)

			for stepIndex, step := range steps {
				if step.Relationship.Range == nil || stepIndex+1 >= len(steps) {
					continue
				}

				target := PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}.TraversalStep(stepIndex)
				if hasTraversalDirectionFlip(plan, target) || expansionStepMayFlipForConstraintBalance(stepIndex, step, declaredEndpoints[stepIndex]) {
					continue
				}

				if suffixLength := expansionSuffixPushdownLength(steps[stepIndex+1:]); suffixLength > 0 {
					plan.ExpansionSuffixPushdown = append(plan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
						Target:          target,
						SuffixLength:    suffixLength,
						SuffixStartStep: stepIndex + 1,
						SuffixEndStep:   stepIndex + suffixLength,
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

func relationshipPatternHasConstraints(relationshipPattern *cypher.RelationshipPattern) bool {
	return relationshipPattern != nil && (len(relationshipPattern.Kinds) > 0 || relationshipPattern.Properties != nil)
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
