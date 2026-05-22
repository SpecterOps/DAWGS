package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

type sourceTraversalStep struct {
	LeftNode     *cypher.NodePattern
	Relationship *cypher.RelationshipPattern
	RightNode    *cypher.NodePattern
}

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

			if err := appendQueryPartLowerings(&plan, queryPartIndex, part, part.ReadingClauses); err != nil {
				return LoweringPlan{}, err
			}
		}

		if finalPart := query.SingleQuery.MultiPartQuery.SinglePartQuery; finalPart != nil {
			if err := appendQueryPartLowerings(&plan, len(query.SingleQuery.MultiPartQuery.Parts), finalPart, finalPart.ReadingClauses); err != nil {
				return LoweringPlan{}, err
			}
		}
	} else if singlePart := query.SingleQuery.SinglePartQuery; singlePart != nil {
		if err := appendQueryPartLowerings(&plan, 0, singlePart, singlePart.ReadingClauses); err != nil {
			return LoweringPlan{}, err
		}
	}

	appendPredicatePlacementDecisions(&plan, query, predicateAttachments)
	attachPredicatePlacementsToSuffixPushdowns(&plan)
	return plan, nil
}

func appendQueryPartLowerings(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode, readingClauses []*cypher.ReadingClause) error {
	sourceReferences, err := collectReferencedSourceIdentifiers(queryPart)
	if err != nil {
		return err
	}

	appendProjectionPruningDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendLatePathMaterializationDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendExpandIntoDecisions(plan, queryPartIndex, readingClauses)
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

func appendLatePathMaterializationDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, sourceReferences map[string]struct{}) {
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Optional {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			if !referencesSourceIdentifier(sourceReferences, variableSymbol(patternPart.Variable)) {
				continue
			}

			for stepIndex, step := range traversalStepsForPattern(patternPart) {
				target := PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}.TraversalStep(stepIndex)

				if step.Relationship.Range != nil {
					plan.LatePathMaterialization = append(plan.LatePathMaterialization, LatePathMaterializationDecision{
						Target: target,
						Mode:   LatePathMaterializationExpansionPath,
					})
					continue
				}

				mode := LatePathMaterializationPathEdgeID
				if referencesSourceIdentifier(sourceReferences, variableSymbol(step.Relationship.Variable)) {
					mode = LatePathMaterializationEdgeComposite
				}

				plan.LatePathMaterialization = append(plan.LatePathMaterialization, LatePathMaterializationDecision{
					Target: target,
					Mode:   mode,
				})
			}
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
			declaredBeforeRightNode := declaredSymbolsBeforeRightNodes(declaredSymbols, steps)

			for stepIndex, step := range steps {
				if step.Relationship.Range == nil || stepIndex+1 >= len(steps) {
					continue
				}

				if suffixLength := expansionSuffixPushdownLength(steps[stepIndex+1:], declaredBeforeRightNode[stepIndex+1:]); suffixLength > 0 {
					plan.ExpansionSuffixPushdown = append(plan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
						Target: PatternTarget{
							QueryPartIndex: queryPartIndex,
							ClauseIndex:    clauseIndex,
							PatternIndex:   patternIndex,
						}.TraversalStep(stepIndex),
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

func expansionSuffixPushdownLength(suffixSteps []sourceTraversalStep, declaredBeforeRightNode []map[string]struct{}) int {
	var suffixLength int

	for idx, step := range suffixSteps {
		if step.Relationship.Range != nil || step.Relationship.Direction == graph.DirectionBoth {
			break
		}

		if nodeSymbol := variableSymbol(step.RightNode.Variable); nodeSymbol != "" {
			if _, bound := declaredBeforeRightNode[idx][nodeSymbol]; bound && nodePatternHasConstraints(step.RightNode) {
				break
			}
		}

		suffixLength++
	}

	return suffixLength
}

func declaredSymbolsBeforeRightNodes(initial map[string]struct{}, steps []sourceTraversalStep) []map[string]struct{} {
	declared := copyStringSet(initial)
	declaredBeforeRightNode := make([]map[string]struct{}, len(steps))

	for idx, step := range steps {
		addSymbol(declared, variableSymbol(step.LeftNode.Variable))
		addSymbol(declared, variableSymbol(step.Relationship.Variable))
		declaredBeforeRightNode[idx] = copyStringSet(declared)
		addSymbol(declared, variableSymbol(step.RightNode.Variable))
	}

	return declaredBeforeRightNode
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
