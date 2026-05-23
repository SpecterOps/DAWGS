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
	boundSourceSelectivityPredicate
	boundSourceSelectivityUnique
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
	appendAggregateTraversalCountDecisions(&plan, query)
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
	shortestPathSearchSymbols := shortestPathSearchPredicateSymbols(readingClauses)
	appendShortestPathStrategyDecisions(plan, queryPartIndex, readingClauses, shortestPathSearchSymbols)
	appendShortestPathFilterDecisions(plan, queryPartIndex, readingClauses, shortestPathSearchSymbols)
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
	declaredSourceSelectivity := map[string]boundSourceSelectivity{}

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

	if expressionIsStringLikeConstant(value) {
		return boundSourceSelectivityPredicate
	}

	return boundSourceSelectivityNone
}

func expressionIsConstant(expression cypher.Expression) bool {
	switch expression.(type) {
	case *cypher.Literal, *cypher.Parameter:
		return true
	default:
		return false
	}
}

func expressionIsStringLikeConstant(expression cypher.Expression) bool {
	switch typedExpression := expression.(type) {
	case *cypher.Literal:
		if typedExpression == nil || typedExpression.Null {
			return false
		}

		_, isString := typedExpression.Value.(string)
		return isString
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

	rightSymbol := variableSymbol(step.RightNode.Variable)
	leftSymbol := variableSymbol(step.LeftNode.Variable)
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

func boundLeftExpansionDirectionDecisionForStep(
	target TraversalStepTarget,
	patternPart *cypher.PatternPart,
	steps []sourceTraversalStep,
	stepIndex int,
	step sourceTraversalStep,
	declaredEndpoints declaredStepEndpoints,
	rightHasAttachedPredicate bool,
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

	leftSymbol := variableSymbol(step.LeftNode.Variable)
	rightSymbol := variableSymbol(step.RightNode.Variable)
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

	if leftSourceSelectivity == boundSourceSelectivityUnique &&
		!nodePatternHasUniquePropertyConstraint(step.RightNode) &&
		!rightHasAttachedPredicate {
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

	relationship, terminalNode, terminalSymbol, ok := aggregateTraversalMatch(part.ReadingClauses[1], sourceSymbol)
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

func aggregateTraversalMatch(readingClause *cypher.ReadingClause, sourceSymbol string) (*cypher.RelationshipPattern, *cypher.NodePattern, string, bool) {
	if readingClause == nil || readingClause.Match == nil {
		return nil, nil, "", false
	}

	match := readingClause.Match
	if match.Optional || match.Where != nil || len(match.Pattern) != 1 {
		return nil, nil, "", false
	}

	patternPart := match.Pattern[0]
	if patternPart == nil || patternPart.Variable != nil || patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern || len(patternPart.PatternElements) != 3 {
		return nil, nil, "", false
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
		return nil, nil, "", false
	}

	return relationship, rightNode, rightNode.Variable.Symbol, true
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

	finalProjection := aggregateTraversalFinalProjectionShape{
		SourceAlias: sourceSymbol,
		CountAlias:  countAlias,
	}

	sourceSeen := false
	countSeen := false
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
