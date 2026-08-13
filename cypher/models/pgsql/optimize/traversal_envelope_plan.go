package optimize

import (
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type endpointResolutionCandidate struct {
	class            EndpointResolutionClass
	property         string
	staticValueCount int
	parameterizedSet bool
	rank             int
}

func endpointResolutionCaps() EndpointResolutionCaps {
	return EndpointResolutionCaps{
		SingletonLimit:    EndpointResolutionSingletonLimit,
		SingletonSentinel: EndpointResolutionSingletonSentinel,
		SmallSetLimit:     EndpointResolutionSmallSetLimit,
		SmallSetSentinel:  EndpointResolutionSmallSetSentinel,
	}
}

func endpointResolutionInput(symbol string, node *cypher.NodePattern, where *cypher.Where) EndpointResolutionInput {
	input := EndpointResolutionInput{
		Symbol: symbol,
		Class:  EndpointResolutionClassUnsupported,
	}
	if symbol == "" {
		return input
	}

	var candidates []endpointResolutionCandidate
	if where != nil {
		for _, expression := range where.Expressions {
			for _, term := range cypherConjunctionTerms(expression) {
				if candidate, found := endpointResolutionCandidateForTerm(term, symbol); found {
					candidates = append(candidates, candidate)
				}
			}
		}
	}
	candidates = append(candidates, inlineEndpointResolutionCandidates(node, symbol)...)
	if len(candidates) == 0 {
		return input
	}

	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.rank > best.rank {
			best = candidate
		}
	}
	input.Class = best.class
	input.Property = best.property
	input.StaticValueCount = best.staticValueCount
	input.ParameterizedSet = best.parameterizedSet
	switch best.class {
	case EndpointResolutionClassIDEquality, EndpointResolutionClassUniquePropertyEquality:
		input.Limit = EndpointResolutionSingletonLimit
		input.Sentinel = EndpointResolutionSingletonSentinel
	case EndpointResolutionClassNonUniquePropertyEquality, EndpointResolutionClassExplicitSmallSet:
		input.Limit = EndpointResolutionSmallSetLimit
		input.Sentinel = EndpointResolutionSmallSetSentinel
	}

	return input
}

func endpointResolutionCandidateForTerm(expression cypher.Expression, symbol string) (endpointResolutionCandidate, bool) {
	expression = unwrapCypherParenthetical(expression)
	comparison, ok := expression.(*cypher.Comparison)
	if !ok || comparison == nil || len(comparison.Partials) != 1 || comparison.Partials[0] == nil {
		return endpointResolutionCandidate{}, false
	}
	partial := comparison.Partials[0]
	switch partial.Operator {
	case cypher.OperatorEquals:
		if identitySymbol, found := identityFunctionSymbol(comparison.Left); found && identitySymbol == symbol && expressionIsConstant(partial.Right) {
			return endpointResolutionCandidate{class: EndpointResolutionClassIDEquality, staticValueCount: 1, rank: 5}, true
		}
		if identitySymbol, found := identityFunctionSymbol(partial.Right); found && identitySymbol == symbol && expressionIsConstant(comparison.Left) {
			return endpointResolutionCandidate{class: EndpointResolutionClassIDEquality, staticValueCount: 1, rank: 5}, true
		}
		if propertySymbol, property, found := propertyLookupSymbol(comparison.Left); found && propertySymbol == symbol && expressionIsConstant(partial.Right) {
			return propertyEndpointResolutionCandidate(property), true
		}
		if propertySymbol, property, found := propertyLookupSymbol(partial.Right); found && propertySymbol == symbol && expressionIsConstant(comparison.Left) {
			return propertyEndpointResolutionCandidate(property), true
		}

	case cypher.OperatorIn:
		values, parameterized, recognized := explicitSetCardinality(partial.Right)
		if !recognized {
			return endpointResolutionCandidate{}, false
		}
		if identitySymbol, found := identityFunctionSymbol(comparison.Left); found && identitySymbol == symbol {
			return endpointResolutionCandidate{class: EndpointResolutionClassExplicitSmallSet, staticValueCount: values, parameterizedSet: parameterized, rank: 4}, true
		}
		if propertySymbol, property, found := propertyLookupSymbol(comparison.Left); found && propertySymbol == symbol {
			return endpointResolutionCandidate{class: EndpointResolutionClassExplicitSmallSet, property: property, staticValueCount: values, parameterizedSet: parameterized, rank: 4}, true
		}
	}

	return endpointResolutionCandidate{}, false
}

func propertyEndpointResolutionCandidate(property string) endpointResolutionCandidate {
	// A property name is not a uniqueness proof. Until graph-schema metadata is
	// available to the optimizer, every property equality uses the bounded
	// non-unique envelope and its cap+1 runtime sentinel.
	return endpointResolutionCandidate{
		class:            EndpointResolutionClassNonUniquePropertyEquality,
		property:         property,
		staticValueCount: 1,
		rank:             2,
	}
}

func inlineEndpointResolutionCandidates(node *cypher.NodePattern, symbol string) []endpointResolutionCandidate {
	if node == nil || variableSymbol(node.Variable) != symbol {
		return nil
	}
	properties, ok := node.Properties.(*cypher.Properties)
	if !ok || properties == nil || properties.Parameter != nil {
		return nil
	}

	candidates := make([]endpointResolutionCandidate, 0, len(properties.Map))
	for property, value := range properties.Map {
		if expressionIsConstant(value) {
			candidates = append(candidates, propertyEndpointResolutionCandidate(property))
		}
	}
	return candidates
}

func constantListCardinality(expression cypher.Expression) (int, bool) {
	literal, ok := unwrapCypherParenthetical(expression).(*cypher.ListLiteral)
	if !ok || literal == nil || len(*literal) == 0 {
		return 0, false
	}
	for _, value := range *literal {
		if !expressionIsConstant(value) {
			return 0, false
		}
	}
	return len(*literal), true
}

// explicitSetCardinality recognizes both statically enumerable list literals
// and parameterized sets. Parameter contents remain runtime evidence and must
// pass the same 32/33 bounded-resolution sentinel as a literal set.
func explicitSetCardinality(expression cypher.Expression) (values int, parameterized, recognized bool) {
	expression = unwrapCypherParenthetical(expression)
	if _, ok := expression.(*cypher.Parameter); ok {
		return 0, true, true
	}
	values, recognized = constantListCardinality(expression)
	return values, false, recognized
}

func endpointInputWithinStaticCap(input EndpointResolutionInput) bool {
	return input.Class != EndpointResolutionClassExplicitSmallSet || input.StaticValueCount <= int(EndpointResolutionSmallSetLimit)
}

func endpointResolutionClassSupported(class EndpointResolutionClass) bool {
	return class != "" && class != EndpointResolutionClassUnsupported && class != EndpointResolutionClassCorrelatedPair
}

func endpointPairPredicateCorrelated(where *cypher.Where, leftSymbol, rightSymbol string) bool {
	if where == nil || leftSymbol == "" || rightSymbol == "" {
		return false
	}
	for _, expression := range where.Expressions {
		for _, term := range cypherConjunctionTerms(expression) {
			dependencies := sortedDependencies(term)
			if stringSliceContains(dependencies, leftSymbol) && stringSliceContains(dependencies, rightSymbol) {
				return true
			}
		}
	}
	return false
}

func appendEndpointResolutionDecisions(
	plan *LoweringPlan,
	queryPartIndex int,
	queryPart cypher.SyntaxNode,
	readingClauses []*cypher.ReadingClause,
	initialDeclaredSymbols map[string]struct{},
) {
	_, updatingClauses := queryPartProjection(queryPart)
	declaredSymbols := copyStringSet(initialDeclaredSymbols)
	hasUnwind := false
	for _, readingClause := range readingClauses {
		if readingClause != nil && readingClause.Unwind != nil {
			hasUnwind = true
		}
	}

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}
		for patternIndex, patternPart := range readingClause.Match.Pattern {
			if patternPart == nil || (!patternPart.ShortestPathPattern && !patternPart.AllShortestPathsPattern) {
				declarePatternSymbols(declaredSymbols, patternPart)
				continue
			}
			steps := traversalStepsForPattern(patternPart)
			for stepIndex, step := range steps {
				if step.Relationship == nil || step.Relationship.Range == nil || step.LeftNode == nil || step.RightNode == nil {
					continue
				}
				leftSymbol := variableSymbol(step.LeftNode.Variable)
				rightSymbol := variableSymbol(step.RightNode.Variable)
				root := endpointResolutionInput(leftSymbol, step.LeftNode, readingClause.Match.Where)
				terminal := endpointResolutionInput(rightSymbol, step.RightNode, readingClause.Match.Where)
				_, leftPreviouslyBound := declaredSymbols[leftSymbol]
				_, rightPreviouslyBound := declaredSymbols[rightSymbol]
				correlated := queryPartIndex > 0 || hasUnwind || len(readingClause.Match.Pattern) != 1 || leftPreviouslyBound || rightPreviouslyBound || endpointPairPredicateCorrelated(readingClause.Match.Where, leftSymbol, rightSymbol)
				classesSupported := endpointResolutionClassSupported(root.Class) && endpointResolutionClassSupported(terminal.Class)
				withinCaps := endpointInputWithinStaticCap(root) && endpointInputWithinStaticCap(terminal)
				facts := []EndpointResolutionEligibilityFact{
					{Name: "supported_shortest_path_mode", Eligible: patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern},
					{Name: "single_traversal_step", Eligible: len(steps) == 1 && len(patternPart.PatternElements) == 3},
					{Name: "read_only", Eligible: updatingClauses == 0},
					{Name: "non_optional", Eligible: !readingClause.Match.Optional},
					{Name: "bounded_endpoint_classes", Eligible: classesSupported},
					{Name: "within_static_endpoint_caps", Eligible: withinCaps},
					{Name: "uncorrelated_pair", Eligible: !correlated},
				}
				eligible := endpointResolutionFactsEligible(facts)
				fallbackReason := EndpointResolutionFallbackPlannedOnly
				switch {
				case updatingClauses != 0:
					fallbackReason = EndpointResolutionFallbackMutation
				case readingClause.Match.Optional:
					fallbackReason = EndpointResolutionFallbackOptionalMatch
				case correlated:
					fallbackReason = EndpointResolutionFallbackCorrelatedPair
				case !withinCaps:
					fallbackReason = EndpointResolutionFallbackSmallSetOverflow
				case !classesSupported || len(steps) != 1 || len(patternPart.PatternElements) != 3:
					fallbackReason = EndpointResolutionFallbackUnsupported
				}

				plannedClasses := []EndpointResolutionClass{root.Class, terminal.Class}
				pairClass := EndpointResolutionClass("")
				if correlated {
					pairClass = EndpointResolutionClassCorrelatedPair
					plannedClasses = append(plannedClasses, pairClass)
				}
				family := "SP"
				if patternPart.AllShortestPathsPattern {
					family = "ASP"
				}
				plan.EndpointResolution = append(plan.EndpointResolution, EndpointResolutionDecision{
					Target: PatternTarget{
						QueryPartIndex: queryPartIndex,
						ClauseIndex:    clauseIndex,
						PatternIndex:   patternIndex,
					}.TraversalStep(stepIndex),
					Family:               family,
					Root:                 root,
					Terminal:             terminal,
					PairClass:            pairClass,
					PlannedClasses:       plannedClasses,
					Caps:                 endpointResolutionCaps(),
					PlannedCandidates:    []EndpointResolutionPlan{EndpointResolutionPlanIncumbent, EndpointResolutionPlanBounded},
					CandidatePlan:        EndpointResolutionPlanBounded,
					SelectedPlan:         EndpointResolutionPlanIncumbent,
					FallbackPlan:         EndpointResolutionPlanIncumbent,
					EligibilityFacts:     facts,
					StructurallyEligible: eligible,
					StaticallyEligible:   false,
					SelectionMode:        "analysis_only",
					SelectorVersion:      "endpoint-resolution-v1",
					FallbackReason:       fallbackReason,
				})
			}
			declarePatternSymbols(declaredSymbols, patternPart)
		}
		declareWhereSymbols(declaredSymbols, readingClause.Match)
	}
}

func endpointResolutionFactsEligible(facts []EndpointResolutionEligibilityFact) bool {
	for _, fact := range facts {
		if !fact.Eligible {
			return false
		}
	}
	return true
}

func setEndpointResolutionFact(decision *EndpointResolutionDecision, name string, eligible bool) {
	for idx := range decision.EligibilityFacts {
		if decision.EligibilityFacts[idx].Name == name {
			decision.EligibilityFacts[idx].Eligible = eligible
			return
		}
	}
}

type traversalPredicateClassification struct {
	class         TraversalPredicateClass
	bindingSymbol string
	relevant      bool
	correlated    bool
}

func classifyTraversalPredicate(
	expression cypher.Expression,
	pathSymbol string,
	nodeSymbols map[string]struct{},
	relationshipSymbols map[string]struct{},
) traversalPredicateClassification {
	expression = unwrapCypherParenthetical(expression)
	if quantifier, ok := expression.(*cypher.Quantifier); ok {
		return classifyTraversalQuantifier(quantifier, pathSymbol)
	}

	dependencies := sortedDependencies(expression)
	if pathSymbol != "" && stringSliceContains(dependencies, pathSymbol) {
		return traversalPredicateClassification{
			class:      TraversalPredicateClassWholePath,
			relevant:   true,
			correlated: len(dependencies) > 1,
		}
	}

	var nodeDependencies, relationshipDependencies int
	for _, dependency := range dependencies {
		if _, found := nodeSymbols[dependency]; found {
			nodeDependencies++
		}
		if _, found := relationshipSymbols[dependency]; found {
			relationshipDependencies++
		}
	}
	relevantDependencies := nodeDependencies + relationshipDependencies
	if relevantDependencies == 0 {
		return traversalPredicateClassification{}
	}
	correlated := len(dependencies) != 1 || relevantDependencies != 1
	// A WHERE reference to an endpoint symbol is a boundary predicate, and a
	// variable-length relationship binding can be list/path-valued. Neither
	// syntax proves evaluation against every recursive step. Only explicit
	// path quantifiers and inline relationship properties are classified as
	// step-evaluable below.
	return traversalPredicateClassification{
		class:      TraversalPredicateClassUnsupported,
		relevant:   true,
		correlated: correlated,
	}
}

func classifyTraversalQuantifier(quantifier *cypher.Quantifier, pathSymbol string) traversalPredicateClassification {
	if quantifier == nil || quantifier.Filter == nil || quantifier.Filter.Specifier == nil || quantifier.Filter.Specifier.Variable == nil {
		return traversalPredicateClassification{class: TraversalPredicateClassUnsupported, relevant: true}
	}
	function, ok := quantifier.Filter.Specifier.Expression.(*cypher.FunctionInvocation)
	if !ok || function == nil || function.NumArguments() != 1 {
		return traversalPredicateClassification{class: TraversalPredicateClassUnsupported, relevant: true}
	}
	pathVariable, ok := function.Arguments[0].(*cypher.Variable)
	if !ok || pathVariable == nil || pathSymbol == "" || pathVariable.Symbol != pathSymbol {
		return traversalPredicateClassification{}
	}
	bindingSymbol := quantifier.Filter.Specifier.Variable.Symbol
	bodyDependencies := sortedDependencies(quantifier.Filter.Where)
	correlated := false
	for _, dependency := range bodyDependencies {
		if dependency != bindingSymbol {
			correlated = true
		}
	}
	collectionNodes := strings.EqualFold(function.Name, cypher.NodesFunction)
	collectionRelationships := strings.EqualFold(function.Name, cypher.RelationshipsFunction)
	if !collectionNodes && !collectionRelationships {
		return traversalPredicateClassification{class: TraversalPredicateClassWholePath, bindingSymbol: bindingSymbol, relevant: true, correlated: correlated}
	}
	if correlated || quantifier.Filter.Where == nil || !traversalPredicateUsesOnlySafeFunctions(quantifier.Filter.Where, collectionRelationships) {
		return traversalPredicateClassification{class: TraversalPredicateClassWholePath, bindingSymbol: bindingSymbol, relevant: true, correlated: correlated}
	}

	classification := traversalPredicateClassification{bindingSymbol: bindingSymbol, relevant: true}
	switch {
	case collectionNodes && quantifier.Type == cypher.QuantifierTypeAll:
		classification.class = TraversalPredicateClassUniversalAllNodes
	case collectionNodes && quantifier.Type == cypher.QuantifierTypeNone:
		classification.class = TraversalPredicateClassUniversalNoneNodes
	case collectionRelationships && quantifier.Type == cypher.QuantifierTypeAll:
		classification.class = TraversalPredicateClassUniversalAllRelationships
	case collectionRelationships && quantifier.Type == cypher.QuantifierTypeNone:
		classification.class = TraversalPredicateClassUniversalNoneRelationships
	default:
		classification.class = TraversalPredicateClassWholePath
	}
	return classification
}

func traversalPredicateUsesOnlySafeFunctions(node cypher.SyntaxNode, relationshipBinding bool) bool {
	safe := true
	_ = walk.Cypher(node, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		function, ok := node.(*cypher.FunctionInvocation)
		if !ok || function == nil {
			return
		}
		if strings.EqualFold(function.Name, cypher.IdentityFunction) {
			return
		}
		if relationshipBinding && strings.EqualFold(function.Name, cypher.EdgeTypeFunction) {
			return
		}
		safe = false
	}))
	return safe
}

func traversalPredicateClassStepEvaluable(class TraversalPredicateClass) bool {
	switch class {
	case TraversalPredicateClassStepLocalNode,
		TraversalPredicateClassStepLocalRelationship,
		TraversalPredicateClassUniversalAllNodes,
		TraversalPredicateClassUniversalNoneNodes,
		TraversalPredicateClassUniversalAllRelationships,
		TraversalPredicateClassUniversalNoneRelationships:
		return true
	default:
		return false
	}
}

func appendTraversalPredicateDecisions(
	plan *LoweringPlan,
	queryPartIndex int,
	queryPart cypher.SyntaxNode,
	readingClauses []*cypher.ReadingClause,
) {
	_, updatingClauses := queryPartProjection(queryPart)
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}
		for patternIndex, patternPart := range readingClause.Match.Pattern {
			if patternPart == nil {
				continue
			}
			pathSymbol := variableSymbol(patternPart.Variable)
			steps := traversalStepsForPattern(patternPart)
			for stepIndex, step := range steps {
				if step.Relationship == nil || step.Relationship.Range == nil {
					continue
				}
				target := PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}.TraversalStep(stepIndex)
				nodeSymbols := map[string]struct{}{}
				if symbol := variableSymbol(step.LeftNode.Variable); symbol != "" {
					nodeSymbols[symbol] = struct{}{}
				}
				if symbol := variableSymbol(step.RightNode.Variable); symbol != "" {
					nodeSymbols[symbol] = struct{}{}
				}
				relationshipSymbols := map[string]struct{}{}
				if symbol := variableSymbol(step.Relationship.Variable); symbol != "" {
					relationshipSymbols[symbol] = struct{}{}
				}

				predicateIndex := 0
				if readingClause.Match.Where != nil {
					for _, whereExpression := range readingClause.Match.Where.Expressions {
						for _, term := range cypherConjunctionTerms(whereExpression) {
							classification := classifyTraversalPredicate(term, pathSymbol, nodeSymbols, relationshipSymbols)
							if !classification.relevant {
								continue
							}
							appendTraversalPredicateDecision(plan, target, predicateIndex, "where", pathSymbol, classification, sortedDependencies(term), updatingClauses == 0, !readingClause.Match.Optional)
							predicateIndex++
						}
					}
				}
				if step.Relationship.Properties != nil {
					classification := traversalPredicateClassification{class: TraversalPredicateClassStepLocalRelationship, relevant: true}
					if !inlinePropertiesStepLocal(step.Relationship.Properties) {
						classification.class = TraversalPredicateClassUnsupported
						classification.correlated = len(sortedDependencies(step.Relationship.Properties)) > 0
					}
					appendTraversalPredicateDecision(plan, target, predicateIndex, "relationship_pattern", pathSymbol, classification, sortedDependencies(step.Relationship.Properties), updatingClauses == 0, !readingClause.Match.Optional)
					predicateIndex++
				}
				for _, node := range []*cypher.NodePattern{step.LeftNode, step.RightNode} {
					if node == nil || node.Properties == nil {
						continue
					}
					// Node-pattern properties constrain the pattern boundary; they
					// are not predicates over every node visited by a variable range.
					classification := traversalPredicateClassification{
						class:      TraversalPredicateClassUnsupported,
						relevant:   true,
						correlated: len(sortedDependencies(node.Properties)) > 0,
					}
					appendTraversalPredicateDecision(plan, target, predicateIndex, "node_pattern", pathSymbol, classification, sortedDependencies(node.Properties), updatingClauses == 0, !readingClause.Match.Optional)
					predicateIndex++
				}
			}
		}
	}
}

func inlinePropertiesStepLocal(expression cypher.Expression) bool {
	properties, ok := expression.(*cypher.Properties)
	if !ok || properties == nil || properties.Parameter != nil {
		return false
	}
	for _, value := range properties.Map {
		if !expressionIsConstant(value) {
			return false
		}
	}
	return true
}

func appendTraversalPredicateDecision(
	plan *LoweringPlan,
	target TraversalStepTarget,
	predicateIndex int,
	source, pathSymbol string,
	classification traversalPredicateClassification,
	referencedSymbols []string,
	readOnly, nonOptional bool,
) {
	stepEvaluable := traversalPredicateClassStepEvaluable(classification.class)
	facts := []TraversalPredicateEligibilityFact{
		{Name: "read_only", Eligible: readOnly},
		{Name: "non_optional", Eligible: nonOptional},
		{Name: "step_evaluable", Eligible: stepEvaluable},
		{Name: "uncorrelated", Eligible: !classification.correlated},
	}
	eligible := traversalPredicateFactsEligible(facts)
	fallbackReason := TraversalPredicateFallbackPlannedOnly
	switch {
	case !readOnly:
		fallbackReason = TraversalPredicateFallbackMutation
	case !nonOptional:
		fallbackReason = TraversalPredicateFallbackOptional
	case classification.correlated:
		fallbackReason = TraversalPredicateFallbackCorrelation
	case classification.class == TraversalPredicateClassWholePath:
		fallbackReason = TraversalPredicateFallbackWholePath
	case !stepEvaluable:
		fallbackReason = TraversalPredicateFallbackUnsupported
	}
	plannedCandidates := []TraversalPredicatePlan{TraversalPredicatePlanIncumbent}
	candidatePlan := TraversalPredicatePlan("")
	if stepEvaluable {
		candidatePlan = TraversalPredicatePlanStep
		plannedCandidates = append(plannedCandidates, candidatePlan)
	}
	plan.TraversalPredicate = append(plan.TraversalPredicate, TraversalPredicateDecision{
		Target:               target,
		PredicateIndex:       predicateIndex,
		Source:               source,
		Class:                classification.class,
		PathSymbol:           pathSymbol,
		BindingSymbol:        classification.bindingSymbol,
		ReferencedSymbols:    referencedSymbols,
		PlannedCandidates:    plannedCandidates,
		CandidatePlan:        candidatePlan,
		SelectedPlan:         TraversalPredicatePlanIncumbent,
		FallbackPlan:         TraversalPredicatePlanIncumbent,
		EligibilityFacts:     facts,
		StructurallyEligible: eligible,
		StaticallyEligible:   false,
		SelectionMode:        "analysis_only",
		ClassifierVersion:    "traversal-predicate-v1",
		FallbackReason:       fallbackReason,
	})
}

func traversalPredicateFactsEligible(facts []TraversalPredicateEligibilityFact) bool {
	for _, fact := range facts {
		if !fact.Eligible {
			return false
		}
	}
	return true
}

func setTraversalPredicateFact(decision *TraversalPredicateDecision, name string, eligible bool) {
	for idx := range decision.EligibilityFacts {
		if decision.EligibilityFacts[idx].Name == name {
			decision.EligibilityFacts[idx].Eligible = eligible
			return
		}
	}
}

func finalizeTraversalEnvelopeDecisions(plan *LoweringPlan, query *cypher.RegularQuery) {
	if plan == nil || query == nil || query.SingleQuery == nil {
		return
	}
	readOnly := statementUpdatingClauseCount(query) == 0
	for idx := range plan.EndpointResolution {
		decision := &plan.EndpointResolution[idx]
		setEndpointResolutionFact(decision, "read_only", readOnly)
		decision.StructurallyEligible = endpointResolutionFactsEligible(decision.EligibilityFacts)
		decision.StaticallyEligible = false
		if !readOnly {
			decision.FallbackReason = EndpointResolutionFallbackMutation
		}
	}
	for idx := range plan.TraversalPredicate {
		decision := &plan.TraversalPredicate[idx]
		setTraversalPredicateFact(decision, "read_only", readOnly)
		decision.StructurallyEligible = traversalPredicateFactsEligible(decision.EligibilityFacts)
		decision.StaticallyEligible = false
		if !readOnly {
			decision.FallbackReason = TraversalPredicateFallbackMutation
		}
	}
}

func statementUpdatingClauseCount(query *cypher.RegularQuery) int {
	if query == nil || query.SingleQuery == nil {
		return 0
	}
	count := 0
	if multiPart := query.SingleQuery.MultiPartQuery; multiPart != nil {
		for _, part := range multiPart.Parts {
			if part != nil {
				count += len(part.UpdatingClauses)
			}
		}
		if finalPart := multiPart.SinglePartQuery; finalPart != nil {
			count += len(finalPart.UpdatingClauses)
		}
	} else if singlePart := query.SingleQuery.SinglePartQuery; singlePart != nil {
		count += len(singlePart.UpdatingClauses)
	}
	return count
}

func unwrapCypherParenthetical(expression cypher.Expression) cypher.Expression {
	for {
		parenthetical, ok := expression.(*cypher.Parenthetical)
		if !ok || parenthetical == nil {
			return expression
		}
		expression = parenthetical.Expression
	}
}

func stringSliceContains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
