package translate

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

// selectAutomaticBudgetedAllShortestPaths normalizes builder-style positive
// relationship-kind predicates only when fresh planning can select V2. Other
// queries retain their original AST and legacy SQL, including observed r lists,
// relationship properties, explicit policy contexts, and forced canaries. A
// successful normalized candidate replaces the supplied plan with its owned AST.
func selectAutomaticBudgetedAllShortestPaths(plan *optimize.Plan, query *cypher.RegularQuery) (bool, error) {
	if normalized, changed := normalizeBudgetedAllShortestKinds(query); changed {
		if candidate, err := optimize.Optimize(normalized); err != nil {
			return false, err
		} else if selectBudgetedAllShortestPaths(&candidate) {
			*plan = candidate
			return true, nil
		}
	}

	return selectBudgetedAllShortestPaths(plan), nil
}

// normalizeBudgetedAllShortestKinds keeps the caller's cached builder AST
// immutable. Copy only when a pure positive kind predicate can be relocated;
// resolved kind IDs and parameter values never enter normalization state.
func normalizeBudgetedAllShortestKinds(query *cypher.RegularQuery) (*cypher.RegularQuery, bool) {
	if query == nil {
		return query, false
	}

	found := false
	_ = walk.Cypher(query, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if match, ok := node.(*cypher.Match); ok && normalizeBudgetedMatchKinds(match, false) {
			found = true
		}
	}))
	if !found {
		return query, false
	}

	owned := cypher.Copy(query)
	_ = walk.Cypher(owned, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if match, ok := node.(*cypher.Match); ok {
			normalizeBudgetedMatchKinds(match, true)
		}
	}))

	return owned, true
}

// normalizeBudgetedMatchKinds relocates only conjunctive terms that constrain
// this relationship's kind domain. Empty domains retain the exact existing
// predicate/legacy route: an empty pattern kind list would instead mean wildcard.
// The apply flag permits mutation only after the caller has copied the AST.
func normalizeBudgetedMatchKinds(match *cypher.Match, apply bool) bool {
	if match == nil || match.Where == nil || len(match.Pattern) != 1 {
		return false
	}

	part := match.Pattern[0]
	if part == nil || !part.AllShortestPathsPattern || len(part.PatternElements) != 3 {
		return false
	}

	relationship, ok := part.PatternElements[1].AsRelationshipPattern()
	if !ok || relationship.Range == nil || relationship.Variable == nil || relationship.Variable.Symbol == "" {
		return false
	}

	var (
		domain   graph.Kinds
		residual []cypher.Expression
		found    bool
	)

	for _, expression := range match.Where.Expressions {
		extraction, extracted := removeConjunctiveRelationshipKinds(expression, relationship.Variable.Symbol)
		if extracted {
			if found {
				domain = intersectRelationshipKinds(domain, extraction.Kinds)
			} else {
				domain = extraction.Kinds
			}

			found = true
		}

		if extraction.Residual != nil {
			residual = append(residual, extraction.Residual)
		}
	}

	if !found {
		return false
	}

	if len(relationship.Kinds) > 0 {
		domain = intersectRelationshipKinds(relationship.Kinds, domain)
	}

	if len(domain) == 0 {
		return false
	}

	if apply {
		relationship.Kinds = domain
		match.Where.Expressions = residual
		if len(residual) == 0 {
			match.Where = nil
		}
	}

	return true
}

// unwrapBudgetedKindParenthetical exposes the underlying kind formula without
// changing the caller's expression or the surrounding cached AST.
func unwrapBudgetedKindParenthetical(expression cypher.Expression) cypher.Expression {
	unwrapped := expression
	for {
		parenthetical, ok := unwrapped.(*cypher.Parenthetical)
		if !ok || parenthetical == nil {
			return unwrapped
		}

		unwrapped = parenthetical.Expression
	}
}

// positiveRelationshipKindDomain accepts only a formula composed entirely of
// positive kind matchers for one variable. Mixed ORs, NOT, XOR, properties, and
// references to other bindings are retained for normal semantic lowering.
func positiveRelationshipKindDomain(expression cypher.Expression, symbol string) (graph.Kinds, bool) {
	switch typed := unwrapBudgetedKindParenthetical(expression).(type) {
	case *cypher.KindMatcher:
		if typed == nil || typed.IsExclusive {
			return nil, false
		}

		reference, ok := typed.Reference.(*cypher.Variable)
		if !ok || reference == nil || reference.Symbol != symbol {
			return nil, false
		}

		var kinds graph.Kinds
		for _, kind := range typed.Kinds {
			if kind == nil {
				return nil, false
			}

			kinds = appendRelationshipKind(kinds, kind)
		}

		return kinds, true

	case *cypher.Conjunction:
		if typed == nil || len(typed.Expressions) == 0 {
			return nil, false
		}

		var domain graph.Kinds
		for index, part := range typed.Expressions {
			kinds, ok := positiveRelationshipKindDomain(part, symbol)
			if !ok {
				return nil, false
			}

			if index == 0 {
				domain = kinds
			} else {
				domain = intersectRelationshipKinds(domain, kinds)
			}
		}

		return domain, true

	case *cypher.Disjunction:
		if typed == nil || len(typed.Expressions) == 0 {
			return nil, false
		}

		var domain graph.Kinds
		for _, part := range typed.Expressions {
			kinds, ok := positiveRelationshipKindDomain(part, symbol)
			if !ok {
				return nil, false
			}

			for _, kind := range kinds {
				domain = appendRelationshipKind(domain, kind)
			}
		}

		return domain, true

	default:
		return nil, false
	}
}

// intersectRelationshipKinds retains the shared names in the first domain's
// order, preserving graph.Kind implementations supplied by the caller.
func intersectRelationshipKinds(left, right graph.Kinds) graph.Kinds {
	var intersection graph.Kinds
	for _, kind := range left {
		if kind == nil {
			return nil
		}

		for _, candidate := range right {
			if candidate != nil && candidate.String() == kind.String() {
				intersection = appendRelationshipKind(intersection, kind)
				break
			}
		}
	}

	return intersection
}

// appendRelationshipKind adds a kind only when its semantic name is absent.
func appendRelationshipKind(kinds graph.Kinds, kind graph.Kind) graph.Kinds {
	for _, candidate := range kinds {
		if candidate.String() == kind.String() {
			return kinds
		}
	}

	return append(kinds, kind)
}

// budgetedRelationshipKindExtraction separates a relocated kind domain from
// the predicate that must remain available for fresh eligibility analysis.
type budgetedRelationshipKindExtraction struct {
	// Residual contains every predicate that was not safely relocated.
	Residual cypher.Expression

	// Kinds contains the positive relationship-kind domain being relocated.
	Kinds graph.Kinds
}

// removeConjunctiveRelationshipKinds never extracts a kind predicate from an
// unsupported disjunction or negation. Every remaining reference is analyzed
// again before V2 can be selected.
func removeConjunctiveRelationshipKinds(expression cypher.Expression, symbol string) (budgetedRelationshipKindExtraction, bool) {
	if domain, ok := positiveRelationshipKindDomain(expression, symbol); ok {
		return budgetedRelationshipKindExtraction{
			Kinds: domain,
		}, true
	}

	conjunction, ok := unwrapBudgetedKindParenthetical(expression).(*cypher.Conjunction)
	if !ok || conjunction == nil {
		return budgetedRelationshipKindExtraction{
			Residual: expression,
		}, false
	}

	var (
		remaining []cypher.Expression
		domain    graph.Kinds
		found     bool
	)

	for _, part := range conjunction.Expressions {
		extraction, extracted := removeConjunctiveRelationshipKinds(part, symbol)
		if extracted {
			if found {
				domain = intersectRelationshipKinds(domain, extraction.Kinds)
			} else {
				domain = extraction.Kinds
			}

			found = true
		}

		if extraction.Residual != nil {
			remaining = append(remaining, extraction.Residual)
		}
	}

	if !found {
		return budgetedRelationshipKindExtraction{
			Residual: expression,
		}, false
	}

	extraction := budgetedRelationshipKindExtraction{
		Kinds: domain,
	}
	switch len(remaining) {
	case 0:
		return extraction, true

	case 1:
		extraction.Residual = remaining[0]
		return extraction, true

	default:
		extraction.Residual = cypher.NewConjunction(remaining...)
		return extraction, true
	}
}
