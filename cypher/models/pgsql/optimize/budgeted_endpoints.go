package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

// EndpointPairPredicate identifies a same-MATCH predicate proven safe to apply
// before budgeted endpoint-pair admission and shortest-path discovery.
type EndpointPairPredicate string

const (
	// EndpointPairPredicateDistinctIDs excludes identical endpoint IDs through a
	// direct conjunctive inequality, independent of operand and traversal order.
	EndpointPairPredicateDistinctIDs EndpointPairPredicate = "distinct_endpoint_ids"
)

// unwrapBudgetedEndpointExpression exposes a parenthesized expression without
// changing the query supplied by the caller.
func unwrapBudgetedEndpointExpression(expression cypher.Expression) cypher.Expression {
	unwrapped := expression
	for {
		if parenthetical, ok := unwrapped.(*cypher.Parenthetical); ok && parenthetical != nil {
			unwrapped = parenthetical.Expression
		} else {
			return unwrapped
		}
	}
}

// budgetedEndpointIdentitySymbol recognizes id(variable), including parentheses
// around the invocation, without broadening the identity translator's arguments.
func budgetedEndpointIdentitySymbol(expression cypher.Expression) (string, bool) {
	function, ok := unwrapBudgetedEndpointExpression(expression).(*cypher.FunctionInvocation)
	if !ok || function == nil || len(function.Arguments) != 1 || len(function.Namespace) > 0 || function.Distinct {
		return "", false
	}

	return identityFunctionSymbol(function)
}

// isBudgetedDistinctEndpointPredicate recognizes only a direct inequality
// between the two endpoint IDs. Negation, OR, and comparison chains are excluded.
func isBudgetedDistinctEndpointPredicate(expression cypher.Expression, rootSymbol, terminalSymbol string) bool {
	comparison, ok := unwrapBudgetedEndpointExpression(expression).(*cypher.Comparison)
	if !ok || comparison == nil || len(comparison.Partials) != 1 || comparison.Partials[0] == nil ||
		comparison.Partials[0].Operator != cypher.OperatorNotEquals {
		return false
	}

	var (
		leftSymbol, leftIsID   = budgetedEndpointIdentitySymbol(comparison.Left)
		rightSymbol, rightIsID = budgetedEndpointIdentitySymbol(comparison.Partials[0].Right)
	)

	return leftIsID && rightIsID &&
		((leftSymbol == rootSymbol && rightSymbol == terminalSymbol) ||
			(leftSymbol == terminalSymbol && rightSymbol == rootSymbol))
}

// removeBudgetedDistinctEndpointTerms removes only independent conjunctive ID
// inequalities for proof analysis. Unsupported formulas retain their full AST.
func removeBudgetedDistinctEndpointTerms(expression cypher.Expression, rootSymbol, terminalSymbol string) (cypher.Expression, bool) {
	if isBudgetedDistinctEndpointPredicate(expression, rootSymbol, terminalSymbol) {
		return nil, true
	}

	conjunction, ok := unwrapBudgetedEndpointExpression(expression).(*cypher.Conjunction)
	if !ok || conjunction == nil {
		return expression, false
	}

	var (
		remaining []cypher.Expression
		removed   bool
	)

	for _, term := range conjunction.Expressions {
		residual, changed := removeBudgetedDistinctEndpointTerms(term, rootSymbol, terminalSymbol)
		removed = removed || changed
		if residual != nil {
			remaining = append(remaining, residual)
		}
	}

	if !removed {
		return expression, false
	}

	switch len(remaining) {
	case 0:
		return nil, true

	case 1:
		return remaining[0], true

	default:
		return cypher.NewConjunction(remaining...), true
	}
}

// extractBudgetedEndpointPairPredicate classifies a single all-shortest MATCH.
// Applying extraction is permitted only on the caller's owned proof-analysis AST.
func extractBudgetedEndpointPairPredicate(match *cypher.Match, apply bool) EndpointPairPredicate {
	if match == nil || match.Where == nil || len(match.Pattern) != 1 {
		return ""
	}

	part := match.Pattern[0]
	if part == nil || !part.AllShortestPathsPattern || len(part.PatternElements) != 3 {
		return ""
	}

	var (
		root, rootIsNode         = part.PatternElements[0].AsNodePattern()
		terminal, terminalIsNode = part.PatternElements[2].AsNodePattern()
	)

	if !rootIsNode || !terminalIsNode || root.Variable == nil || terminal.Variable == nil ||
		root.Variable.Symbol == "" || terminal.Variable.Symbol == "" || root.Variable.Symbol == terminal.Variable.Symbol {
		return ""
	}

	var (
		remaining []cypher.Expression
		removed   bool
	)

	for _, expression := range match.Where.Expressions {
		residual, changed := removeBudgetedDistinctEndpointTerms(expression, root.Variable.Symbol, terminal.Variable.Symbol)
		removed = removed || changed
		if residual != nil {
			remaining = append(remaining, residual)
		}
	}

	if !removed {
		return ""
	}

	if apply {
		if len(remaining) == 0 {
			match.Where = nil
		} else {
			match.Where.Expressions = remaining
		}
	}

	return EndpointPairPredicateDistinctIDs
}

// BudgetedEndpointPairPredicates proves pair predicates for automatic V2 only.
// An owned copy with just the recognized same-MATCH inequalities removed must
// satisfy the historical uncorrelated-pair gate. Other correlation, row sources,
// and later-WITH predicates cannot establish this proof. The original query and
// every historical planner decision remain unchanged.
func BudgetedEndpointPairPredicates(query *cypher.RegularQuery) map[TraversalStepTarget]EndpointPairPredicate {
	if query == nil {
		return nil
	}

	found := false
	_ = walk.Cypher(query, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, handler walk.VisitorHandler) {
		if match, ok := node.(*cypher.Match); ok && extractBudgetedEndpointPairPredicate(match, false) != "" {
			found = true
			handler.SetDone()
		}
	}))
	if !found {
		return nil
	}

	var (
		owned      = cypher.Copy(query)
		targets    = IndexPatternTargets(owned)
		predicates = map[TraversalStepTarget]EndpointPairPredicate{}
	)

	_ = walk.Cypher(owned, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if match, ok := node.(*cypher.Match); ok {
			if predicate := extractBudgetedEndpointPairPredicate(match, true); predicate != "" {
				if target, indexed := targets[match.Pattern[0]]; indexed {
					predicates[target.TraversalStep(0)] = predicate
				}
			}
		}
	}))

	if proofPlan, err := Optimize(owned); err != nil {
		return nil
	} else {
		proven := map[TraversalStepTarget]EndpointPairPredicate{}
		for _, endpoint := range proofPlan.LoweringPlan.EndpointResolution {
			if predicate, extracted := predicates[endpoint.Target]; extracted {
				for _, fact := range endpoint.EligibilityFacts {
					if fact.Name == EndpointResolutionEligibilityUncorrelatedPair && fact.Eligible {
						proven[endpoint.Target] = predicate
					}
				}
			}
		}

		return proven
	}
}
