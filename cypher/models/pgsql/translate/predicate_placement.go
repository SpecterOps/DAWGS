package translate

import (
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

func (s *Translator) recordPredicatePlacementConsumption(part *PatternPart, stepIndex int, traversalStep *TraversalStep, constraints PatternConstraints) {
	if part == nil || !part.HasTarget || traversalStep == nil {
		return
	}

	for _, decision := range s.predicatePlacementDecisions[part.Target.TraversalStep(stepIndex)] {
		if predicatePlacementDecisionConsumed(decision, traversalStep, constraints) {
			s.recordLowering(optimize.LoweringPredicatePlacement)
			return
		}
	}
}

func predicatePlacementDecisionConsumed(decision optimize.PredicatePlacementDecision, traversalStep *TraversalStep, constraints PatternConstraints) bool {
	for _, symbol := range decision.Attachment.BindingSymbols {
		if bindingConstraintConsumed(symbol, traversalStep.LeftNode, constraints.LeftNode) ||
			bindingConstraintConsumed(symbol, traversalStep.Edge, constraints.Edge) ||
			bindingConstraintConsumed(symbol, traversalStep.RightNode, constraints.RightNode) {
			return true
		}
	}

	return false
}

func bindingConstraintConsumed(symbol string, binding *BoundIdentifier, constraint *Constraint) bool {
	return constraint != nil &&
		constraint.Expression != nil &&
		bindingMatchesSymbol(binding, pgsql.Identifier(symbol))
}

func bindingMatchesSymbol(binding *BoundIdentifier, symbol pgsql.Identifier) bool {
	if binding == nil {
		return false
	}

	return binding.Identifier == symbol ||
		(binding.Alias.Set && binding.Alias.Value == symbol)
}
