package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) translateRelationshipPattern(relationshipPattern *cypher.RelationshipPattern) error {
	var (
		currentQueryPart = s.query.CurrentPart()
		patternPart      = currentQueryPart.currentPattern.CurrentPart()
	)

	if bindingResult, err := s.bindPatternExpression(relationshipPattern, pgsql.EdgeComposite); err != nil {
		return err
	} else if currentQueryPart.isCreating {
		return s.collectCreateEdgePattern(relationshipPattern, patternPart, bindingResult)
	} else {
		if err := s.translateRelationshipPatternToStep(bindingResult, patternPart, relationshipPattern); err != nil {
			return err
		}

		if currentQueryPart.HasProperties() {
			var propertyConstraints pgsql.Expression

			for key, value := range currentQueryPart.ConsumeProperties() {
				s.treeTranslator.PushOperand(pgsql.NewPropertyLookup(pgsql.CompoundIdentifier{bindingResult.Binding.Identifier, pgsql.ColumnProperties}, pgsql.NewLiteral(key, pgsql.Text)))
				s.treeTranslator.PushOperand(value)

				if newConstraint, err := s.treeTranslator.PopBinaryExpression(pgsql.OperatorEquals); err != nil {
					return err
				} else {
					propertyConstraints = pgsql.OptionalAnd(propertyConstraints, newConstraint)
				}
			}

			if err := s.treeTranslator.AddTranslationConstraint(pgsql.NewIdentifierSet().Add(bindingResult.Binding.Identifier), propertyConstraints); err != nil {
				return err
			}
		}

		// Capture the kind matchers for this relationship pattern
		if len(relationshipPattern.Kinds) > 0 {
			if kindIDs, err := s.kindMapper.MapKinds(relationshipPattern.Kinds); err != nil {
				return fmt.Errorf("failed to translate kinds: %w", err)
			} else if err := s.treeTranslator.AddTranslationConstraint(pgsql.NewIdentifierSet().Add(bindingResult.Binding.Identifier), pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{bindingResult.Binding.Identifier, pgsql.ColumnKindID},
				pgsql.OperatorEquals,
				pgsql.NewAnyExpressionHinted(pgsql.NewLiteral(kindIDs, pgsql.Int2Array)),
			)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Translator) collectCreateEdgePattern(relationshipPattern *cypher.RelationshipPattern, part *PatternPart, bindingResult BindingResult) error {
	var (
		queryPart = s.query.CurrentPart()
		numSteps  = len(part.TraversalSteps)
	)

	if numSteps == 0 {
		return fmt.Errorf("relationship pattern encountered before any left node in CREATE pattern")
	}

	currentStep := part.TraversalSteps[numSteps-1]
	if currentStep.Edge != nil {
		// Multiple relationships in one CREATE pattern share the prior right node
		// as the next left node, so start a continuation step before collecting
		// the new edge.
		part.TraversalSteps = append(part.TraversalSteps, &TraversalStep{
			LeftNode:      currentStep.RightNode,
			LeftNodeBound: currentStep.RightNodeBound,
		})

		currentStep = part.TraversalSteps[len(part.TraversalSteps)-1]
	}

	currentStep.Edge = bindingResult.Binding
	currentStep.Direction = relationshipPattern.Direction

	if part.PatternBinding != nil {
		// Pattern bindings are materialized later from their dependencies; record
		// the created edge even though the INSERT has not been built yet.
		part.PatternBinding.DependOn(bindingResult.Binding)
	}

	queryPart.mutations.EdgeCreations.Put(bindingResult.Binding.Identifier, &EdgeCreate{
		Binding:    bindingResult.Binding,
		Properties: queryPart.ConsumeProperties(),
		Kinds:      relationshipPattern.Kinds,
		LeftNode:   currentStep.LeftNode,
		Direction:  relationshipPattern.Direction,
	})

	return nil
}

func (s *Translator) translateRelationshipPatternToStep(bindingResult BindingResult, part *PatternPart, relationshipPattern *cypher.RelationshipPattern) error {
	var (
		expansion      *Expansion
		numSteps       = len(part.TraversalSteps)
		currentStep    = part.TraversalSteps[numSteps-1]
		isContinuation = currentStep.Edge != nil
	)

	if bindingResult.AlreadyBound {
		if isContinuation {
			// This is a traversal continuation so copy the right node identifier of the preceding step and then
			// add the new step
			nextStep := &TraversalStep{
				Edge:      bindingResult.Binding,
				Direction: relationshipPattern.Direction,
			}

			// Mark the left node as already bound as it's part of the previous step's continuation
			nextStep.LeftNode = currentStep.RightNode
			nextStep.LeftNodeBound = true

			part.TraversalSteps = append(part.TraversalSteps, nextStep)
		} else {
			// Carry over the left node identifier if the edge identifier for the preceding step isn't set
			currentStep.Edge = bindingResult.Binding
			currentStep.Direction = relationshipPattern.Direction
		}

		return nil
	}

	// Look for any relationship pattern ranges. These indicate some kind of variable expansion of the path pattern.
	if relationshipPattern.Range != nil {
		// Set the edge type to an expansion of edges
		bindingResult.Binding.DataType = pgsql.ExpansionEdge

		if !isContinuation {
			// If this isn't a continuation then the left node was defined in isolation from the preceding node
			// pattern. Retype the left node to an expansion root node and link it to the expansion
			currentStep.LeftNode.DataType = pgsql.ExpansionRootNode
		}

		if expansionPathBinding, err := s.scope.DefineNew(pgsql.ExpansionPath); err != nil {
			return err
		} else {
			// Set up the new expansion model here
			expansion = NewExpansionModel(part, relationshipPattern)

			// Set the path binding in the expansion struct for easier referencing upstream
			expansion.PathBinding = expansionPathBinding
			expansionPathBinding.DependOn(bindingResult.Binding)

			if part.PatternBinding != nil {
				// If there's a bound pattern track this expansion's path as a dependency of the
				// pattern identifier
				part.PatternBinding.DependOn(expansionPathBinding)
			}
		}
	} else if part.PatternBinding != nil {
		// If there's a bound pattern track this edge as a dependency of the pattern identifier
		part.PatternBinding.DependOn(bindingResult.Binding)
	}

	if isContinuation {
		// This is a traversal continuation so copy the right node identifier of the preceding step and then
		// add the new step
		part.TraversalSteps = append(part.TraversalSteps, &TraversalStep{
			Edge:          bindingResult.Binding,
			Direction:     relationshipPattern.Direction,
			LeftNode:      currentStep.RightNode,
			LeftNodeBound: true,
			Expansion:     expansion,
		})
	} else {
		// Carry over the left node identifier if the edge identifier for the preceding step isn't set
		currentStep.Edge = bindingResult.Binding
		currentStep.Direction = relationshipPattern.Direction
		currentStep.Expansion = expansion
	}

	return nil
}
