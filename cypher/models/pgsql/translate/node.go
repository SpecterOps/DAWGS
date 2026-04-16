package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) translateNodePattern(nodePattern *cypher.NodePattern) error {
	var (
		queryPart   = s.query.CurrentPart()
		patternPart = queryPart.currentPattern.CurrentPart()
	)

	if bindingResult, err := s.bindPatternExpression(nodePattern, pgsql.NodeComposite); err != nil {
		return err
	} else if queryPart.isCreating {
		return s.collectCreateNodePattern(nodePattern, patternPart, bindingResult)
	} else if err := s.translateNodePatternToStep(nodePattern, patternPart, bindingResult); err != nil {
		return err
	}

	return nil
}

func (s *Translator) collectCreateNodePattern(nodePattern *cypher.NodePattern, part *PatternPart, bindingResult BindingResult) error {
	queryPart := s.query.CurrentPart()

	if !bindingResult.AlreadyBound {
		// Only insert nodes that are being newly created, not those already bound from a match statement.
		queryPart.mutations.Creations.Put(bindingResult.Binding.Identifier, &NodeCreate{
			Binding:    bindingResult.Binding,
			Properties: queryPart.ConsumeProperties(),
			Kinds:      nodePattern.Kinds,
		})
	} else {
		// Consume any accumulated properties even if the node is already bound.
		queryPart.ConsumeProperties()
	}

	if part.IsTraversal {
		// Track nodes in traversal steps so that edge creation can resolve start/end IDs.
		numSteps := len(part.TraversalSteps)

		if numSteps == 0 {
			// This is the left (start) node of the pattern.
			part.TraversalSteps = append(part.TraversalSteps, &TraversalStep{
				LeftNode:      bindingResult.Binding,
				LeftNodeBound: bindingResult.AlreadyBound,
			})
		} else {
			currentStep := part.TraversalSteps[numSteps-1]

			if currentStep.RightNode == nil {
				// This is the right (end) node of the current step.
				currentStep.RightNode = bindingResult.Binding
				currentStep.RightNodeBound = bindingResult.AlreadyBound

				// Propagate the right node to any pending EdgeCreate for this step.
				if currentStep.Edge != nil {
					if pendingEdge := queryPart.mutations.EdgeCreations.Get(currentStep.Edge.Identifier); pendingEdge != nil {
						pendingEdge.RightNode = bindingResult.Binding
					}
				}
			}
		}
	} else {
		part.NodeSelect.Binding = bindingResult.Binding
	}

	// Register this node as a dependency of any enclosing path binding.
	if part.PatternBinding != nil {
		part.PatternBinding.DependOn(bindingResult.Binding)
	}

	return nil
}

func (s *Translator) translateNodePatternToStep(nodePattern *cypher.NodePattern, part *PatternPart, bindingResult BindingResult) error {
	currentQueryPart := s.query.CurrentPart()

	// Check the IR for any collected properties
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

		if err := s.treeTranslator.AddTranslationConstraint(pgsql.AsIdentifierSet(bindingResult.Binding.Identifier), propertyConstraints); err != nil {
			return err
		}
	}

	// Check for kind constraints
	if len(nodePattern.Kinds) > 0 {
		if kindIDs, err := s.kindMapper.MapKinds(nodePattern.Kinds); err != nil {
			return fmt.Errorf("failed to translate kinds: %w", err)
		} else if kindIDsLiteral, err := pgsql.AsLiteral(kindIDs); err != nil {
			return err
		} else if err := s.treeTranslator.AddTranslationConstraint(pgsql.NewIdentifierSet().Add(bindingResult.Binding.Identifier), pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{bindingResult.Binding.Identifier, pgsql.ColumnKindIDs},
			pgsql.OperatorPGArrayLHSContainsRHS,
			kindIDsLiteral,
		)); err != nil {
			return err
		}
	}

	if part.IsTraversal {
		if numSteps := len(part.TraversalSteps); numSteps == 0 {
			// This is the traversal step's left node
			part.TraversalSteps = append(part.TraversalSteps, &TraversalStep{
				LeftNode:      bindingResult.Binding,
				LeftNodeBound: bindingResult.AlreadyBound,
			})
		} else {
			currentStep := part.TraversalSteps[numSteps-1]

			// Set the right node pattern identifier
			currentStep.RightNode = bindingResult.Binding
			currentStep.RightNodeBound = bindingResult.AlreadyBound

			// Finish setting up this traversal step for the expansion
			if currentStep.Expansion != nil {
				// Set the right node data type to the terminal of an expansion
				currentStep.RightNode.DataType = pgsql.ExpansionTerminalNode

				// TODO: This is a little recursive and could use some refactor love
				currentExpansion := currentStep.Expansion

				if err := currentExpansion.CompletePattern(currentStep); err != nil {
					return err
				}
			}
		}
	} else {
		// Make this the node select of the pattern part
		part.NodeSelect.Binding = bindingResult.Binding
	}

	if part.PatternBinding != nil {
		// If there's a bound pattern track this node as a dependency of the pattern identifier
		part.PatternBinding.DependOn(bindingResult.Binding)
	}

	return nil
}

func (s *Translator) buildNodePatternPart(part *PatternPart) error {
	var (
		partFrame  = part.NodeSelect.Frame
		nextSelect = pgsql.Select{
			Projection: part.NodeSelect.Select.Projection,
			Where:      part.NodeSelect.Constraints,
		}
	)

	// The current query part may not have a frame associated with it if is a single part query component
	if previousFrame, hasPrevious := s.previousValidFrame(partFrame); hasPrevious {
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
			},
		})
	}

	// Consume any pending UNWIND clauses so that the unnest(...) sources are
	// available in this CTE's FROM, allowing downstream WHERE to reference the
	// unwind binding.
	nextSelect.From = append(nextSelect.From, unwindFromClauses(s.query.CurrentPart().ConsumeUnwindClauses())...)

	nextSelect.From = append(nextSelect.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
			Binding: models.OptionalValue(part.NodeSelect.Binding.Identifier),
		},
	})

	// Prepare the next select statement
	s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: partFrame.Binding.Identifier,
		},
		Query: pgsql.Query{
			Body: nextSelect,
		},
	})

	return nil
}
