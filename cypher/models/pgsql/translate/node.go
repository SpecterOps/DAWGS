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
	} else if err := s.translateNodePatternToStep(nodePattern, patternPart, bindingResult); err != nil {
		return err
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
			pgsql.OperatorPGArrayOverlap,
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
