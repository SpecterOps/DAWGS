package translate

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

type BindingResult struct {
	Binding      *BoundIdentifier
	AlreadyBound bool
}

func (s *Translator) bindPatternExpression(cypherExpression cypher.Expression, dataType pgsql.DataType) (BindingResult, error) {
	if cypherBinding, hasCypherBinding, err := extractIdentifierFromCypherExpression(cypherExpression); err != nil {
		return BindingResult{}, err
	} else if existingBinding, bound := s.scope.AliasedLookup(cypherBinding); bound {
		return BindingResult{
			Binding:      existingBinding,
			AlreadyBound: true,
		}, nil
	} else if binding, err := s.scope.DefineNew(dataType); err != nil {
		return BindingResult{}, err
	} else {
		if hasCypherBinding {
			s.scope.Alias(cypherBinding, binding)
		}

		return BindingResult{
			Binding:      binding,
			AlreadyBound: false,
		}, nil
	}
}

func (s *Translator) translatePatternPart(patternPart *cypher.PatternPart) error {
	// We expect this to be a node select if there aren't enough pattern elements for a traversal
	newPatternPart := s.query.CurrentPart().currentPattern.NewPart()
	newPatternPart.IsTraversal = len(patternPart.PatternElements) > 1
	newPatternPart.ShortestPath = patternPart.ShortestPathPattern
	newPatternPart.AllShortestPaths = patternPart.AllShortestPathsPattern

	if cypherBinding, hasCypherSymbol, err := extractIdentifierFromCypherExpression(patternPart); err != nil {
		return err
	} else if hasCypherSymbol {
		if pathBinding, err := s.scope.DefineNew(pgsql.PathComposite); err != nil {
			return err
		} else {
			// Generate an alias for this binding
			s.scope.Alias(cypherBinding, pathBinding)

			// Record the new binding in the traversal pattern being built
			newPatternPart.PatternBinding = pathBinding
		}
	}

	return nil
}

func (s *Translator) buildPatternPart(part *PatternPart) error {
	if part.IsTraversal {
		return s.buildTraversalPatternPart(part)
	} else {
		return s.buildNodePatternPart(part)
	}
}

func (s *Translator) buildTraversalPattern(traversalStep *TraversalStep, isRootStep bool) error {
	if isRootStep {
		if traversalStepQuery, err := s.buildTraversalPatternRoot(traversalStep.Frame, traversalStep); err != nil {
			return err
		} else {
			s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.Frame.Binding.Identifier,
				},
				Query: traversalStepQuery,
			})
		}
	} else {
		if traversalStepQuery, err := s.buildTraversalPatternStep(traversalStep.Frame, traversalStep); err != nil {
			return err
		} else {
			s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.Frame.Binding.Identifier,
				},
				Query: traversalStepQuery,
			})
		}
	}

	return nil
}

func (s *Translator) buildExpansionPattern(traversalStep *TraversalStep, expansion *ExpansionBuilder, isRootStep bool) error {
	if isRootStep {
		if traversalStepQuery, err := s.buildExpansionPatternRoot(traversalStep, expansion); err != nil {
			return err
		} else {
			s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.Frame.Binding.Identifier,
				},
				Query: traversalStepQuery,
			})
		}
	} else {
		if traversalStepQuery, err := s.buildExpansionPatternStep(traversalStep, expansion); err != nil {
			return err
		} else {
			s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.Frame.Binding.Identifier,
				},
				Query: traversalStepQuery,
			})
		}
	}

	return nil
}

func (s *Translator) buildShortestPathsExpansionPattern(traversalStep *TraversalStep, expansion *ExpansionBuilder, isRootStep, allPaths bool) error {
	if isRootStep {
		if allPaths {
			if traversalStep.Expansion.CanExecuteBidirectionalSearch() {
				if traversalStepQuery, err := expansion.BuildBiDirectionalAllShortestPathsRoot(); err != nil {
					return err
				} else {
					s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
						Alias: pgsql.TableAlias{
							Name: traversalStep.Frame.Binding.Identifier,
						},
						Query: traversalStepQuery,
					})
				}
			} else if traversalStepQuery, err := expansion.BuildAllShortestPathsRoot(); err != nil {
				return err
			} else {
				s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
					Alias: pgsql.TableAlias{
						Name: traversalStep.Frame.Binding.Identifier,
					},
					Query: traversalStepQuery,
				})
			}
		} else if traversalStepQuery, err := expansion.BuildShortestPathsRoot(); err != nil {
			return err
		} else {
			s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.Frame.Binding.Identifier,
				},
				Query: traversalStepQuery,
			})
		}
	} else {
		if traversalStepQuery, err := s.buildExpansionPatternStep(traversalStep, expansion); err != nil {
			return err
		} else {
			s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.Frame.Binding.Identifier,
				},
				Query: traversalStepQuery,
			})
		}
	}

	return nil
}

func (s *Translator) buildTraversalPatternPart(part *PatternPart) error {
	for idx, traversalStep := range part.TraversalSteps {
		isRootStep := idx == 0

		if traversalStep.Expansion != nil {
			if expansion, err := NewExpansionBuilder(s.translation.Parameters, traversalStep); err != nil {
				return err
			} else if part.ShortestPath || part.AllShortestPaths {
				if err := s.buildShortestPathsExpansionPattern(traversalStep, expansion, isRootStep, part.AllShortestPaths); err != nil {
					return err
				}
			} else if err := s.buildExpansionPattern(traversalStep, expansion, isRootStep); err != nil {
				return err
			}
		} else if err := s.buildTraversalPattern(traversalStep, isRootStep); err != nil {
			return err
		}
	}

	return nil
}
