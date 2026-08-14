package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

// BindingResult groups SQL model state that must remain consistent while translating binding result.
type BindingResult struct {
	// Binding supplies the binding input to the BindingResult contract.
	Binding *BoundIdentifier
	// AlreadyBound indicates whether already bound applies.
	AlreadyBound bool
}

// bindPatternExpression binds a completed traversal result to its pattern variable when one was declared.
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

// translatePatternPart dispatches shortest-path, variable-expansion, and fixed traversal patterns to their builders.
func (s *Translator) translatePatternPart(patternPart *cypher.PatternPart) error {
	// We expect this to be a node select if there aren't enough pattern elements for a traversal
	newPatternPart := s.query.CurrentPart().currentPattern.NewPart()
	newPatternPart.IsTraversal = len(patternPart.PatternElements) > 1
	newPatternPart.ShortestPath = patternPart.ShortestPathPattern
	newPatternPart.AllShortestPaths = patternPart.AllShortestPathsPattern
	newPatternPart.PathDirectionReversed = patternPart.PathDirectionReversed
	if target, hasTarget := s.patternTargets[patternPart]; hasTarget {
		newPatternPart.Target = target
		newPatternPart.HasTarget = true
	}

	if cypherBinding, hasCypherSymbol, err := extractIdentifierFromCypherExpression(patternPart); err != nil {
		return err
	} else if hasCypherSymbol {
		if pathBinding, err := s.scope.DefineNew(pgsql.PathComposite); err != nil {
			return err
		} else {
			// Generate an alias for this binding
			s.scope.Alias(cypherBinding, pathBinding)

			// Propagate the optimizer's pattern reversal so path materialization can restore the
			// original left-to-right logical order for this bound path.
			pathBinding.PathDirectionReversed = patternPart.PathDirectionReversed

			// Record the new binding in the traversal pattern being built
			newPatternPart.PatternBinding = pathBinding
		}
	}

	return nil
}

// buildPatternPart finalizes a translated pattern part and exports its visible bindings.
func (s *Translator) buildPatternPart(part *PatternPart) error {
	if part.IsTraversal {
		return s.buildTraversalPatternPart(part)
	} else {
		return s.buildNodePatternPart(part)
	}
}

// buildTraversalPattern emits fixed traversal steps and applies any exact-range unrolling decisions.
func (s *Translator) buildTraversalPattern(traversalStep *TraversalStep, isRootStep bool) error {
	if isRootStep {
		if traversalStepQuery, err := s.buildTraversalPatternRoot(traversalStep.Frame, traversalStep); err != nil {
			return err
		} else {
			if selectBody, ok := traversalStepQuery.Body.(pgsql.Select); ok {
				selectBody.From = append(selectBody.From, unwindFromClauses(s.query.CurrentPart().ConsumeUnwindClauses())...)
				traversalStepQuery.Body = selectBody
			}

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

// buildExpansionPattern emits an ordinary variable expansion and any qualified specialized-search rewrite.
func (s *Translator) buildExpansionPattern(traversalStepContext TraversalStepContext, expansion *ExpansionBuilder) error {
	traversalStep := traversalStepContext.CurrentStep

	if traversalStepContext.IsRootStep {
		if traversalStepQuery, err := s.buildExpansionPatternRoot(traversalStepContext, expansion); err != nil {
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
		if traversalStepQuery, err := s.buildExpansionPatternStep(traversalStepContext, expansion); err != nil {
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

// buildShortestPathsExpansionPattern emits the selected shortest-path executor and its projection frame.
func (s *Translator) buildShortestPathsExpansionPattern(traversalStepContext TraversalStepContext, expansion *ExpansionBuilder, allPaths bool) error {
	traversalStep := traversalStepContext.CurrentStep

	if traversalStepContext.IsRootStep {
		expansion.SetUnwindClauses(s.query.CurrentPart().ConsumeUnwindClauses())

		if allPaths {
			if compactShortestExecutor(traversalStep.Expansion.ShortestPathExecutor) {
				var (
					traversalStepQuery pgsql.Query
					err                error
				)
				switch traversalStep.Expansion.ShortestPathExecutor {
				case optimize.ShortestPathExecutorASPA1DAG:
					traversalStepQuery, err = expansion.BuildAllShortestPathsDAGRoot()
				case optimize.ShortestPathExecutorASPI1DAG:
					traversalStepQuery, err = expansion.BuildInlineAllShortestPathsDAGRoot()
				case optimize.ShortestPathExecutorASPB1AlternatingNodeDAG:
					traversalStepQuery, err = expansion.BuildB1AllShortestPathsDAGRoot()
				case optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG:
					traversalStepQuery, err = expansion.BuildB2AllShortestPathsDAGRoot()
				default:
					err = fmt.Errorf("compact executor %q does not implement all-shortest-path enumeration", traversalStep.Expansion.ShortestPathExecutor)
				}
				if err != nil {
					return err
				}
				s.recordShortestPathExecutor(traversalStep.Expansion.ShortestPathTarget, traversalStep.Expansion.ShortestPathExecutor)
				s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
					Alias: pgsql.TableAlias{
						Name: traversalStep.Frame.Binding.Identifier,
					},
					Query: traversalStepQuery,
				})
			} else if traversalStep.Expansion.UseBidirectionalSearch {
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
		} else {
			var (
				traversalStepQuery pgsql.Query
				err                error
			)

			if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS3Unidirectional || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalDistance {
				traversalStepQuery, err = expansion.BuildShortestDistanceRoot()
			} else if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorI2GuardedDistance {
				traversalStepQuery, err = expansion.BuildInlineGuardedShortestDistanceRoot()
			} else if isV2GuardedDistanceExecutor(traversalStep.Expansion.ShortestPathExecutor) {
				traversalStepQuery, err = expansion.buildInlineGuardedShortestDistanceRoot(
					traversalStep.Expansion.ShortestPathExecutor,
					spI2DevelopmentArchitecture(traversalStep.Expansion.ShortestPathExecutor),
				)
			} else if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS3EdgeM0 || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalWitness {
				traversalStepQuery, err = expansion.BuildShortestPathEdgeM0Root()
			} else if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
				traversalStepQuery, err = expansion.BuildInlineCanonicalShortestPathRoot()
			} else if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorB1AlternatingNodeDistance || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorB1AlternatingNodeWitness {
				traversalStepQuery, err = expansion.BuildB1CompactShortestPathRoot()
			} else if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness {
				traversalStepQuery, err = expansion.BuildB2CompactShortestPathRoot()
			} else if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS4CanonicalDistance || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS4CanonicalWitness {
				traversalStepQuery, err = expansion.BuildCompactShortestPathRoot()
			} else if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS0Direct {
				traversalStepQuery, err = expansion.BuildBiDirectionalShortestPathsRootWithDirectPreflight()
			} else if traversalStep.Expansion.UseBidirectionalSearch {
				traversalStepQuery, err = expansion.BuildBiDirectionalShortestPathsRoot()
			} else {
				traversalStepQuery, err = expansion.BuildShortestPathsRoot()
			}

			if err != nil {
				return err
			}
			if traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS3Unidirectional || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS3EdgeM0 || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalDistance || isGuardedDistanceExecutor(traversalStep.Expansion.ShortestPathExecutor) || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalWitness || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness || compactShortestExecutor(traversalStep.Expansion.ShortestPathExecutor) || traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorS0Direct ||
				(traversalStep.Expansion.ShortestPathExecutor == optimize.ShortestPathExecutorIncumbentWorkspace && decisionIsForcedShortest(s, traversalStep.Expansion.ShortestPathTarget)) {
				s.recordShortestPathExecutor(traversalStep.Expansion.ShortestPathTarget, traversalStep.Expansion.ShortestPathExecutor)
			}

			s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.Frame.Binding.Identifier,
				},
				Query: traversalStepQuery,
			})
		}
	} else {
		if traversalStepQuery, err := s.buildExpansionPatternStep(traversalStepContext, expansion); err != nil {
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

// TraversalStepContext groups SQL model state that must remain consistent while translating traversal step context.
type TraversalStepContext struct {
	// PreviousStep supplies the previous step input to the TraversalStepContext contract.
	PreviousStep *TraversalStep
	// CurrentStep supplies the current step input to the TraversalStepContext contract.
	CurrentStep *TraversalStep
	// IsRootStep indicates whether is root step applies.
	IsRootStep bool
}

// buildTraversalPatternPart translates all steps in a non-expanding pattern chain.
func (s *Translator) buildTraversalPatternPart(part *PatternPart) error {
	firstCTE := len(s.query.CurrentPart().Model.CommonTableExpressions.Expressions)
	fixedSuffixDecision, useFixedSuffixStrategy := selectedFixedSuffixDecision(part, s.expansionSearchStrategyDecisions)
	suffixReverseGuardDecision, useSuffixReverseGuard := selectedSuffixReverseGuardDecision(part, s.expansionSearchStrategyDecisions)
	guardedSuffixDecision, useGuardedSuffixStrategy := selectedGuardedFixedSuffixDecision(part, s.expansionSearchStrategyDecisions)
	endpointSeededDecision, useEndpointSeededStrategy := selectedEndpointSeededDecision(part, s.expansionSearchStrategyDecisions)

	for idx, traversalStep := range part.TraversalSteps {
		var (
			isRootStep           = idx == 0
			traversalStepContext = TraversalStepContext{
				CurrentStep: traversalStep,
				IsRootStep:  isRootStep,
			}
		)

		if idx > 0 {
			traversalStepContext.PreviousStep = part.TraversalSteps[idx-1]
		}

		if traversalStep.Expansion != nil {
			if expansion, err := NewExpansionBuilder(s.translation.Parameters, traversalStep, s.graphID); err != nil {
				return err
			} else if part.ShortestPath || part.AllShortestPaths {
				if err := s.buildShortestPathsExpansionPattern(traversalStepContext, expansion, part.AllShortestPaths); err != nil {
					return err
				}
			} else if err := s.buildExpansionPattern(traversalStepContext, expansion); err != nil {
				return err
			}
		} else if err := s.buildTraversalPattern(traversalStep, isRootStep); err != nil {
			return err
		}

		s.allowLimitPushdownForStep(part, idx, traversalStep)
	}

	if useFixedSuffixStrategy {
		return s.rewriteTraversalPatternAsSuffixSeededReverse(part, fixedSuffixDecision, firstCTE)
	}
	if useSuffixReverseGuard {
		return s.rewriteTraversalPatternAsSuffixReverseGuard(part, suffixReverseGuardDecision, firstCTE)
	}
	if useGuardedSuffixStrategy {
		return s.rewriteTraversalPatternAsGuardedSuffixOrientation(part, guardedSuffixDecision, firstCTE)
	}
	if useEndpointSeededStrategy {
		return s.rewriteTraversalPatternAsEndpointSeededReverse(part, endpointSeededDecision, firstCTE)
	}

	return nil
}
