package optimize

import "github.com/specterops/dawgs/cypher/models/cypher"

type sourceTraversalStep struct {
	LeftNode     *cypher.NodePattern
	Relationship *cypher.RelationshipPattern
	RightNode    *cypher.NodePattern
}

func BuildLoweringPlan(query *cypher.RegularQuery, _ Analysis) (LoweringPlan, error) {
	if query == nil || query.SingleQuery == nil {
		return LoweringPlan{}, nil
	}

	var plan LoweringPlan

	if query.SingleQuery.MultiPartQuery != nil {
		for queryPartIndex, part := range query.SingleQuery.MultiPartQuery.Parts {
			if part == nil {
				continue
			}

			if err := appendQueryPartLowerings(&plan, queryPartIndex, part, part.ReadingClauses); err != nil {
				return LoweringPlan{}, err
			}
		}

		if finalPart := query.SingleQuery.MultiPartQuery.SinglePartQuery; finalPart != nil {
			if err := appendQueryPartLowerings(&plan, len(query.SingleQuery.MultiPartQuery.Parts), finalPart, finalPart.ReadingClauses); err != nil {
				return LoweringPlan{}, err
			}
		}
	} else if singlePart := query.SingleQuery.SinglePartQuery; singlePart != nil {
		if err := appendQueryPartLowerings(&plan, 0, singlePart, singlePart.ReadingClauses); err != nil {
			return LoweringPlan{}, err
		}
	}

	return plan, nil
}

func appendQueryPartLowerings(plan *LoweringPlan, queryPartIndex int, queryPart cypher.SyntaxNode, readingClauses []*cypher.ReadingClause) error {
	sourceReferences, err := collectReferencedSourceIdentifiers(queryPart)
	if err != nil {
		return err
	}

	appendProjectionPruningDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
	appendLatePathMaterializationDecisions(plan, queryPartIndex, readingClauses, sourceReferences)
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
			hasPruning = !edgeReferenced || !pathReferenced
		} else {
			leftReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(step.LeftNode.Variable))
			rightReferenced := referencesSourceIdentifier(sourceReferences, variableSymbol(step.RightNode.Variable))

			hasPruning = !(leftReferenced || pathReferenced) ||
				!(edgeReferenced || pathReferenced) ||
				!(rightReferenced || pathReferenced || stepIndex+1 < len(steps))
		}

		if hasPruning {
			plan.ProjectionPruning = append(plan.ProjectionPruning, decision)
		}
	}
}

func appendLatePathMaterializationDecisions(plan *LoweringPlan, queryPartIndex int, readingClauses []*cypher.ReadingClause, sourceReferences map[string]struct{}) {
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil || readingClause.Match.Optional {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			if !referencesSourceIdentifier(sourceReferences, variableSymbol(patternPart.Variable)) {
				continue
			}

			for stepIndex, step := range traversalStepsForPattern(patternPart) {
				target := PatternTarget{
					QueryPartIndex: queryPartIndex,
					ClauseIndex:    clauseIndex,
					PatternIndex:   patternIndex,
				}.TraversalStep(stepIndex)

				if step.Relationship.Range != nil {
					plan.LatePathMaterialization = append(plan.LatePathMaterialization, LatePathMaterializationDecision{
						Target: target,
						Mode:   LatePathMaterializationExpansionPath,
					})
					continue
				}

				mode := LatePathMaterializationPathEdgeID
				if referencesSourceIdentifier(sourceReferences, variableSymbol(step.Relationship.Variable)) {
					mode = LatePathMaterializationEdgeComposite
				}

				plan.LatePathMaterialization = append(plan.LatePathMaterialization, LatePathMaterializationDecision{
					Target: target,
					Mode:   mode,
				})
			}
		}
	}
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
