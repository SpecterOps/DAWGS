package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
)

type endpointSeededIdentifiers struct {
	endpoints pgsql.Identifier
	reverse   pgsql.Identifier
	states    pgsql.Identifier
	incumbent pgsql.Identifier
}

func newEndpointSeededIdentifiers(finalFrame pgsql.Identifier) endpointSeededIdentifiers {
	prefix := string(finalFrame) + "_endpoint_seeded_"
	return endpointSeededIdentifiers{
		endpoints: pgsql.Identifier(prefix + "endpoints"),
		reverse:   pgsql.Identifier(prefix + "reverse"),
		states:    pgsql.Identifier(prefix + "states"),
		incumbent: pgsql.Identifier(prefix + "incumbent"),
	}
}

func selectedEndpointSeededDecision(part *PatternPart, decisions map[optimize.TraversalStepTarget]optimize.ExpansionSearchStrategyDecision) (optimize.ExpansionSearchStrategyDecision, bool) {
	for _, step := range part.TraversalSteps {
		if step == nil || !step.HasSourceTarget {
			continue
		}
		if decision, found := decisions[step.SourceTarget]; found && decision.SelectedStrategy == optimize.ExpansionSearchEndpointSeededReverse {
			return decision, true
		}
	}
	return optimize.ExpansionSearchStrategyDecision{}, false
}

func (s *Translator) rewriteTraversalPatternAsEndpointSeededReverse(part *PatternPart, decision optimize.ExpansionSearchStrategyDecision, firstCTE int) error {
	if decision.PrefixLength != 1 || decision.Target.StepIndex != 1 || len(part.TraversalSteps) != 2 {
		return fmt.Errorf("endpoint-seeded reverse target requires exactly one fixed prefix step and one terminal expansion")
	}
	prefixStep := part.TraversalSteps[0]
	expansionStep := part.TraversalSteps[1]
	if prefixStep == nil || prefixStep.Edge == nil || prefixStep.Frame == nil || expansionStep == nil || expansionStep.Expansion == nil || expansionStep.Frame == nil || expansionStep.RightNode == nil || expansionStep.LeftNode == nil || expansionStep.Edge == nil {
		return fmt.Errorf("endpoint-seeded reverse target has an incomplete traversal step")
	}

	ctes := s.query.CurrentPart().Model.CommonTableExpressions.Expressions
	if firstCTE < 0 || firstCTE >= len(ctes) {
		return fmt.Errorf("endpoint-seeded reverse target did not emit an incumbent frame chain")
	}
	incumbentFinal := ctes[len(ctes)-1]
	if incumbentFinal.Alias.Name != expansionStep.Frame.Binding.Identifier {
		return fmt.Errorf("endpoint-seeded reverse final frame mismatch: expected %s but found %s", expansionStep.Frame.Binding.Identifier, incumbentFinal.Alias.Name)
	}
	incumbentSelect, ok := incumbentFinal.Query.Body.(pgsql.Select)
	if !ok {
		return fmt.Errorf("endpoint-seeded reverse final frame must be a select")
	}
	prefixEdgeIDs := pgsql.ArrayLiteral{
		Values:   []pgsql.Expression{pgsql.CompoundIdentifier{prefixStep.Frame.Binding.Identifier, prefixStep.Edge.Identifier}},
		CastType: pgsql.Int8Array,
	}
	incumbentSelect.Where = pgsql.OptionalAnd(incumbentSelect.Where, pgd.Not(pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{expansionStep.Expansion.Frame.Binding.Identifier, expansionPath},
		pgsql.OperatorArrayOverlap,
		prefixEdgeIDs,
	)))
	incumbentQuery := incumbentFinal.Query
	incumbentQuery.Body = incumbentSelect

	ids := newEndpointSeededIdentifiers(incumbentFinal.Alias.Name)
	query, err := s.buildGuardedEndpointSeededQuery(decision, prefixStep, expansionStep, ids, incumbentQuery, incumbentSelect.Projection)
	if err != nil {
		return err
	}
	s.query.CurrentPart().Model.CommonTableExpressions.Expressions = append(ctes[:len(ctes)-1], pgsql.CommonTableExpression{
		Alias: incumbentFinal.Alias,
		Query: query,
	})
	s.recordExpansionSearchStrategy(decision.Target, optimize.ExpansionSearchEndpointSeededReverse)
	return nil
}

func (s *Translator) buildGuardedEndpointSeededQuery(
	decision optimize.ExpansionSearchStrategyDecision,
	prefixStep *TraversalStep,
	expansionStep *TraversalStep,
	ids endpointSeededIdentifiers,
	incumbent pgsql.Query,
	incumbentProjection pgsql.Projection,
) (pgsql.Query, error) {
	endpointCTE, err := buildEndpointSeedCTE(decision, expansionStep, ids)
	if err != nil {
		return pgsql.Query{}, err
	}
	reverseCTE, err := buildEndpointReverseCTE(decision, expansionStep, ids)
	if err != nil {
		return pgsql.Query{}, err
	}
	statesCTE := buildEndpointStateProbeCTE(decision, ids)
	incumbentCTE := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.incumbent},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        incumbent,
	}

	candidateProjection, fallbackProjection, err := endpointSeededProjections(prefixStep, expansionStep, ids, incumbentProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	prefixFrame := prefixStep.Frame.Binding.Identifier
	endpointOverflow := endpointSeededOverflow(ids.endpoints, decision.EndpointLimit)
	stateOverflow := endpointSeededOverflow(ids.states, decision.StateLimit)
	admitted := pgsql.OptionalAnd(
		pgd.Not(endpointOverflow),
		pgd.Not(stateOverflow),
	)

	prefixEdgeIDs := pgsql.ArrayLiteral{
		Values:   []pgsql.Expression{pgsql.CompoundIdentifier{prefixFrame, prefixStep.Edge.Identifier}},
		CastType: pgsql.Int8Array,
	}
	candidateWhere := pgsql.OptionalAnd(
		admitted,
		pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{ids.states, expansionDepth}, pgsql.OperatorGreaterThanOrEqualTo, pgsql.NewLiteral(decision.MinimumDepth, pgsql.Int8)),
	)
	candidateWhere = pgsql.OptionalAnd(candidateWhere, pgd.Not(pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{ids.states, expansionPath}, pgsql.OperatorArrayOverlap, prefixEdgeIDs,
	)))

	candidate := pgsql.Select{
		Projection: candidateProjection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: prefixFrame.AsCompoundIdentifier()},
			Joins: []pgsql.Join{
				{
					Table: pgsql.TableReference{Name: ids.states.AsCompoundIdentifier()},
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
						projectedNodeIDReference(prefixFrame, expansionStep.LeftNode), pgsql.OperatorEquals, pgsql.CompoundIdentifier{ids.states, expansionNextID},
					)},
				},
				{
					Table: pgsql.TableReference{Name: ids.endpoints.AsCompoundIdentifier()},
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{ids.endpoints, pgsql.ColumnID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{ids.states, expansionRootID},
					)},
				},
			},
		}},
		Where: candidateWhere,
	}
	fallback := pgsql.Select{
		Projection: fallbackProjection,
		From:       []pgsql.FromClause{tableFrom(ids.incumbent)},
		Where:      pgsql.NewBinaryExpression(endpointOverflow, pgsql.OperatorOr, stateOverflow),
	}

	return pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive:   true,
			Expressions: []pgsql.CommonTableExpression{endpointCTE, reverseCTE, statesCTE, incumbentCTE},
		},
		Body: pgsql.SetOperation{Operator: pgsql.OperatorUnion, All: true, LOperand: candidate, ROperand: fallback},
	}, nil
}

func buildEndpointSeedCTE(decision optimize.ExpansionSearchStrategyDecision, expansionStep *TraversalStep, ids endpointSeededIdentifiers) (pgsql.CommonTableExpression, error) {
	local, external := partitionConstraintByLocality(expansionStep.Expansion.TerminalNodeConstraints, pgsql.AsIdentifierSet(expansionStep.RightNode.Identifier))
	if external != nil {
		return pgsql.CommonTableExpression{}, fmt.Errorf("endpoint-seeded reverse terminal predicate is not local")
	}
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.endpoints},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					&pgsql.AliasedExpression{Expression: pgd.EntityID(expansionStep.RightNode.Identifier), Alias: models.OptionalValue(pgsql.ColumnID)},
					&pgsql.AliasedExpression{Expression: suffixSeededNodeValue(expansionStep.RightNode), Alias: models.OptionalValue(expansionStep.RightNode.Identifier)},
				},
				From:  []pgsql.FromClause{{Source: expansionNodeTableReference(expansionStep.RightNode.Identifier)}},
				Where: local,
			},
			Limit: pgsql.NewLiteral(decision.EndpointLimit+1, pgsql.Int8),
		},
	}, nil
}

func buildEndpointReverseCTE(decision optimize.ExpansionSearchStrategyDecision, expansionStep *TraversalStep, ids endpointSeededIdentifiers) (pgsql.CommonTableExpression, error) {
	localEdgeConstraint, external := partitionConstraintByLocality(expansionStep.Expansion.EdgeConstraints, pgsql.AsIdentifierSet(expansionStep.Edge.Identifier))
	if external != nil {
		return pgsql.CommonTableExpression{}, fmt.Errorf("endpoint-seeded reverse relationship predicate is not local")
	}
	emptyPath := pgsql.ArrayLiteral{CastType: pgsql.Int8Array}
	seed := pgsql.Select{
		Projection: []pgsql.SelectItem{
			pgsql.CompoundIdentifier{ids.endpoints, pgsql.ColumnID},
			pgsql.CompoundIdentifier{ids.endpoints, pgsql.ColumnID},
			pgsql.NewLiteral(int64(0), pgsql.Int8),
			emptyPath,
		},
		From: []pgsql.FromClause{tableFrom(ids.endpoints)},
	}
	path := pgsql.CompoundIdentifier{ids.reverse, expansionPath}
	recursiveWhere := pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{ids.reverse, expansionDepth}, pgsql.OperatorLessThan, pgsql.NewLiteral(decision.MaximumDepth, pgsql.Int8)),
		pgsql.NewBinaryExpression(pgd.EntityID(expansionStep.Edge.Identifier), pgsql.OperatorNotEquals, pgsql.NewAllExpression(path)),
	)
	recursiveWhere = pgsql.OptionalAnd(recursiveWhere, localEdgeConstraint)
	recursive := pgsql.Select{
		Projection: []pgsql.SelectItem{
			pgsql.CompoundIdentifier{ids.reverse, expansionRootID},
			pgsql.CompoundIdentifier{expansionStep.Edge.Identifier, expansionStep.Expansion.EdgeStartIdentifier},
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{ids.reverse, expansionDepth}, pgsql.OperatorAdd, pgsql.NewLiteral(int64(1), pgsql.Int8)),
			pgsql.FunctionCall{Function: pgsql.Identifier("array_prepend"), Parameters: []pgsql.Expression{pgd.EntityID(expansionStep.Edge.Identifier), path}, CastType: pgsql.Int8Array},
		},
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: ids.reverse.AsCompoundIdentifier()},
			Joins: []pgsql.Join{{
				Table: expansionEdgeTableReference(expansionStep.Edge.Identifier),
				JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{expansionStep.Edge.Identifier, expansionStep.Expansion.EdgeEndIdentifier}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{ids.reverse, expansionNextID},
				)},
			}},
		}},
		Where: recursiveWhere,
	}
	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: ids.reverse, Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionRootID, expansionNextID, expansionDepth, expansionPath})},
		Query: pgsql.Query{Body: pgsql.SetOperation{Operator: pgsql.OperatorUnion, All: true, LOperand: seed, ROperand: recursive}},
	}, nil
}

func buildEndpointStateProbeCTE(decision optimize.ExpansionSearchStrategyDecision, ids endpointSeededIdentifiers) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.states},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					pgsql.CompoundIdentifier{ids.reverse, expansionRootID},
					pgsql.CompoundIdentifier{ids.reverse, expansionNextID},
					pgsql.CompoundIdentifier{ids.reverse, expansionDepth},
					pgsql.CompoundIdentifier{ids.reverse, expansionPath},
				},
				From: []pgsql.FromClause{tableFrom(ids.reverse)},
			},
			Limit: pgsql.NewLiteral(decision.StateLimit+1, pgsql.Int8),
		},
	}
}

func endpointSeededOverflow(source pgsql.Identifier, limit int64) pgsql.ExistsExpression {
	return pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{
		Body:   pgsql.Select{Projection: []pgsql.SelectItem{pgsql.NewLiteral(int64(1), pgsql.Int8)}, From: []pgsql.FromClause{tableFrom(source)}},
		Offset: pgsql.NewLiteral(limit, pgsql.Int8),
		Limit:  pgsql.NewLiteral(int64(1), pgsql.Int8),
	}}}
}

func endpointSeededProjections(prefixStep, expansionStep *TraversalStep, ids endpointSeededIdentifiers, incumbent pgsql.Projection) (pgsql.Projection, pgsql.Projection, error) {
	prefixFrame := prefixStep.Frame.Binding.Identifier
	candidate := make(pgsql.Projection, 0, len(incumbent))
	fallback := make(pgsql.Projection, 0, len(incumbent))
	for _, item := range incumbent {
		alias, ok := selectItemAlias(item)
		if !ok {
			return nil, nil, fmt.Errorf("endpoint-seeded reverse final projection contains an unaliased item %T", item)
		}
		var expression pgsql.Expression
		switch {
		case expansionStep.Expansion.PathBinding != nil && alias == expansionStep.Expansion.PathBinding.Identifier:
			expression = pgsql.CompoundIdentifier{ids.states, expansionPath}
		case alias == expansionStep.LeftNode.Identifier:
			expression = pgsql.CompoundIdentifier{prefixFrame, alias}
		case alias == expansionStep.RightNode.Identifier:
			expression = pgsql.CompoundIdentifier{ids.endpoints, alias}
		default:
			expression = pgsql.CompoundIdentifier{prefixFrame, alias}
		}
		candidate = append(candidate, &pgsql.AliasedExpression{Expression: expression, Alias: models.OptionalValue(alias)})
		fallback = append(fallback, &pgsql.AliasedExpression{Expression: pgsql.CompoundIdentifier{ids.incumbent, alias}, Alias: models.OptionalValue(alias)})
	}
	return candidate, fallback, nil
}
