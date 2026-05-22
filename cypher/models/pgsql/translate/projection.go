package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

type BoundProjections struct {
	Items    pgsql.Projection
	Bindings []*BoundIdentifier
}

func rewriteConstraintIdentifierReferences(scope *Scope, frame *Frame, constraints []*Constraint) error {
	if frame.Previous == nil {
		return nil
	}

	for _, constraint := range constraints {
		if err := RewriteFrameBindings(scope, constraint.Expression); err != nil {
			return err
		}
	}

	return nil
}

func buildExternalProjection(scope *Scope, projections []*Projection) (pgsql.Projection, error) {
	var sqlProjection pgsql.Projection

	for _, projection := range projections {
		switch typedProjectionExpression := projection.SelectItem.(type) {
		case pgsql.Identifier:
			alias := projection.Alias.Value

			if projectedBinding, bound := scope.Lookup(typedProjectionExpression); !bound {
				return nil, fmt.Errorf("invalid identifier: %s", typedProjectionExpression)
			} else {
				if !projection.Alias.Set {
					alias = projectedBinding.Alias.Value
				}

				if builtProjection, err := buildProjection(alias, projectedBinding, scope, projectedBinding.LastProjection); err != nil {
					return nil, err
				} else {
					for _, buildProjectionItem := range builtProjection {
						sqlProjection = append(sqlProjection, buildProjectionItem)
					}
				}
			}

		default:
			builtProjection := projection.SelectItem

			if projection.Alias.Set {
				builtProjection = &pgsql.AliasedExpression{
					Expression: builtProjection,
					Alias:      projection.Alias,
				}
			}

			sqlProjection = append(sqlProjection, builtProjection)
		}
	}

	if resolvedProjection, err := resolvePathCompositeFieldReferencesInProjection(scope, sqlProjection); err != nil {
		return nil, err
	} else {
		sqlProjection = resolvedProjection
	}

	if err := RewriteFrameBindings(scope, sqlProjection); err != nil {
		return nil, err
	}

	// Lastly, return the projections while rewriting the given constraints
	return sqlProjection, nil
}

func buildInternalProjection(scope *Scope, projectedBindings []*BoundIdentifier) (BoundProjections, error) {
	var (
		boundProjections = BoundProjections{
			Bindings: projectedBindings,
		}
		projected = map[pgsql.Identifier]struct{}{}
	)

	for _, projectedBinding := range projectedBindings {
		if _, alreadyProjected := projected[projectedBinding.Identifier]; alreadyProjected {
			continue
		}

		projected[projectedBinding.Identifier] = struct{}{}

		// Build the identifier's projection
		if newSelectItems, err := buildProjection(projectedBinding.Identifier, projectedBinding, scope, projectedBinding.LastProjection); err != nil {
			return BoundProjections{}, err
		} else {
			boundProjections.Items = append(boundProjections.Items, newSelectItems...)
		}
	}

	if len(boundProjections.Items) == 0 {
		boundProjections.Items = append(boundProjections.Items, pgsql.NewLiteral(1, pgsql.Int))
	}

	// Lastly, return the projections while rewriting the given constraints
	return boundProjections, nil
}

func buildVisibleProjections(scope *Scope) (BoundProjections, error) {
	currentFrame := scope.CurrentFrame()

	if knownBindings, err := scope.LookupBindings(currentFrame.Known().Slice()...); err != nil {
		return BoundProjections{}, err
	} else {
		return buildInternalProjection(scope, knownBindings)
	}
}

func buildProjectionForExpansionPath(alias pgsql.Identifier, projected *BoundIdentifier, scope *Scope, referenceFrame *Frame) ([]pgsql.SelectItem, error) {
	if projected.LastProjection != nil {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{referenceFrame.Binding.Identifier, projected.Identifier},
				Alias:      pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}

	return []pgsql.SelectItem{
		&pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{scope.CurrentFrame().Binding.Identifier, pgsql.ColumnPath},
			Alias:      models.OptionalValue(alias),
		},
	}, nil
}

func concatenatePathCompositeParts(parts []pgsql.Expression) pgsql.Expression {
	if len(parts) == 0 {
		return nil
	}

	joined := parts[0]
	for idx := 1; idx < len(parts); idx++ {
		joined = pgsql.NewBinaryExpression(joined, pgsql.OperatorConcatenate, parts[idx])
	}

	return joined
}

func bindingFrameReference(scope *Scope, binding *BoundIdentifier) pgsql.CompoundIdentifier {
	frameIdentifier := scope.CurrentFrameBinding().Identifier
	if binding.LastProjection != nil {
		frameIdentifier = binding.LastProjection.Binding.Identifier
	}

	return pgsql.CompoundIdentifier{frameIdentifier, binding.Identifier}
}

func pathBindingReference(scope *Scope, binding *BoundIdentifier) pgsql.Expression {
	if binding.LastProjection != nil {
		return pgsql.CompoundIdentifier{binding.LastProjection.Binding.Identifier, binding.Identifier}
	}

	if frameBinding := scope.CurrentFrameBinding(); frameBinding != nil {
		return pgsql.CompoundIdentifier{frameBinding.Identifier, binding.Identifier}
	}

	return binding.Identifier
}

func pathCompositeReference(scope *Scope, binding *BoundIdentifier, columns []pgsql.Identifier) pgsql.Expression {
	if binding.LastProjection != nil || scope.CurrentFrameBinding() != nil {
		return pathBindingReference(scope, binding)
	}

	values := make([]pgsql.Expression, len(columns))
	for idx, column := range columns {
		values[idx] = pgsql.CompoundIdentifier{binding.Identifier, column}
	}

	return pgsql.CompositeValue{
		Values:   values,
		DataType: bindingExpressionType(binding),
	}
}

func pathCompositeColumnReference(scope *Scope, binding *BoundIdentifier, column pgsql.Identifier) pgsql.Expression {
	if binding.LastProjection != nil || scope.CurrentFrameBinding() != nil {
		return pgsql.RowColumnReference{
			Identifier: pathBindingReference(scope, binding),
			Column:     column,
		}
	}

	return pgsql.CompoundIdentifier{binding.Identifier, column}
}

func pathEdgeIDReference(scope *Scope, binding *BoundIdentifier) pgsql.Expression {
	if binding.LastProjection != nil || scope.CurrentFrameBinding() != nil {
		return pathBindingReference(scope, binding)
	}

	return pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnID}
}

func pathEdgeArrayExpression(scope *Scope, edge *BoundIdentifier) pgsql.Expression {
	return &pgsql.EdgeArrayFromPathIDs{
		PathIDs: pgsql.ArrayLiteral{
			Values: []pgsql.Expression{
				pathEdgeIDReference(scope, edge),
			},
			CastType: pgsql.Int8Array,
		},
	}
}

func expansionPathEdgeArrayExpression(scope *Scope, expansionPath *BoundIdentifier) (pgsql.Expression, error) {
	return &pgsql.EdgeArrayFromPathIDs{
		PathIDs: pathBindingReference(scope, expansionPath),
	}, nil
}

func optionalOr(leftOperand, rightOperand pgsql.Expression) pgsql.Expression {
	if leftOperand == nil {
		return rightOperand
	} else if rightOperand == nil {
		return leftOperand
	}

	return pgsql.NewBinaryExpression(leftOperand, pgsql.OperatorOr, rightOperand)
}

func expressionIsNull(expression pgsql.Expression) pgsql.Expression {
	return pgsql.NewBinaryExpression(expression, pgsql.OperatorIs, pgsql.NullLiteral())
}

func pathCompositeDependencyNullGuard(scope *Scope, dependency *BoundIdentifier) pgsql.Expression {
	if dependency == nil {
		return nil
	}

	switch dependency.DataType {
	case pgsql.ExpansionPath:
		return expressionIsNull(pathBindingReference(scope, dependency))

	case pgsql.EdgeComposite:
		return expressionIsNull(pathCompositeColumnReference(scope, dependency, pgsql.ColumnID))

	case pgsql.PathEdge:
		return expressionIsNull(pathEdgeIDReference(scope, dependency))

	case pgsql.NodeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
		return expressionIsNull(pathCompositeColumnReference(scope, dependency, pgsql.ColumnID))

	default:
		return nil
	}
}

func nullGuardPathCompositeExpression(expression, nullGuard pgsql.Expression) pgsql.Expression {
	if nullGuard == nil {
		return expression
	}

	return pgsql.Case{
		Conditions: []pgsql.Expression{nullGuard},
		Then:       []pgsql.Expression{pgsql.NullLiteral()},
		Else:       expression,
	}
}

func expressionForPathComposite(projected *BoundIdentifier, scope *Scope) (pgsql.Expression, error) {
	if projected.LastProjection != nil {
		return pgsql.CompoundIdentifier{projected.LastProjection.Binding.Identifier, projected.Identifier}, nil
	}

	var (
		edgeArrayReferences  []pgsql.Expression
		nodeReferences       []pgsql.Expression
		directNodeReferences []pgsql.Expression
		directEdgeReferences []pgsql.Expression
		seenExpansionPath    = false
		seenPathEdge         = false
		nullGuard            pgsql.Expression
	)

	// Path composite components are encoded as dependencies on the bound identifier representing the
	// path. This is not ideal as it escapes normal translation flow as driven by the structure of the
	// originating cypher AST.
	for _, dependency := range projected.Dependencies {
		nullGuard = optionalOr(nullGuard, pathCompositeDependencyNullGuard(scope, dependency))

		switch dependency.DataType {
		case pgsql.ExpansionPath:
			seenExpansionPath = true
			if edgeArrayReference, err := expansionPathEdgeArrayExpression(scope, dependency); err != nil {
				return nil, err
			} else {
				edgeArrayReferences = append(edgeArrayReferences, edgeArrayReference)
			}

		case pgsql.EdgeComposite:
			directEdgeReference := pathCompositeReference(scope, dependency, pgsql.EdgeTableColumns)

			directEdgeReferences = append(directEdgeReferences, directEdgeReference)
			edgeArrayReferences = append(edgeArrayReferences, pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{directEdgeReference},
				CastType: pgsql.EdgeCompositeArray,
			})

		case pgsql.PathEdge:
			seenPathEdge = true
			edgeArrayReferences = append(edgeArrayReferences, pathEdgeArrayExpression(scope, dependency))

		case pgsql.NodeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
			directNodeReferences = append(directNodeReferences, pathCompositeReference(scope, dependency, pgsql.NodeTableColumns))
			nodeReferences = append(nodeReferences, pathCompositeColumnReference(scope, dependency, pgsql.ColumnID))

		default:
			return nil, fmt.Errorf("unsupported type for path rendering: %s", dependency.DataType)
		}
	}

	// Direct, non-expansion path bindings already have their node and edge composites in scope. Keep
	// those explicit components instead of reconstructing the path from edge IDs: this preserves path
	// order and duplicate nodes, and it also works for rows produced by data-modifying CTEs where
	// re-reading node/edge tables in the same statement may not see the RETURNING values.
	if !seenExpansionPath && !seenPathEdge && len(directNodeReferences) > 0 {
		return nullGuardPathCompositeExpression(pgsql.CompositeValue{
			DataType: pgsql.PathComposite,
			Values: []pgsql.Expression{
				pgsql.ArrayLiteral{
					Values:   directNodeReferences,
					CastType: pgsql.NodeCompositeArray,
				},
				pgsql.ArrayLiteral{
					Values:   directEdgeReferences,
					CastType: pgsql.EdgeCompositeArray,
				},
			},
		}, nullGuard), nil
	}

	if seenExpansionPath || seenPathEdge {
		if len(directNodeReferences) == 0 {
			return nil, fmt.Errorf("expansion path %s does not contain a root node reference", projected.Identifier)
		}

		edgeArrayExpression := concatenatePathCompositeParts(edgeArrayReferences)
		if edgeArrayExpression == nil {
			edgeArrayExpression = pgsql.ArrayLiteral{CastType: pgsql.EdgeCompositeArray}
		}

		return nullGuardPathCompositeExpression(pgsql.FunctionCall{
			Function: pgsql.FunctionOrderedEdgesToPath,
			Parameters: []pgsql.Expression{
				directNodeReferences[0],
				edgeArrayExpression,
				pgsql.ArrayLiteral{
					Values:   directNodeReferences,
					CastType: pgsql.NodeCompositeArray,
				},
			},
			CastType: pgsql.PathComposite,
		}, nullGuard), nil
	} else if len(nodeReferences) > 0 {
		return nullGuardPathCompositeExpression(pgsql.FunctionCall{
			Function: pgsql.FunctionNodesToPath,
			Parameters: []pgsql.Expression{
				pgsql.Variadic{
					Expression: pgsql.ArrayLiteral{
						Values:   nodeReferences,
						CastType: pgsql.Int8Array,
					},
				},
			},
			CastType: pgsql.PathComposite,
		}, nullGuard), nil
	}

	return nil, fmt.Errorf("path variable does not contain valid components")
}

func buildProjectionForPathComposite(alias pgsql.Identifier, projected *BoundIdentifier, scope *Scope) ([]pgsql.SelectItem, error) {
	if expression, err := expressionForPathComposite(projected, scope); err != nil {
		return nil, err
	} else {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: expression,
				Alias:      pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}
}

func buildProjectionForExpansionNode(alias pgsql.Identifier, projected *BoundIdentifier, referenceFrame *Frame) ([]pgsql.SelectItem, error) {
	if projected.LastProjection != nil {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{referenceFrame.Binding.Identifier, projected.Identifier},
				Alias:      pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}

	value := pgsql.CompositeValue{
		DataType: pgsql.NodeComposite,
	}

	for _, nodeTableColumn := range pgsql.NodeTableColumns {
		value.Values = append(value.Values, pgsql.CompoundIdentifier{projected.Identifier, nodeTableColumn})
	}

	// Change the type to the node composite now that this is projected
	projected.DataType = pgsql.NodeComposite

	// Create a new final projection that's aliased to the visible binding's identifier
	return []pgsql.SelectItem{
		&pgsql.AliasedExpression{
			Expression: value,
			Alias:      pgsql.AsOptionalIdentifier(alias),
		},
	}, nil
}

func buildProjectionForNodeComposite(alias pgsql.Identifier, projected *BoundIdentifier, referenceFrame *Frame) ([]pgsql.SelectItem, error) {
	if projected.LastProjection != nil {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{referenceFrame.Binding.Identifier, projected.Identifier},
				Alias:      pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}

	value := pgsql.CompositeValue{
		DataType: pgsql.NodeComposite,
	}

	for _, nodeTableColumn := range pgsql.NodeTableColumns {
		value.Values = append(value.Values, pgsql.CompoundIdentifier{projected.Identifier, nodeTableColumn})
	}

	// Create a new final projection that's aliased to the visible binding's identifier
	return []pgsql.SelectItem{
		&pgsql.AliasedExpression{
			Expression: value,
			Alias:      pgsql.AsOptionalIdentifier(alias),
		},
	}, nil
}

func buildProjectionForExpansionEdge(alias pgsql.Identifier, projected *BoundIdentifier, scope *Scope) ([]pgsql.SelectItem, error) {
	// Change the type to the edge composite now that this is projected
	projected.DataType = pgsql.EdgeComposite

	// Create a new final projection that's aliased to the visible binding's identifier
	return []pgsql.SelectItem{
		&pgsql.AliasedExpression{
			Expression: &pgsql.EdgeArrayFromPathIDs{
				PathIDs: pgsql.CompoundIdentifier{
					scope.CurrentFrame().Binding.Identifier,
					pgsql.ColumnPath,
				},
			},
			Alias: pgsql.AsOptionalIdentifier(alias),
		},
	}, nil
}

func buildProjectionForEdgeComposite(alias pgsql.Identifier, projected *BoundIdentifier, referenceFrame *Frame) ([]pgsql.SelectItem, error) {
	if projected.LastProjection != nil {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{referenceFrame.Binding.Identifier, projected.Identifier},
				Alias:      pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}

	value := pgsql.CompositeValue{
		DataType: pgsql.EdgeComposite,
	}

	for _, edgeTableColumn := range pgsql.EdgeTableColumns {
		value.Values = append(value.Values, pgsql.CompoundIdentifier{projected.Identifier, edgeTableColumn})
	}

	// Create a new final projection that's aliased to the visible binding's identifier
	return []pgsql.SelectItem{
		&pgsql.AliasedExpression{
			Expression: value,
			Alias:      pgsql.AsOptionalIdentifier(alias),
		},
	}, nil
}

func buildProjectionForPathEdge(alias pgsql.Identifier, projected *BoundIdentifier, referenceFrame *Frame) ([]pgsql.SelectItem, error) {
	var expression pgsql.Expression

	if projected.LastProjection != nil {
		if referenceFrame == nil {
			referenceFrame = projected.LastProjection
		}

		expression = pgsql.CompoundIdentifier{referenceFrame.Binding.Identifier, projected.Identifier}
	} else {
		expression = pgsql.CompoundIdentifier{projected.Identifier, pgsql.ColumnID}
	}

	return []pgsql.SelectItem{
		&pgsql.AliasedExpression{
			Expression: expression,
			Alias:      pgsql.AsOptionalIdentifier(alias),
		},
	}, nil
}

func buildProjection(alias pgsql.Identifier, projected *BoundIdentifier, scope *Scope, referenceFrame *Frame) ([]pgsql.SelectItem, error) {
	switch projected.DataType {
	case pgsql.ExpansionPath:
		return buildProjectionForExpansionPath(alias, projected, scope, referenceFrame)

	case pgsql.PathComposite:
		return buildProjectionForPathComposite(alias, projected, scope)

	case pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
		return buildProjectionForExpansionNode(alias, projected, referenceFrame)

	case pgsql.NodeComposite:
		return buildProjectionForNodeComposite(alias, projected, referenceFrame)

	case pgsql.ExpansionEdge:
		return buildProjectionForExpansionEdge(alias, projected, scope)

	case pgsql.EdgeComposite:
		return buildProjectionForEdgeComposite(alias, projected, referenceFrame)

	case pgsql.PathEdge:
		return buildProjectionForPathEdge(alias, projected, referenceFrame)

	default:
		// If this isn't a type that requires a unique projection, reflect the identifier as-is with its alias
		var expression pgsql.Expression
		if referenceFrame != nil {
			expression = pgsql.CompoundIdentifier{referenceFrame.Binding.Identifier, projected.Identifier}
		} else {
			expression = projected.Identifier
		}

		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: expression,
				Alias:      pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}
}

func (s *Translator) buildInlineProjection(part *QueryPart) (pgsql.Select, error) {
	sqlSelect := pgsql.Select{
		Distinct: part.projections.Distinct,
		Where:    part.projections.Constraints,
	}

	// If there's a projection frame set, some additional negotiation is required to identify which frame the
	// from-statement should be written to. Some of this would be better figured out during the translation
	// of the projection where query scope and other components are not yet fully translated.
	if part.projections.Frame != nil && !part.projections.Frame.Synthetic {
		// Look up to see if there are CTE expressions registered. If there are then it is likely
		// there was a projection between this CTE and the previous multipart query part
		hasCTEs := part.Model.CommonTableExpressions != nil && len(part.Model.CommonTableExpressions.Expressions) > 0

		if part.Frame.Previous == nil || hasCTEs {
			sqlSelect.From = []pgsql.FromClause{{
				Source: part.projections.Frame.Binding.Identifier,
			}}
		} else {
			sqlSelect.From = []pgsql.FromClause{{
				Source: part.Frame.Previous.Binding.Identifier,
			}}
		}
	}

	sqlSelect.From = append(sqlSelect.From, unwindFromClauses(part.ConsumeUnwindClauses())...)

	for _, projection := range part.projections.Items {
		builtProjection := projection.SelectItem

		if projection.Alias.Set {
			builtProjection = &pgsql.AliasedExpression{
				Expression: builtProjection,
				Alias:      projection.Alias,
			}
		}

		sqlSelect.Projection = append(sqlSelect.Projection, builtProjection)
	}

	if len(part.projections.GroupBy) > 0 {
		for _, groupBy := range part.projections.GroupBy {
			sqlSelect.GroupBy = append(sqlSelect.GroupBy, groupBy)
		}
	}

	if resolvedWhere, err := resolvePathCompositeFieldReferences(s.scope, sqlSelect.Where); err != nil {
		return pgsql.Select{}, err
	} else {
		sqlSelect.Where = resolvedWhere
	}

	if resolvedProjection, err := resolvePathCompositeFieldReferencesInProjection(s.scope, sqlSelect.Projection); err != nil {
		return pgsql.Select{}, err
	} else {
		sqlSelect.Projection = resolvedProjection
	}

	return sqlSelect, nil
}

func (s *Translator) collectProjectionFromFrames(projections []*Projection) []pgsql.FromClause {
	fromClauseBuilder := NewFromClauseBuilder()

	for _, projection := range projections {
		identExpr, ok := projection.SelectItem.(pgsql.Identifier)
		if !ok {
			continue
		}

		binding, bound := s.scope.Lookup(identExpr)
		if !bound {
			continue
		}

		if binding.LastProjection != nil {
			fromClauseBuilder.AddBinding(binding)
		} else if binding.DataType == pgsql.PathComposite {
			for _, dep := range binding.Dependencies {
				fromClauseBuilder.AddBinding(dep)
			}
		}
	}

	if len(fromClauseBuilder.Clauses()) == 0 {
		if currentFrame := s.scope.CurrentFrame(); currentFrame != nil && !currentFrame.Synthetic {
			fromClauseBuilder.AddIdentifier(currentFrame.Binding.Identifier)
		}
	}

	return fromClauseBuilder.Clauses()
}

func countLimitPushdownShortestPathHarnessCalls(query pgsql.Query) int {
	var count int

	if query.CommonTableExpressions != nil {
		for _, cte := range query.CommonTableExpressions.Expressions {
			count += countLimitPushdownShortestPathHarnessCalls(cte.Query)
		}
	}

	if selectBody, isSelect := query.Body.(pgsql.Select); isSelect {
		for _, fromClause := range selectBody.From {
			if functionCall, isFunctionCall := fromClause.Source.(pgsql.FunctionCall); isFunctionCall &&
				isLimitPushdownShortestPathHarness(functionCall.Function) {
				count += 1
			}
		}
	}

	return count
}

func isLimitPushdownShortestPathHarness(function pgsql.Identifier) bool {
	return function == pgsql.FunctionUnidirectionalSPHarness || function == pgsql.FunctionBidirectionalSPHarness
}

func appendLimitToShortestPathHarness(query *pgsql.Query, limit pgsql.Expression) {
	if query.CommonTableExpressions != nil {
		for idx := range query.CommonTableExpressions.Expressions {
			appendLimitToShortestPathHarness(&query.CommonTableExpressions.Expressions[idx].Query, limit)
		}
	}

	if selectBody, isSelect := query.Body.(pgsql.Select); isSelect {
		for idx := range selectBody.From {
			if functionCall, isFunctionCall := selectBody.From[idx].Source.(pgsql.FunctionCall); isFunctionCall &&
				isLimitPushdownShortestPathHarness(functionCall.Function) {
				// The shortest-path harness accepts an optional path_limit. Passing
				// the query LIMIT here lets the BFS stop before producing rows the
				// outer query will discard.
				functionCall.Parameters = append(functionCall.Parameters, pgsql.NewTypeCast(limit, pgsql.Int8))
				selectBody.From[idx].Source = functionCall
			}
		}

		query.Body = selectBody
	}
}

func selectContainsAggregate(selectBody pgsql.Select) bool {
	containsAggregate := false

	if err := walk.PgSQL(selectBody, walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, errorHandler walk.VisitorHandler) {
		if functionCall, isFunctionCall := node.(pgsql.FunctionCall); isFunctionCall && pgsql.IsAggregateFunction(functionCall.Function) {
			containsAggregate = true
		}
	})); err != nil {
		return true
	}

	return containsAggregate
}

func compoundIdentifierEqual(left, right pgsql.CompoundIdentifier) bool {
	if len(left) != len(right) {
		return false
	}

	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}

	return true
}

func directShortestPathHarnessFrame(query pgsql.Query) (pgsql.Identifier, bool) {
	if query.CommonTableExpressions == nil {
		return "", false
	}

	var harnessFrame pgsql.Identifier

	for _, cte := range query.CommonTableExpressions.Expressions {
		selectBody, isSelect := cte.Query.Body.(pgsql.Select)
		if !isSelect {
			continue
		}

		for _, fromClause := range selectBody.From {
			if functionCall, isFunctionCall := fromClause.Source.(pgsql.FunctionCall); isFunctionCall &&
				isLimitPushdownShortestPathHarness(functionCall.Function) {
				if harnessFrame != "" {
					return "", false
				}

				harnessFrame = cte.Alias.Name
			}
		}
	}

	return harnessFrame, harnessFrame != ""
}

func isCompoundIdentifierOperand(expression pgsql.Expression, identifier pgsql.CompoundIdentifier) bool {
	compoundIdentifier, isCompoundIdentifier := unwrapParenthetical(expression).(pgsql.CompoundIdentifier)
	return isCompoundIdentifier && compoundIdentifierEqual(compoundIdentifier, identifier)
}

func isEqualityBetweenCompoundIdentifiers(expression pgsql.Expression, left, right pgsql.CompoundIdentifier) bool {
	binaryExpression, isBinaryExpression := unwrapParenthetical(expression).(*pgsql.BinaryExpression)
	if !isBinaryExpression || binaryExpression.Operator != pgsql.OperatorEquals {
		return false
	}

	return (isCompoundIdentifierOperand(binaryExpression.LOperand, left) && isCompoundIdentifierOperand(binaryExpression.ROperand, right)) ||
		(isCompoundIdentifierOperand(binaryExpression.LOperand, right) && isCompoundIdentifierOperand(binaryExpression.ROperand, left))
}

func expansionEndpointJoin(join pgsql.Join, harnessFrame pgsql.Identifier) (pgsql.Identifier, pgsql.Identifier, bool) {
	tableReference, isTableReference := join.Table.(pgsql.TableReference)
	if !isTableReference ||
		!tableReference.Binding.Set ||
		!compoundIdentifierEqual(tableReference.Name, pgsql.TableNode.AsCompoundIdentifier()) {
		return "", "", false
	}

	var (
		nodeAlias = tableReference.Binding.Value
		nodeID    = pgsql.CompoundIdentifier{nodeAlias, pgsql.ColumnID}
		rootID    = pgsql.CompoundIdentifier{harnessFrame, expansionRootID}
		nextID    = pgsql.CompoundIdentifier{harnessFrame, expansionNextID}
	)

	if isEqualityBetweenCompoundIdentifiers(join.JoinOperator.Constraint, nodeID, rootID) {
		return nodeAlias, expansionRootID, true
	}

	if isEqualityBetweenCompoundIdentifiers(join.JoinOperator.Constraint, nodeID, nextID) {
		return nodeAlias, expansionNextID, true
	}

	return "", "", false
}

func shortestPathEndpointAliases(query pgsql.Query) (pgsql.Identifier, pgsql.Identifier, bool) {
	harnessFrame, hasHarnessFrame := directShortestPathHarnessFrame(query)
	if !hasHarnessFrame {
		return "", "", false
	}

	selectBody, isSelect := query.Body.(pgsql.Select)
	if !isSelect {
		return "", "", false
	}

	var rootAlias, terminalAlias pgsql.Identifier

	for _, fromClause := range selectBody.From {
		for _, join := range fromClause.Joins {
			if nodeAlias, expansionColumn, isEndpointJoin := expansionEndpointJoin(join, harnessFrame); isEndpointJoin {
				switch expansionColumn {
				case expansionRootID:
					rootAlias = nodeAlias
				case expansionNextID:
					terminalAlias = nodeAlias
				}
			}
		}
	}

	return rootAlias, terminalAlias, rootAlias != "" && terminalAlias != "" && rootAlias != terminalAlias
}

func harnessEndpointColumn(expression pgsql.Expression, harnessFrame pgsql.Identifier) (pgsql.Identifier, bool) {
	compoundIdentifier, isCompoundIdentifier := unwrapParenthetical(expression).(pgsql.CompoundIdentifier)
	if !isCompoundIdentifier ||
		len(compoundIdentifier) != 2 ||
		compoundIdentifier[0] != harnessFrame ||
		(compoundIdentifier[1] != expansionRootID && compoundIdentifier[1] != expansionNextID) {
		return "", false
	}

	return compoundIdentifier[1], true
}

func rowIDReferenceAlias(expression pgsql.Expression) (pgsql.Identifier, bool) {
	rowColumnReference, isRowColumnReference := unwrapParenthetical(expression).(pgsql.RowColumnReference)
	if !isRowColumnReference || rowColumnReference.Column != pgsql.ColumnID {
		return "", false
	}

	compoundIdentifier, isCompoundIdentifier := unwrapParenthetical(rowColumnReference.Identifier).(pgsql.CompoundIdentifier)
	if !isCompoundIdentifier || len(compoundIdentifier) != 2 {
		return "", false
	}

	return compoundIdentifier[1], true
}

func sourceAliasMatchesEndpointColumn(sourceAlias, endpointColumn, rootAlias, terminalAlias pgsql.Identifier) bool {
	switch endpointColumn {
	case expansionRootID:
		return sourceAlias == rootAlias
	case expansionNextID:
		return sourceAlias == terminalAlias
	default:
		return false
	}
}

func isBoundEndpointProjectionConstraint(expression pgsql.Expression, harnessFrame, rootAlias, terminalAlias pgsql.Identifier) bool {
	binaryExpression, isBinaryExpression := unwrapParenthetical(expression).(*pgsql.BinaryExpression)
	if !isBinaryExpression || binaryExpression.Operator != pgsql.OperatorEquals {
		return false
	}

	leftEndpointColumn, leftIsEndpoint := harnessEndpointColumn(binaryExpression.LOperand, harnessFrame)
	rightEndpointColumn, rightIsEndpoint := harnessEndpointColumn(binaryExpression.ROperand, harnessFrame)
	leftSourceAlias, leftIsRowIDReference := rowIDReferenceAlias(binaryExpression.LOperand)
	rightSourceAlias, rightIsRowIDReference := rowIDReferenceAlias(binaryExpression.ROperand)

	return (leftIsEndpoint && rightIsRowIDReference && sourceAliasMatchesEndpointColumn(rightSourceAlias, leftEndpointColumn, rootAlias, terminalAlias)) ||
		(rightIsEndpoint && leftIsRowIDReference && sourceAliasMatchesEndpointColumn(leftSourceAlias, rightEndpointColumn, rootAlias, terminalAlias))
}

func shortestPathSourceWhereTransparent(query pgsql.Query, rootAlias, terminalAlias pgsql.Identifier) bool {
	harnessFrame, hasHarnessFrame := directShortestPathHarnessFrame(query)
	if !hasHarnessFrame {
		return false
	}

	selectBody, isSelect := query.Body.(pgsql.Select)
	if !isSelect {
		return false
	}

	if selectBody.Where == nil {
		return true
	}

	// The source CTE may add equality predicates that tie endpoint node aliases
	// back to the harness frame. Those are shape-preserving and do not affect
	// whether LIMIT may be pushed into the harness.
	for _, term := range flattenConjunction(unwrapParenthetical(selectBody.Where)) {
		if !isBoundEndpointProjectionConstraint(term, harnessFrame, rootAlias, terminalAlias) {
			return false
		}
	}

	return true
}

func endpointIDReference(expression pgsql.Expression, sourceFrame pgsql.Identifier) (pgsql.Identifier, bool) {
	rowColumnReference, isRowColumnReference := unwrapParenthetical(expression).(pgsql.RowColumnReference)
	compoundIdentifier, isCompoundIdentifier := unwrapParenthetical(rowColumnReference.Identifier).(pgsql.CompoundIdentifier)
	if !isRowColumnReference ||
		!isCompoundIdentifier ||
		rowColumnReference.Column != pgsql.ColumnID ||
		len(compoundIdentifier) != 2 ||
		compoundIdentifier[0] != sourceFrame {
		return "", false
	}

	return compoundIdentifier[1], true
}

func isEndpointAliasPair(leftAlias, rightAlias, rootAlias, terminalAlias pgsql.Identifier) bool {
	return (leftAlias == rootAlias && rightAlias == terminalAlias) ||
		(leftAlias == terminalAlias && rightAlias == rootAlias)
}

func isEndpointInequality(expression pgsql.Expression, sourceFrame, rootAlias, terminalAlias pgsql.Identifier) bool {
	binaryExpression, isBinaryExpression := unwrapParenthetical(expression).(*pgsql.BinaryExpression)
	if !isBinaryExpression ||
		!binaryExpression.Operator.IsIn(pgsql.OperatorNotEquals, pgsql.OperatorCypherNotEquals) {
		return false
	}

	leftAlias, hasLeftAlias := endpointIDReference(binaryExpression.LOperand, sourceFrame)
	rightAlias, hasRightAlias := endpointIDReference(binaryExpression.ROperand, sourceFrame)

	return hasLeftAlias && hasRightAlias && isEndpointAliasPair(leftAlias, rightAlias, rootAlias, terminalAlias)
}

func shortestPathLimitPushdownTransparentWhere(currentPart *QueryPart, sourceFrame pgsql.Identifier, where pgsql.Expression) bool {
	if where == nil {
		return true
	}

	sourceCTE := findCTE(currentPart.Model, sourceFrame)
	if sourceCTE == nil {
		return false
	}

	rootAlias, terminalAlias, hasEndpointAliases := shortestPathEndpointAliases(sourceCTE.Query)
	if !hasEndpointAliases || !shortestPathSourceWhereTransparent(sourceCTE.Query, rootAlias, terminalAlias) {
		return false
	}

	// After the source CTE is known to be transparent, the final tail WHERE may
	// only contain the endpoint anti-reflexive predicate. Pushing LIMIT through
	// arbitrary filters could drop shorter paths that would have survived later.
	for _, term := range flattenConjunction(unwrapParenthetical(where)) {
		if !isEndpointInequality(term, sourceFrame, rootAlias, terminalAlias) {
			return false
		}
	}

	return true
}

func limitPushdownTailSource(currentPart *QueryPart, tailSelect pgsql.Select) (pgsql.Identifier, bool) {
	// Keep this intentionally narrow: LIMIT can move into the harness only when
	// the tail SELECT is a simple pass-through over one shortest-path CTE. Sorts,
	// grouping, aggregation, DISTINCT, SKIP, and writes all change which rows the
	// outer LIMIT would observe.
	if currentPart.Limit == nil ||
		currentPart.Skip != nil ||
		len(currentPart.SortItems) > 0 ||
		currentPart.numReadingClauses != 1 ||
		currentPart.numUpdatingClauses > 0 ||
		currentPart.HasMutations() ||
		currentPart.HasDeletions() {
		return "", false
	}

	if currentPart.projections != nil && currentPart.projections.Distinct {
		return "", false
	}

	if len(tailSelect.GroupBy) > 0 || tailSelect.Having != nil {
		return "", false
	}

	if selectContainsAggregate(tailSelect) {
		return "", false
	}

	if len(tailSelect.From) != 1 || len(tailSelect.From[0].Joins) > 0 {
		return "", false
	}

	tableReference, finalSourceIsCTE := tailSelect.From[0].Source.(pgsql.TableReference)
	if !finalSourceIsCTE || len(tableReference.Name) != 1 {
		return "", false
	}

	sourceFrame := tableReference.Name.Root()
	if !shortestPathLimitPushdownTransparentWhere(currentPart, sourceFrame, tailSelect.Where) {
		return "", false
	}

	return sourceFrame, true
}

func pushDownShortestPathLimit(currentPart *QueryPart, tailSelect pgsql.Select) bool {
	sourceFrame, canPushDown := limitPushdownTailSource(currentPart, tailSelect)
	if !canPushDown {
		return false
	}

	if sourceCTE := findCTE(currentPart.Model, sourceFrame); sourceCTE != nil &&
		currentPart.CanPushDownLimitTo(sourceFrame) &&
		countLimitPushdownShortestPathHarnessCalls(sourceCTE.Query) == 1 {
		// Multiple harness calls in one source CTE would make one outer LIMIT
		// ambiguous, so only the single-harness case is rewritten.
		appendLimitToShortestPathHarness(&sourceCTE.Query, currentPart.Limit)
		return true
	}

	return false
}

func findCTE(query *pgsql.Query, cteName pgsql.Identifier) *pgsql.CommonTableExpression {
	if query.CommonTableExpressions == nil {
		return nil
	}

	for idx := range query.CommonTableExpressions.Expressions {
		nextCTE := &query.CommonTableExpressions.Expressions[idx]

		if nextCTE.Alias.Name == cteName {
			return nextCTE
		}
	}

	return nil
}

func applyLimitToCTE(query *pgsql.Query, cteName pgsql.Identifier, limit pgsql.Expression) bool {
	if cte := findCTE(query, cteName); cte != nil {
		cte.Query.Limit = limit
		return true
	}

	return false
}

func pushDownTraversalLimit(currentPart *QueryPart, tailSelect pgsql.Select) bool {
	sourceFrame, canPushDown := limitPushdownTailSource(currentPart, tailSelect)
	if !canPushDown || !currentPart.CanPushDownLimitTo(sourceFrame) {
		return false
	}

	return applyLimitToCTE(currentPart.Model, sourceFrame, currentPart.Limit)
}

func projectionAliasBindings(scope *Scope, projections []*Projection) map[pgsql.Identifier]pgsql.Identifier {
	aliases := map[pgsql.Identifier]pgsql.Identifier{}

	for _, projection := range projections {
		if !projection.Alias.Set {
			continue
		}

		if binding, bound := scope.AliasedLookup(projection.Alias.Value); bound {
			aliases[binding.Identifier] = projection.Alias.Value
		}
	}

	return aliases
}

func rewriteOrderByProjectionAlias(orderBy *pgsql.OrderBy, aliases map[pgsql.Identifier]pgsql.Identifier) {
	identifier, isIdentifier := orderBy.Expression.(pgsql.Identifier)
	if !isIdentifier {
		return
	}

	if alias, isProjectionAlias := aliases[identifier]; isProjectionAlias {
		orderBy.Expression = alias
	}
}

func tailPathCompositeStageBindings(scope *Scope, expression pgsql.Expression) ([]*BoundIdentifier, error) {
	if expression == nil {
		return nil, nil
	}

	var (
		bindings = make([]*BoundIdentifier, 0)
		seen     = map[pgsql.Identifier]struct{}{}
	)

	if err := walk.PgSQL(expression, walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, _ walk.VisitorHandler) {
		reference, isRowColumnReference := node.(pgsql.RowColumnReference)
		if !isRowColumnReference || reference.Column != pgsql.ColumnNodes {
			return
		}

		identifier, isIdentifier := unwrapParenthetical(reference.Identifier).(pgsql.Identifier)
		if !isIdentifier {
			return
		}

		binding, bound := scope.Lookup(identifier)
		if !bound {
			binding, bound = scope.AliasedLookup(identifier)
		}
		if !bound || binding.DataType != pgsql.PathComposite || binding.LastProjection != nil {
			return
		}

		if _, alreadySeen := seen[binding.Identifier]; alreadySeen {
			return
		}

		seen[binding.Identifier] = struct{}{}
		bindings = append(bindings, binding)
	})); err != nil {
		return nil, err
	}

	return bindings, nil
}

func (s *Translator) stageTailPathCompositeBindings(fromClauses []pgsql.FromClause, bindings []*BoundIdentifier) ([]pgsql.FromClause, error) {
	for _, binding := range bindings {
		stageBinding, err := s.scope.DefineNew(pgsql.Scope)
		if err != nil {
			return nil, err
		}

		stageFrame := &Frame{
			Binding:         stageBinding,
			Visible:         pgsql.AsIdentifierSet(binding.Identifier),
			Exported:        pgsql.AsIdentifierSet(binding.Identifier),
			stashedVisible:  pgsql.NewIdentifierSet(),
			stashedExported: pgsql.NewIdentifierSet(),
			Synthetic:       true,
		}

		stageProjection, err := buildProjection(binding.Identifier, binding, s.scope, binding.LastProjection)
		if err != nil {
			return nil, err
		}

		fromClauses = append(fromClauses, pgsql.FromClause{
			Source: pgsql.LateralSubquery{
				Query: pgsql.Query{
					Body: pgsql.Select{
						Projection: stageProjection,
					},
					Offset: pgsql.NewLiteral(0, pgsql.Int),
				},
				Binding: models.OptionalValue(stageBinding.Identifier),
			},
		})

		binding.MaterializedBy(stageFrame)
	}

	return fromClauses, nil
}

func (s *Translator) buildTailProjection() error {
	var (
		currentPart           = s.query.CurrentPart()
		singlePartQuerySelect = pgsql.Select{
			Distinct: currentPart.projections.Distinct,
		}
	)

	singlePartQuerySelect.From = s.collectProjectionFromFrames(currentPart.projections.Items)
	singlePartQuerySelect.From = append(singlePartQuerySelect.From, unwindFromClauses(currentPart.ConsumeUnwindClauses())...)

	if projectionConstraint, err := s.treeTranslator.ConsumeAllConstraints(); err != nil {
		return err
	} else if stagedBindings, err := tailPathCompositeStageBindings(s.scope, projectionConstraint.Expression); err != nil {
		return err
	} else if stagedFromClauses, err := s.stageTailPathCompositeBindings(singlePartQuerySelect.From, stagedBindings); err != nil {
		return err
	} else if projection, err := buildExternalProjection(s.scope, currentPart.projections.Items); err != nil {
		return err
	} else if resolvedConstraint, err := resolvePathCompositeFieldReferences(s.scope, projectionConstraint.Expression); err != nil {
		return err
	} else if err := RewriteFrameBindings(s.scope, resolvedConstraint); err != nil {
		return err
	} else {
		singlePartQuerySelect.From = stagedFromClauses
		singlePartQuerySelect.Projection = projection
		singlePartQuerySelect.Where = resolvedConstraint

		// Apply GROUP BY logic after projections are built and frame bindings are rewritten
		if currentPart.HasProjections() {
			var (
				hasAggregates     = false
				nonAggregateExprs = []pgsql.Expression{}
			)

			// Check if any projections contain aggregate functions
			for _, projectionItem := range currentPart.projections.Items {
				if aggregatedFunctionSymbols, err := GetAggregatedFunctionParameterSymbolsIn(projectionItem.SelectItem); err != nil {
					return err
				} else if !aggregatedFunctionSymbols.IsEmpty() {
					hasAggregates = true
					continue
				}
			}

			// If aggregates are present, collect non-aggregate expressions for GROUP BY
			if hasAggregates {
				for _, projectionItem := range projection {
					if groupByExpressions, err := NonAggregateGroupByExpressions(projectionItem); err != nil {
						return err
					} else {
						nonAggregateExprs = append(nonAggregateExprs, groupByExpressions...)
					}
				}

				// Add non-aggregate expressions to GROUP BY
				singlePartQuerySelect.GroupBy = nonAggregateExprs
			}
		}
	}

	currentPart.Model.Body = singlePartQuerySelect
	if pushDownShortestPathLimit(currentPart, singlePartQuerySelect) ||
		pushDownTraversalLimit(currentPart, singlePartQuerySelect) {
		s.recordLowering(optimize.LoweringLimitPushdown)
	}

	if currentPart.Skip != nil {
		currentPart.Model.Offset = currentPart.Skip
	}

	if currentPart.Limit != nil {
		currentPart.Model.Limit = currentPart.Limit
	}

	if len(currentPart.SortItems) > 0 {
		projectionAliases := projectionAliasBindings(s.scope, currentPart.projections.Items)

		// If there are expressions in the order by of the current query part they will need to be visited to ensure
		// that frame references are rewritten
		for _, orderByExpression := range currentPart.SortItems {
			if err := RewriteFrameBindings(s.scope, orderByExpression); err != nil {
				return err
			}

			rewriteOrderByProjectionAlias(orderByExpression, projectionAliases)
		}

		currentPart.Model.OrderBy = currentPart.SortItems
	}

	return nil
}

func (s *Translator) ensureProjectionAliasBinding(alias pgsql.Identifier, selectItem pgsql.SelectItem) error {
	if _, isBound := s.scope.AliasedLookup(alias); isBound {
		return nil
	}

	inferredType, err := s.inferExpressionType(selectItem)
	if err != nil {
		return err
	}

	newBinding, err := s.scope.DefineNew(inferredType)
	if err != nil {
		return err
	}

	s.scope.Alias(alias, newBinding)
	return nil
}

func (s *Translator) ensureSortItemProjectionAliases() error {
	currentPart := s.query.CurrentPart()
	if currentPart.projections == nil {
		return nil
	}

	for _, projection := range currentPart.projections.Items {
		if !projection.Alias.Set {
			continue
		}

		if _, isIdentifier := unwrapParenthetical(projection.SelectItem).(pgsql.Identifier); !isIdentifier {
			continue
		}

		if err := s.ensureProjectionAliasBinding(projection.Alias.Value, projection.SelectItem); err != nil {
			return err
		}
	}

	return nil
}

func (s *Translator) translateProjectionItem(scope *Scope, projectionItem *cypher.ProjectionItem) error {
	if alias, hasAlias, err := extractIdentifierFromCypherExpression(projectionItem); err != nil {
		return err
	} else if nextExpression, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if selectItem, isProjection := nextExpression.(pgsql.SelectItem); !isProjection {
		s.SetErrorf("invalid type for select item: %T", nextExpression)
	} else {
		if identifiers, err := ExtractSyntaxNodeReferences(selectItem); err != nil {
			return err
		} else if identifiers.Len() > 0 {
			// Identifier lookups will require a scope reference
			s.query.CurrentPart().projections.Frame = s.scope.CurrentFrame()
		}

		switch typedSelectItem := unwrapParenthetical(selectItem).(type) {
		case pgsql.Identifier:
			// If this is an identifier then assume the identifier as the projection alias since the translator
			// rewrites all identifiers
			if !hasAlias {
				if boundSelectItem, bound := scope.Lookup(typedSelectItem); !bound {
					return fmt.Errorf("invalid identifier: %s", typedSelectItem)
				} else {
					s.query.CurrentPart().CurrentProjection().SetAlias(boundSelectItem.Aliased())
				}
			}

		case *pgsql.BinaryExpression:
			// Binary expressions are used when properties are returned from a result projection
			// e.g. match (n) return n.prop
			if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(typedSelectItem); isPropertyLookup {
				// Ensure that projections maintain the raw JSONB type of the field
				propertyLookup.Operator = pgsql.OperatorJSONField
			}

			if hasAlias {
				if err := s.ensureProjectionAliasBinding(alias, selectItem); err != nil {
					return err
				}
			}

		default:
			if hasAlias {
				if err := s.ensureProjectionAliasBinding(alias, selectItem); err != nil {
					return err
				}
			}
		}

		if hasAlias {
			s.query.CurrentPart().CurrentProjection().SetAlias(alias)
		}

		s.query.CurrentPart().CurrentProjection().SelectItem = selectItem
	}

	return nil
}

func (s *Translator) prepareProjection(projection *cypher.Projection) error {
	currentPart := s.query.CurrentPart()
	currentPart.PrepareProjections(projection.Distinct)

	if projection.Skip != nil {
		if cypherLiteral, isLiteral := projection.Skip.Value.(*cypher.Literal); !isLiteral {
			return fmt.Errorf("expected a literal skip value but received: %T", projection.Skip.Value)
		} else if pgLiteral, err := pgsql.AsLiteral(cypherLiteral.Value); err != nil {
			return err
		} else {
			currentPart.Skip = pgLiteral
		}
	}

	if projection.Limit != nil {
		if cypherLiteral, isLiteral := projection.Limit.Value.(*cypher.Literal); !isLiteral {
			return fmt.Errorf("expected a literal limit value but received: %T", projection.Limit.Value)
		} else if pgLiteral, err := pgsql.AsLiteral(cypherLiteral.Value); err != nil {
			return err
		} else {
			currentPart.Limit = pgLiteral
		}
	}

	return nil
}
