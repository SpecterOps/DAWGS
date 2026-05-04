package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
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

func buildProjectionForPathComposite(alias pgsql.Identifier, projected *BoundIdentifier, scope *Scope) ([]pgsql.SelectItem, error) {
	if projected.LastProjection != nil {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{projected.LastProjection.Binding.Identifier, projected.Identifier},
				Alias:      pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}

	var (
		parameterExpression    pgsql.Expression
		preExpansionEdgeRefs   []pgsql.Expression
		postExpansionEdgeRefs  []pgsql.Expression
		nodeReferences         []pgsql.Expression
		directNodeReferences   []pgsql.Expression
		directEdgeReferences   []pgsql.Expression
		seenExpansionPath      = false
		useEdgesToPathFunction = false
	)

	// Path composite components are encoded as dependencies on the bound identifier representing the
	// path. This is not ideal as it escapes normal translation flow as driven by the structure of the
	// originating cypher AST.
	for _, dependency := range projected.Dependencies {
		switch dependency.DataType {
		case pgsql.ExpansionPath:
			seenExpansionPath = true
			useEdgesToPathFunction = true

			parameterExpression = pgsql.OptionalBinaryExpressionJoin(
				parameterExpression, pgsql.OperatorConcatenate, dependency.Identifier,
			)

		case pgsql.EdgeComposite:
			useEdgesToPathFunction = true
			directEdgeReferences = append(directEdgeReferences, pgsql.CompoundIdentifier{
				scope.CurrentFrameBinding().Identifier,
				dependency.Identifier,
			})

			ref := rewriteCompositeTypeFieldReference(
				scope.CurrentFrameBinding().Identifier,
				pgsql.CompoundIdentifier{dependency.Identifier, pgsql.ColumnID},
			)

			if seenExpansionPath {
				postExpansionEdgeRefs = append(postExpansionEdgeRefs, ref)
			} else {
				preExpansionEdgeRefs = append(preExpansionEdgeRefs, ref)
			}

		case pgsql.NodeComposite:
			directNodeReferences = append(directNodeReferences, pgsql.CompoundIdentifier{
				scope.CurrentFrameBinding().Identifier,
				dependency.Identifier,
			})

			nodeReferences = append(nodeReferences, rewriteCompositeTypeFieldReference(
				scope.CurrentFrameBinding().Identifier,
				pgsql.CompoundIdentifier{dependency.Identifier, pgsql.ColumnID},
			))

		default:
			return nil, fmt.Errorf("unsupported type for path rendering: %s", dependency.DataType)
		}
	}

	// Direct, non-expansion path bindings already have their node and edge composites in scope. Keep
	// those explicit components instead of reconstructing the path from edge IDs: this preserves path
	// order and duplicate nodes, and it also works for rows produced by data-modifying CTEs where
	// re-reading node/edge tables in the same statement may not see the RETURNING values.
	if !seenExpansionPath && len(directNodeReferences) > 0 {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.CompositeValue{
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
				},
				Alias: pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}

	if useEdgesToPathFunction {
		// Pre-expansion edges go LEFT of the expansion (existing prepend semantics).
		if len(preExpansionEdgeRefs) > 0 {
			parameterExpression = pgsql.OptionalBinaryExpressionJoin(
				parameterExpression,
				pgsql.OperatorConcatenate,
				pgsql.ArrayLiteral{Values: preExpansionEdgeRefs, CastType: pgsql.Int8Array},
			)
		}

		// Post-expansion edges go RIGHT of the expansion — use NewBinaryExpression directly.
		if len(postExpansionEdgeRefs) > 0 {
			postArray := pgsql.ArrayLiteral{Values: postExpansionEdgeRefs, CastType: pgsql.Int8Array}

			if parameterExpression == nil {
				parameterExpression = postArray
			} else {
				parameterExpression = pgsql.NewBinaryExpression(
					parameterExpression, pgsql.OperatorConcatenate, postArray,
				)
			}
		}

		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.FunctionCall{
					Function: pgsql.FunctionEdgesToPath,
					Parameters: []pgsql.Expression{
						pgsql.Variadic{
							Expression: parameterExpression,
						},
					},
					CastType: pgsql.PathComposite,
				},
				Alias: pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	} else if len(nodeReferences) > 0 {
		return []pgsql.SelectItem{
			&pgsql.AliasedExpression{
				Expression: pgsql.FunctionCall{
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
				},
				Alias: pgsql.AsOptionalIdentifier(alias),
			},
		}, nil
	}

	return nil, fmt.Errorf("path variable does not contain valid components")
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
	var (
		edgeAggregateParameter = pgsql.CompositeValue{
			DataType: pgsql.EdgeComposite,
		}

		edgeWhereClause = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{projected.Identifier, pgsql.ColumnID},
			pgsql.OperatorEquals,
			pgsql.NewAnyExpression(
				pgsql.CompoundIdentifier{scope.CurrentFrame().Binding.Identifier, pgsql.ColumnPath},
				pgsql.ExpansionPath,
			),
		)
	)

	// Reference all of the edge table columns to create the edge composites
	for _, edgeTableColumn := range pgsql.EdgeTableColumns {
		edgeAggregateParameter.Values = append(edgeAggregateParameter.Values, pgsql.CompoundIdentifier{projected.Identifier, edgeTableColumn})
	}

	// Change the type to the node composite now that this is projected
	projected.DataType = pgsql.EdgeComposite

	// Create a new final projection that's aliased to the visible binding's identifier
	return []pgsql.SelectItem{
		&pgsql.AliasedExpression{
			Expression: &pgsql.Parenthetical{
				Expression: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgsql.FunctionCall{
							Function:   pgsql.FunctionArrayAggregate,
							Parameters: []pgsql.Expression{edgeAggregateParameter},
						},
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
							Binding: models.OptionalValue(projected.Identifier),
						},
						Joins: nil,
					}},
					Where: edgeWhereClause,
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

func pushDownShortestPathLimit(currentPart *QueryPart, tailSelect pgsql.Select) {
	sourceFrame, canPushDown := limitPushdownTailSource(currentPart, tailSelect)
	if !canPushDown {
		return
	}

	if sourceCTE := findCTE(currentPart.Model, sourceFrame); sourceCTE != nil &&
		countLimitPushdownShortestPathHarnessCalls(sourceCTE.Query) == 1 {
		// Multiple harness calls in one source CTE would make one outer LIMIT
		// ambiguous, so only the single-harness case is rewritten.
		appendLimitToShortestPathHarness(&sourceCTE.Query, currentPart.Limit)
	}
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

func pushDownTraversalLimit(currentPart *QueryPart, tailSelect pgsql.Select) {
	sourceFrame, canPushDown := limitPushdownTailSource(currentPart, tailSelect)
	if !canPushDown || !currentPart.CanPushDownLimitTo(sourceFrame) {
		return
	}

	applyLimitToCTE(currentPart.Model, sourceFrame, currentPart.Limit)
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
	} else if projection, err := buildExternalProjection(s.scope, currentPart.projections.Items); err != nil {
		return err
	} else if err := RewriteFrameBindings(s.scope, projectionConstraint.Expression); err != nil {
		return err
	} else {
		singlePartQuerySelect.Projection = projection
		singlePartQuerySelect.Where = projectionConstraint.Expression

		// Apply GROUP BY logic after projections are built and frame bindings are rewritten
		if currentPart.HasProjections() {
			var (
				hasAggregates     = false
				nonAggregateExprs = []pgsql.Expression{}
			)

			// Check if any projections contain aggregate functions
			for _, projectionItem := range currentPart.projections.Items {
				if typedSelectItem, ok := projectionItem.SelectItem.(pgsql.FunctionCall); ok {
					if aggregatedFunctionSymbols, err := GetAggregatedFunctionParameterSymbols(typedSelectItem); err != nil {
						return err
					} else if !aggregatedFunctionSymbols.IsEmpty() {
						hasAggregates = true
						continue
					}
				}
			}

			// If aggregates are present, collect non-aggregate expressions for GROUP BY
			if hasAggregates {
				for i, projectionItem := range currentPart.projections.Items {
					if typedSelectItem, ok := projectionItem.SelectItem.(pgsql.FunctionCall); ok {
						if aggregatedFunctionSymbols, err := GetAggregatedFunctionParameterSymbols(typedSelectItem); err != nil {
							return err
						} else if !aggregatedFunctionSymbols.IsEmpty() {
							// This is an aggregate function, skip it
							continue
						}
					}

					// Use the final processed projection expression for GROUP BY
					// This ensures the GROUP BY uses the same fully-qualified expressions as SELECT
					projExpr := projection[i]
					if aliasedExpr, isAliased := projExpr.(*pgsql.AliasedExpression); isAliased {
						nonAggregateExprs = append(nonAggregateExprs, aliasedExpr.Expression)
					} else {
						nonAggregateExprs = append(nonAggregateExprs, projExpr)
					}
				}

				// Add non-aggregate expressions to GROUP BY
				singlePartQuerySelect.GroupBy = nonAggregateExprs
			}
		}
	}

	currentPart.Model.Body = singlePartQuerySelect
	pushDownShortestPathLimit(currentPart, singlePartQuerySelect)
	pushDownTraversalLimit(currentPart, singlePartQuerySelect)

	if currentPart.Skip != nil {
		currentPart.Model.Offset = currentPart.Skip
	}

	if currentPart.Limit != nil {
		currentPart.Model.Limit = currentPart.Limit
	}

	if len(currentPart.SortItems) > 0 {
		// If there are expressions in the order by of the current query part they will need to be visited to ensure
		// that frame references are rewritten
		for _, orderByExpression := range currentPart.SortItems {
			if err := RewriteFrameBindings(s.scope, orderByExpression); err != nil {
				return err
			}
		}

		currentPart.Model.OrderBy = currentPart.SortItems
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

		default:
			if hasAlias {
				if inferredType, err := InferExpressionType(typedSelectItem); err != nil {
					return err
				} else if _, isBound := s.scope.AliasedLookup(alias); !isBound {
					if newBinding, err := s.scope.DefineNew(inferredType); err != nil {
						return err
					} else {
						// This binding is its own alias
						s.scope.Alias(alias, newBinding)
					}
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
