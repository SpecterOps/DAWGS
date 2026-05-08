package translate

import (
	"fmt"
	"sort"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

func createIDIdentifier(binding *BoundIdentifier) pgsql.Identifier {
	return pgsql.Identifier(binding.Identifier.String() + "_id")
}

func sequenceValue(table pgsql.Identifier) pgsql.Expression {
	return pgsql.FunctionCall{
		Function: pgsql.FunctionNextValue,
		Parameters: []pgsql.Expression{
			pgsql.FunctionCall{
				Function: pgsql.FunctionPGGetSerialSequence,
				Parameters: []pgsql.Expression{
					pgsql.NewLiteral(table.String(), pgsql.Text),
					pgsql.NewLiteral(pgsql.ColumnID.String(), pgsql.Text),
				},
			},
		},
		CastType: pgsql.Int8,
	}
}

func frameReference(frame *Frame) pgsql.FromClause {
	return pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{frame.Binding.Identifier},
		},
	}
}

func (s *Translator) createSourceFromClauses(previousFrame *Frame) []pgsql.FromClause {
	var fromClauses []pgsql.FromClause

	if previousFrame != nil && !previousFrame.Synthetic {
		fromClauses = append(fromClauses, frameReference(previousFrame))
	}

	return append(fromClauses, unwindFromClauses(s.query.CurrentPart().ConsumeUnwindClauses())...)
}

func bindingProjectionExpression(binding *BoundIdentifier, referenceFrame *Frame) pgsql.Expression {
	if referenceFrame != nil && !referenceFrame.Synthetic {
		return pgsql.CompoundIdentifier{referenceFrame.Binding.Identifier, binding.Identifier}
	}

	if binding.LastProjection != nil {
		return pgsql.CompoundIdentifier{binding.LastProjection.Binding.Identifier, binding.Identifier}
	}

	return binding.Identifier
}

func buildCarryProjection(scope *Scope, identifiers *pgsql.IdentifierSet, referenceFrame *Frame) (pgsql.Projection, error) {
	var projection pgsql.Projection

	for _, identifier := range identifiers.Slice() {
		binding, bound := scope.Lookup(identifier)
		if !bound {
			return nil, fmt.Errorf("missing bound identifier: %s", identifier)
		}

		projection = append(projection, &pgsql.AliasedExpression{
			Expression: bindingProjectionExpression(binding, referenceFrame),
			Alias:      pgsql.AsOptionalIdentifier(identifier),
		})
	}

	return projection, nil
}

func (s *Translator) addCTE(frame *Frame, body pgsql.SetExpression) {
	s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: frame.Binding.Identifier,
		},
		Query: pgsql.Query{
			Body: body,
		},
	})
}

func (s *Translator) buildCreateSourceFrame(idAlias pgsql.Identifier, sequenceTable pgsql.Identifier) (*Frame, *pgsql.IdentifierSet, error) {
	var (
		currentFrame = s.scope.CurrentFrame()
		carried      = pgsql.NewIdentifierSet()
	)

	if currentFrame != nil {
		carried = currentFrame.Known()
	}

	// Reserve the database ID before the INSERT CTE runs. This lets later edge
	// creations in the same Cypher CREATE clause refer to newly-created nodes.
	sourceProjection, err := buildCarryProjection(s.scope, carried, currentFrame)
	if err != nil {
		return nil, nil, err
	}

	sourceProjection = append(sourceProjection, &pgsql.AliasedExpression{
		Expression: sequenceValue(sequenceTable),
		Alias:      pgsql.AsOptionalIdentifier(idAlias),
	})

	sourceFrame, err := s.scope.PushFrame()
	if err != nil {
		return nil, nil, err
	}

	sourceFrame.Visible = carried.Copy()
	sourceFrame.Exported = carried.Copy()

	s.addCTE(sourceFrame, pgsql.Select{
		Projection: sourceProjection,
		From:       s.createSourceFromClauses(currentFrame),
	})

	for _, identifier := range carried.Slice() {
		if binding, bound := s.scope.Lookup(identifier); bound {
			binding.MaterializedBy(sourceFrame)
		}
	}

	return sourceFrame, carried, nil
}

func (s *Translator) buildCreateProjectionFrame(sourceFrame, insertFrame *Frame, carried *pgsql.IdentifierSet, createdBinding *BoundIdentifier, idAlias pgsql.Identifier) (*Frame, error) {
	projection, err := buildCarryProjection(s.scope, carried, sourceFrame)
	if err != nil {
		return nil, err
	}

	projection = append(projection, &pgsql.AliasedExpression{
		Expression: pgsql.CompoundIdentifier{insertFrame.Binding.Identifier, createdBinding.Identifier},
		Alias:      pgsql.AsOptionalIdentifier(createdBinding.Identifier),
	})

	projectFrame, err := s.scope.PushFrame()
	if err != nil {
		return nil, err
	}

	// Join the source and insert CTEs by the preallocated ID so row cardinality
	// from the incoming pipeline is preserved after RETURNING materializes the composite.
	carried = carried.Copy().Add(createdBinding.Identifier)
	projectFrame.Visible = carried.Copy()
	projectFrame.Exported = carried.Copy()

	s.addCTE(projectFrame, pgsql.Select{
		Projection: projection,
		From: []pgsql.FromClause{
			frameReference(sourceFrame),
			frameReference(insertFrame),
		},
		Where: pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{insertFrame.Binding.Identifier, idAlias},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{sourceFrame.Binding.Identifier, idAlias},
		),
	})

	for _, identifier := range carried.Slice() {
		if binding, bound := s.scope.Lookup(identifier); bound {
			binding.MaterializedBy(projectFrame)
		}
	}

	return projectFrame, nil
}

func (s *Translator) translateCreate(_ *cypher.Create) error {
	s.query.CurrentPart().isCreating = false

	return nil
}

func (s *Translator) buildCreates() error {
	if err := s.buildNodeCreations(); err != nil {
		return err
	}

	return s.buildEdgeCreations()
}

func (s *Translator) buildNodeCreations() error {
	currentQueryPart := s.query.CurrentPart()

	for _, nodeCreate := range currentQueryPart.mutations.Creations.Values() {
		idAlias := createIDIdentifier(nodeCreate.Binding)

		// Each CREATE item becomes a source CTE, an INSERT CTE, and a projection
		// CTE. Keeping them separate preserves the existing frame pipeline while
		// making created values visible to later CREATE items.
		sourceFrame, carried, err := s.buildCreateSourceFrame(idAlias, pgsql.TableNode)
		if err != nil {
			return err
		}

		kindIDsExpr, err := s.buildKindIDsArray(nodeCreate)
		if err != nil {
			return err
		}

		propsExpr, err := s.buildPropertiesObject(nodeCreate.Properties)
		if err != nil {
			return err
		}

		insertFrame, err := s.scope.PushFrame()
		if err != nil {
			return err
		}

		s.addCTE(insertFrame, pgsql.Insert{
			Table: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{pgsql.TableNode},
			},
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{
				pgsql.ColumnGraphID,
				pgsql.ColumnID,
				pgsql.ColumnKindIDs,
				pgsql.ColumnProperties,
			}),
			Source: &pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgsql.NewLiteral(s.graphID, pgsql.Int4),
						pgsql.CompoundIdentifier{sourceFrame.Binding.Identifier, idAlias},
						kindIDsExpr,
						propsExpr,
					},
					From: []pgsql.FromClause{frameReference(sourceFrame)},
				},
			},
			Returning: []pgsql.SelectItem{
				&pgsql.AliasedExpression{
					Expression: pgsql.ColumnID,
					Alias:      pgsql.AsOptionalIdentifier(idAlias),
				},
				&pgsql.AliasedExpression{
					Expression: pgsql.CompositeValue{
						DataType: pgsql.NodeComposite,
						Values: []pgsql.Expression{
							pgsql.ColumnID,
							pgsql.ColumnKindIDs,
							pgsql.ColumnProperties,
						},
					},
					Alias: pgsql.AsOptionalIdentifier(nodeCreate.Binding.Identifier),
				},
			},
		})

		if _, err := s.buildCreateProjectionFrame(sourceFrame, insertFrame, carried, nodeCreate.Binding, idAlias); err != nil {
			return err
		}
	}

	return nil
}

func (s *Translator) buildKindIDsArray(nodeCreate *NodeCreate) (pgsql.SelectItem, error) {
	if len(nodeCreate.Kinds) == 0 {
		return pgsql.ArrayLiteral{
			Values:   []pgsql.Expression{},
			CastType: pgsql.Int2Array,
		}, nil
	}

	kindIDs, err := s.kindMapper.AssertKinds(nodeCreate.Kinds)
	if err != nil {
		return nil, fmt.Errorf("failed to translate kinds: %w", err)
	}

	arrayLiteral := pgsql.ArrayLiteral{
		Values:   make([]pgsql.Expression, len(kindIDs)),
		CastType: pgsql.Int2Array,
	}

	for idx, kindID := range kindIDs {
		arrayLiteral.Values[idx] = pgsql.NewLiteral(kindID, pgsql.Int2)
	}

	return arrayLiteral, nil
}

func (s *Translator) buildPropertiesObject(properties map[string]pgsql.Expression) (pgsql.SelectItem, error) {
	jsonObjectFunction := pgsql.FunctionCall{
		Function: pgsql.FunctionJSONBBuildObject,
		CastType: pgsql.JSONB,
	}

	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := properties[key]
		if err := RewriteFrameBindings(s.scope, value); err != nil {
			return nil, err
		}

		jsonObjectFunction.Parameters = append(jsonObjectFunction.Parameters,
			pgsql.NewLiteral(key, pgsql.Text),
			value,
		)
	}

	return jsonObjectFunction, nil
}

func (s *Translator) buildEdgeCreations() error {
	currentQueryPart := s.query.CurrentPart()

	for _, edgeCreate := range currentQueryPart.mutations.EdgeCreations.Values() {
		if edgeCreate.LeftNode == nil || edgeCreate.RightNode == nil {
			return fmt.Errorf("edge creation %s is missing endpoint nodes", edgeCreate.Binding.Identifier)
		}

		idAlias := createIDIdentifier(edgeCreate.Binding)

		// Edge creation runs after node creation so endpoint references can resolve
		// either carried-in nodes or nodes inserted earlier in this CREATE clause.
		sourceFrame, carried, err := s.buildCreateSourceFrame(idAlias, pgsql.TableEdge)
		if err != nil {
			return err
		}

		startNode, endNode, err := resolveEdgeEndpoints(edgeCreate)
		if err != nil {
			return err
		}

		kindIDExpr, err := s.buildEdgeKindIDExpression(edgeCreate)
		if err != nil {
			return err
		}

		startIDRef := createdNodeIDRef(sourceFrame, startNode)
		endIDRef := createdNodeIDRef(sourceFrame, endNode)

		propsExpr, err := s.buildPropertiesObject(edgeCreate.Properties)
		if err != nil {
			return err
		}

		insertFrame, err := s.scope.PushFrame()
		if err != nil {
			return err
		}

		s.addCTE(insertFrame, pgsql.Insert{
			Table: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{pgsql.TableEdge},
			},
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{
				pgsql.ColumnGraphID,
				pgsql.ColumnID,
				pgsql.ColumnStartID,
				pgsql.ColumnEndID,
				pgsql.ColumnKindID,
				pgsql.ColumnProperties,
			}),
			Source: &pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgsql.NewLiteral(s.graphID, pgsql.Int4),
						pgsql.CompoundIdentifier{sourceFrame.Binding.Identifier, idAlias},
						startIDRef,
						endIDRef,
						kindIDExpr,
						propsExpr,
					},
					From: []pgsql.FromClause{frameReference(sourceFrame)},
				},
			},
			Returning: []pgsql.SelectItem{
				&pgsql.AliasedExpression{
					Expression: pgsql.ColumnID,
					Alias:      pgsql.AsOptionalIdentifier(idAlias),
				},
				&pgsql.AliasedExpression{
					Expression: pgsql.CompositeValue{
						DataType: pgsql.EdgeComposite,
						Values: []pgsql.Expression{
							pgsql.ColumnID,
							pgsql.ColumnStartID,
							pgsql.ColumnEndID,
							pgsql.ColumnKindID,
							pgsql.ColumnProperties,
						},
					},
					Alias: pgsql.AsOptionalIdentifier(edgeCreate.Binding.Identifier),
				},
			},
		})

		if _, err := s.buildCreateProjectionFrame(sourceFrame, insertFrame, carried, edgeCreate.Binding, idAlias); err != nil {
			return err
		}
	}

	return nil
}

func resolveEdgeEndpoints(edgeCreate *EdgeCreate) (*BoundIdentifier, *BoundIdentifier, error) {
	switch edgeCreate.Direction {
	case graph.DirectionOutbound:
		return edgeCreate.LeftNode, edgeCreate.RightNode, nil

	case graph.DirectionInbound:
		return edgeCreate.RightNode, edgeCreate.LeftNode, nil

	default:
		return nil, nil, fmt.Errorf("unsupported direction for edge creation: %v", edgeCreate.Direction)
	}
}

func (s *Translator) buildEdgeKindIDExpression(edgeCreate *EdgeCreate) (pgsql.SelectItem, error) {
	if len(edgeCreate.Kinds) == 0 {
		return nil, fmt.Errorf("edge creation requires exactly one kind but none were specified")
	}

	if len(edgeCreate.Kinds) > 1 {
		return nil, fmt.Errorf("edge creation supports only one kind but %d were specified", len(edgeCreate.Kinds))
	}

	if kindIDs, err := s.kindMapper.AssertKinds(edgeCreate.Kinds); err != nil {
		return nil, fmt.Errorf("failed to translate edge kind: %w", err)
	} else {
		return pgsql.NewLiteral(kindIDs[0], pgsql.Int2), nil
	}
}

func createdNodeIDRef(sourceFrame *Frame, nodeBinding *BoundIdentifier) pgsql.SelectItem {
	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{
			sourceFrame.Binding.Identifier,
			nodeBinding.Identifier,
		},
		Column: pgsql.ColumnID,
	}
}
