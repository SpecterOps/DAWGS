package translate

import (
	"fmt"
	"sort"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

func (s *Translator) translateCreate(create *cypher.Create) error {
	s.query.CurrentPart().isCreating = false

	if err := s.buildNodeCreations(); err != nil {
		return err
	}

	return s.buildEdgeCreations()
}

func (s *Translator) buildNodeCreations() error {
	currentQueryPart := s.query.CurrentPart()

	for _, nodeCreate := range currentQueryPart.mutations.Creations.Values() {
		if insertFrame, err := s.scope.PushFrame(); err != nil {
			return err
		} else {
			// Build the kind_ids array expression
			kindIdsExpr, err := s.buildKindIDsArray(nodeCreate)
			if err != nil {
				return err
			}

			// Build the properties jsonb_build_object expression
			propsExpr := buildPropertiesObject(nodeCreate.Properties)

			// Build the RETURNING composite: (id, kind_ids, properties)::nodecomposite as n0
			returningItem := &pgsql.AliasedExpression{
				Expression: pgsql.CompositeValue{
					DataType: pgsql.NodeComposite,
					Values: []pgsql.Expression{
						pgsql.ColumnID,
						pgsql.ColumnKindIDs,
						pgsql.ColumnProperties,
					},
				},
				Alias: pgsql.AsOptionalIdentifier(nodeCreate.Binding.Identifier),
			}

			// Build the column list and value list, including graph_id when a
			// target graph is configured.
			columns := []pgsql.Identifier{
				pgsql.ColumnKindIDs,
				pgsql.ColumnProperties,
			}

			values := []pgsql.Expression{kindIdsExpr, propsExpr}

			if s.graphID != 0 {
				columns = append([]pgsql.Identifier{pgsql.ColumnGraphID}, columns...)
				values = append([]pgsql.Expression{pgsql.NewLiteral(s.graphID, pgsql.Int4)}, values...)
			}

			// Build the INSERT statement
			sqlInsert := pgsql.Insert{
				Table: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{pgsql.TableNode},
				},
				Shape: pgsql.NewRecordShape(columns),
				Source: &pgsql.Query{
					Body: pgsql.Values{
						Values: values,
					},
				},
				Returning: []pgsql.SelectItem{returningItem},
			}

			// Mark the binding as materialized by this frame so RETURN can reference it
			nodeCreate.Binding.MaterializedBy(insertFrame)

			// Export the binding so it is visible in subsequent query parts
			insertFrame.Export(nodeCreate.Binding.Identifier)

			// Add the CTE to the model
			currentQueryPart.Model.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: insertFrame.Binding.Identifier,
				},
				Query: pgsql.Query{
					Body: sqlInsert,
				},
			})
		}
	}

	return nil
}

// buildKindIDsArray produces the array[...]::int2[] expression for the given kinds.
func (s *Translator) buildKindIDsArray(nodeCreate *NodeCreate) (pgsql.Expression, error) {
	if len(nodeCreate.Kinds) == 0 {
		return pgsql.ArrayLiteral{
			Values:   []pgsql.Expression{},
			CastType: pgsql.Int2Array,
		}, nil
	}

	kindIDs, err := s.kindMapper.MapKinds(nodeCreate.Kinds)
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

// buildPropertiesObject produces a jsonb_build_object(...)::jsonb SelectItem expression from
// the given property map. Keys are sorted for deterministic output.
func buildPropertiesObject(properties map[string]pgsql.Expression) pgsql.SelectItem {
	jsonObjectFunction := pgsql.FunctionCall{
		Function: pgsql.FunctionJSONBBuildObject,
		CastType: pgsql.JSONB,
	}

	keys := make([]string, 0, len(properties))
	for k := range properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		jsonObjectFunction.Parameters = append(jsonObjectFunction.Parameters,
			pgsql.NewLiteral(key, pgsql.Text),
			properties[key],
		)
	}

	return jsonObjectFunction
}

// buildEdgeCreations emits an insert CTE for every edge collected in the current create clause. It must be called after
// buildNodeCreations so that every node binding already has its `LastProjection` frame set.
func (s *Translator) buildEdgeCreations() error {
	currentQueryPart := s.query.CurrentPart()

	for _, edgeCreate := range currentQueryPart.mutations.EdgeCreations.Values() {
		if edgeCreate.RightNode == nil {
			return fmt.Errorf("edge creation %s is missing its right-hand node", edgeCreate.Binding.Identifier)
		}

		if insertFrame, err := s.scope.PushFrame(); err != nil {
			return err
		} else {
			// Resolve start/end nodes from the relationship direction.
			var startNode, endNode *BoundIdentifier

			switch edgeCreate.Direction {
			case graph.DirectionOutbound:
				startNode, endNode = edgeCreate.LeftNode, edgeCreate.RightNode

			case graph.DirectionInbound:
				startNode, endNode = edgeCreate.RightNode, edgeCreate.LeftNode

			default:
				return fmt.Errorf("unsupported direction for edge creation: %v", edgeCreate.Direction)
			}

			// Map the single edge kind to its int16 ID. Then build the start_id and end_id references: (frameID.nodeID).id
			if kindIDExpr, err := s.buildEdgeKindIDExpression(edgeCreate); err != nil {
				return err
			} else if startIDRef, err := buildCreatedNodeIDRef(startNode); err != nil {
				return err
			} else if endIDRef, err := buildCreatedNodeIDRef(endNode); err != nil {
				return err
			} else {
				// RETURNING (id, start_id, end_id, kind_id, properties)::edgecomposite as e0
				returningExpr := &pgsql.AliasedExpression{
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
				}

				// Build the column list and projection, including graph_id when a
				// target graph is configured.
				columns := []pgsql.Identifier{
					pgsql.ColumnStartID,
					pgsql.ColumnEndID,
					pgsql.ColumnKindID,
					pgsql.ColumnProperties,
				}

				projection := []pgsql.SelectItem{startIDRef, endIDRef, kindIDExpr, buildPropertiesObject(edgeCreate.Properties)}

				if s.graphID != 0 {
					columns = append([]pgsql.Identifier{pgsql.ColumnGraphID}, columns...)
					projection = append([]pgsql.SelectItem{pgsql.NewLiteral(s.graphID, pgsql.Int4)}, projection...)
				}

				// insert into edge (graph_id, start_id, end_id, kind_id, properties)
				// select <graphID>, <startIDRef>, <endIDRef>, <kindID>, <props> from <nodeFrames>
				sqlInsert := pgsql.Insert{
					Table: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{pgsql.TableEdge},
					},
					Shape: pgsql.NewRecordShape(columns),
					Source: &pgsql.Query{
						Body: pgsql.Select{
							Projection: projection,
							From:       buildEdgeNodeFromClauses(startNode, endNode),
						},
					},
					Returning: []pgsql.SelectItem{returningExpr},
				}

				edgeCreate.Binding.MaterializedBy(insertFrame)
				insertFrame.Export(edgeCreate.Binding.Identifier)

				currentQueryPart.Model.AddCTE(pgsql.CommonTableExpression{
					Alias: pgsql.TableAlias{
						Name: insertFrame.Binding.Identifier,
					},
					Query: pgsql.Query{
						Body: sqlInsert,
					},
				})
			}
		}
	}

	return nil
}

// buildEdgeKindIDExpression maps the edge's single kind to a typed int2 literal.
func (s *Translator) buildEdgeKindIDExpression(edgeCreate *EdgeCreate) (pgsql.SelectItem, error) {
	if len(edgeCreate.Kinds) == 0 {
		return nil, fmt.Errorf("edge creation requires exactly one kind but none were specified")
	}

	if len(edgeCreate.Kinds) > 1 {
		return nil, fmt.Errorf("edge creation supports only one kind but %d were specified", len(edgeCreate.Kinds))
	}

	if kindIDs, err := s.kindMapper.MapKinds(edgeCreate.Kinds); err != nil {
		return nil, fmt.Errorf("failed to translate edge kind: %w", err)
	} else {
		return pgsql.NewLiteral(kindIDs[0], pgsql.Int2), nil
	}
}

// buildCreatedNodeIDRef constructs a (frameID.nodeID).id RowColumnReference for the given node binding. The given binding
// must already have been materialized by a prior insert CTE.
func buildCreatedNodeIDRef(nodeBinding *BoundIdentifier) (pgsql.SelectItem, error) {
	if nodeBinding.LastProjection == nil {
		return nil, fmt.Errorf("node binding %s has not been materialized before edge creation", nodeBinding.Identifier)
	}

	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{
			nodeBinding.LastProjection.Binding.Identifier,
			nodeBinding.Identifier,
		},
		Column: pgsql.ColumnID,
	}, nil
}

// buildEdgeNodeFromClauses produces the from clauses for the edge insert select
func buildEdgeNodeFromClauses(startNode, endNode *BoundIdentifier) []pgsql.FromClause {
	builder := NewFromClauseBuilder()
	builder.AddBinding(startNode)
	builder.AddBinding(endNode)

	return builder.Clauses()
}
