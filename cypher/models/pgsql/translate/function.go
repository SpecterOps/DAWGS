package translate

import (
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/walk"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

const legacyToIntegerFunction = "toint"

func SymbolsFor(node pgsql.SyntaxNode) (*pgsql.SymbolTable, error) {
	instance := pgsql.NewSymbolTable()

	return instance, walk.PgSQL(node, walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, errorHandler walk.VisitorHandler) {
		switch typedNode := node.(type) {
		case pgsql.Identifier:
			instance.AddIdentifier(typedNode)

		case pgsql.CompoundIdentifier:
			instance.AddCompoundIdentifier(typedNode)
		}
	}))
}

func asFunctionCall(node pgsql.SyntaxNode) (pgsql.FunctionCall, bool) {
	switch typedNode := node.(type) {
	case pgsql.FunctionCall:
		return typedNode, true

	case *pgsql.FunctionCall:
		if typedNode == nil {
			return pgsql.FunctionCall{}, false
		}

		return *typedNode, true

	default:
		return pgsql.FunctionCall{}, false
	}
}

func GetAggregatedFunctionParameterSymbols(call pgsql.FunctionCall) (*pgsql.SymbolTable, error) {
	return GetAggregatedFunctionParameterSymbolsIn(call)
}

func GetAggregatedFunctionParameterSymbolsIn(node pgsql.SyntaxNode) (*pgsql.SymbolTable, error) {
	var (
		symbolTable = pgsql.NewSymbolTable()
	)

	return symbolTable, walk.PgSQL(node, walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, visitorHandler walk.VisitorHandler) {
		nextCall, isFunctionCall := asFunctionCall(node)
		if !isFunctionCall {
			return
		}
		if pgsql.IsAggregateFunction(nextCall.Function) {
			if functionParameterSymbols, err := SymbolsFor(nextCall); err != nil {
				visitorHandler.SetError(err)
				return
			} else {
				symbolTable.AddTable(functionParameterSymbols)
			}

			visitorHandler.Consume()
		}
	}))
}

func GetNonAggregatedSymbols(node pgsql.SyntaxNode) (*pgsql.SymbolTable, error) {
	instance := pgsql.NewSymbolTable()

	return instance, walk.PgSQL(node, walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, visitorHandler walk.VisitorHandler) {
		if functionCall, isFunctionCall := asFunctionCall(node); isFunctionCall && pgsql.IsAggregateFunction(functionCall.Function) {
			visitorHandler.Consume()
			return
		}

		switch typedNode := node.(type) {
		case pgsql.RowColumnReference:
			if symbols, err := SymbolsFor(typedNode.Identifier); err != nil {
				visitorHandler.SetError(err)
			} else {
				instance.AddTable(symbols)
			}

			visitorHandler.Consume()

		case pgsql.Identifier:
			instance.AddIdentifier(typedNode)

		case pgsql.CompoundIdentifier:
			instance.AddCompoundIdentifier(typedNode)
		}
	}))
}

func ContainsAggregateFunction(node pgsql.SyntaxNode) (bool, error) {
	containsAggregate := false

	return containsAggregate, walk.PgSQL(node, walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, visitorHandler walk.VisitorHandler) {
		if functionCall, isFunctionCall := asFunctionCall(node); isFunctionCall && pgsql.IsAggregateFunction(functionCall.Function) {
			containsAggregate = true
			visitorHandler.Consume()
		}
	}))
}

func appendIfReferencedGroupByExpression(groupByExpressions []pgsql.Expression, expression pgsql.Expression) ([]pgsql.Expression, error) {
	if references, err := ExtractSyntaxNodeReferences(expression); err != nil {
		return nil, err
	} else if references.Len() == 0 {
		return groupByExpressions, nil
	} else {
		return append(groupByExpressions, expression), nil
	}
}

func appendNonAggregateGroupByExpressions(groupByExpressions []pgsql.Expression, expressions ...pgsql.Expression) ([]pgsql.Expression, error) {
	for _, expression := range expressions {
		nextGroupByExpressions, err := NonAggregateGroupByExpressions(expression)
		if err != nil {
			return nil, err
		}

		groupByExpressions = append(groupByExpressions, nextGroupByExpressions...)
	}

	return groupByExpressions, nil
}

func NonAggregateGroupByExpressions(expression pgsql.Expression) ([]pgsql.Expression, error) {
	if expression == nil {
		return nil, nil
	}

	switch typedExpression := expression.(type) {
	case *pgsql.AliasedExpression:
		if typedExpression == nil {
			return nil, nil
		}

		return NonAggregateGroupByExpressions(typedExpression.Expression)

	case pgsql.AliasedExpression:
		return NonAggregateGroupByExpressions(typedExpression.Expression)
	}

	if functionCall, isFunctionCall := asFunctionCall(expression); isFunctionCall && pgsql.IsAggregateFunction(functionCall.Function) {
		return nil, nil
	}

	if containsAggregate, err := ContainsAggregateFunction(expression); err != nil {
		return nil, err
	} else if !containsAggregate {
		return appendIfReferencedGroupByExpression(nil, expression)
	}

	var groupByExpressions []pgsql.Expression

	switch typedExpression := expression.(type) {
	case *pgsql.BinaryExpression:
		if typedExpression == nil {
			return nil, nil
		}

		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.LOperand, typedExpression.ROperand)

	case pgsql.BinaryExpression:
		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.LOperand, typedExpression.ROperand)

	case *pgsql.UnaryExpression:
		if typedExpression == nil {
			return nil, nil
		}

		return NonAggregateGroupByExpressions(typedExpression.Operand)

	case pgsql.UnaryExpression:
		return NonAggregateGroupByExpressions(typedExpression.Operand)

	case *pgsql.Parenthetical:
		if typedExpression == nil {
			return nil, nil
		}

		return NonAggregateGroupByExpressions(typedExpression.Expression)

	case pgsql.TypeCast:
		return NonAggregateGroupByExpressions(typedExpression.Expression)

	case *pgsql.FunctionCall:
		if typedExpression == nil {
			return nil, nil
		}

		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Parameters...)

	case pgsql.FunctionCall:
		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Parameters...)

	case pgsql.ArrayLiteral:
		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Values...)

	case *pgsql.ArrayIndex:
		if typedExpression == nil {
			return nil, nil
		}

		groupByExpressions, err := appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Expression)
		if err != nil {
			return nil, err
		}

		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Indexes...)

	case pgsql.ArrayIndex:
		groupByExpressions, err := appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Expression)
		if err != nil {
			return nil, err
		}

		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Indexes...)

	case *pgsql.ArraySlice:
		if typedExpression == nil {
			return nil, nil
		}

		groupByExpressions, err := appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Expression)
		if err != nil {
			return nil, err
		}

		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Lower, typedExpression.Upper)

	case pgsql.ArraySlice:
		groupByExpressions, err := appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Expression)
		if err != nil {
			return nil, err
		}

		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Lower, typedExpression.Upper)

	case pgsql.Case:
		if typedExpression.Operand != nil {
			var err error
			groupByExpressions, err = appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Operand)
			if err != nil {
				return nil, err
			}
		}

		groupByExpressions, err := appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Conditions...)
		if err != nil {
			return nil, err
		}

		groupByExpressions, err = appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Then...)
		if err != nil {
			return nil, err
		}

		return appendNonAggregateGroupByExpressions(groupByExpressions, typedExpression.Else)

	default:
		return nil, fmt.Errorf("aggregate grouping expression has unsupported type %T", expression)
	}
}

func bindingExpressionType(binding *BoundIdentifier) pgsql.DataType {
	switch binding.DataType {
	case pgsql.ExpansionEdge:
		return pgsql.EdgeCompositeArray

	case pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
		return pgsql.NodeComposite

	case pgsql.PathEdge:
		return pgsql.Int8

	default:
		return binding.DataType
	}
}

func inferRowColumnReferenceType(expression pgsql.RowColumnReference) pgsql.DataType {
	switch expression.Column {
	case pgsql.ColumnGraphID, pgsql.ColumnID, pgsql.ColumnStartID, pgsql.ColumnEndID:
		return pgsql.Int8

	case pgsql.ColumnKindID:
		return pgsql.Int2

	case pgsql.ColumnKindIDs:
		return pgsql.Int2Array

	case pgsql.ColumnProperties:
		return pgsql.JSONB

	case pgsql.ColumnNodes:
		return pgsql.NodeCompositeArray

	case pgsql.ColumnEdges:
		return pgsql.EdgeCompositeArray

	default:
		return pgsql.UnknownDataType
	}
}

func (s *Translator) inferExpressionType(expression pgsql.Expression) (pgsql.DataType, error) {
	switch typedExpression := unwrapParenthetical(expression).(type) {
	case pgsql.Identifier:
		if binding, bound := s.scope.Lookup(typedExpression); bound {
			return bindingExpressionType(binding), nil
		}

		if binding, bound := s.scope.AliasedLookup(typedExpression); bound {
			return bindingExpressionType(binding), nil
		}

	case pgsql.CompoundIdentifier:
		if len(typedExpression) == 2 {
			return inferRowColumnReferenceType(pgsql.RowColumnReference{
				Identifier: typedExpression[0],
				Column:     typedExpression[1],
			}), nil
		}

	case pgsql.RowColumnReference:
		return inferRowColumnReferenceType(typedExpression), nil
	}

	return InferExpressionType(expression)
}

func (s *Translator) inferArrayExpressionType(expression pgsql.Expression) (pgsql.DataType, error) {
	if expressionType, err := s.inferExpressionType(expression); err != nil {
		return pgsql.UnsetDataType, err
	} else if expressionType == pgsql.ExpansionEdge {
		return pgsql.EdgeCompositeArray, nil
	} else if !expressionType.IsArrayType() {
		return pgsql.UnsetDataType, fmt.Errorf("expected array expression but received %s", expressionType)
	} else {
		return expressionType, nil
	}
}

func (s *Translator) expressionForPath(expression pgsql.Expression) (pgsql.Expression, error) {
	switch typedExpression := unwrapParenthetical(expression).(type) {
	case pgsql.Identifier:
		binding, bound := s.scope.Lookup(typedExpression)
		if !bound {
			binding, bound = s.scope.AliasedLookup(typedExpression)
		}

		if !bound {
			return nil, fmt.Errorf("unable to resolve path identifier %s", typedExpression)
		} else if binding.DataType != pgsql.PathComposite {
			return nil, fmt.Errorf("expected path expression but received %s", binding.DataType)
		} else {
			return expressionForPathComposite(binding, s.scope)
		}

	default:
		if expressionType, err := s.inferExpressionType(expression); err != nil {
			return nil, err
		} else if expressionType != pgsql.PathComposite {
			return nil, fmt.Errorf("expected path expression but received %s", expressionType)
		}

		return expression, nil
	}
}

func (s *Translator) translateHeadFunction(functionInvocation *cypher.FunctionInvocation) error {
	if functionInvocation.NumArguments() != 1 {
		return fmt.Errorf("expected only one argument for cypher function: %s", functionInvocation.Name)
	}

	if argument, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if arrayType, err := s.inferArrayExpressionType(argument); err != nil {
		return err
	} else {
		s.treeTranslator.PushOperand(&pgsql.ArrayIndex{
			Expression: pgsql.NewParenthetical(argument),
			Indexes: []pgsql.Expression{
				pgsql.NewLiteral(1, pgsql.Int),
			},
			CastType: arrayType.ArrayBaseType(),
		})
	}

	return nil
}

func (s *Translator) translateTailFunction(functionInvocation *cypher.FunctionInvocation) error {
	if functionInvocation.NumArguments() != 1 {
		return fmt.Errorf("expected only one argument for cypher function: %s", functionInvocation.Name)
	}

	if argument, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if arrayType, err := s.inferArrayExpressionType(argument); err != nil {
		return err
	} else {
		s.treeTranslator.PushOperand(pgsql.FunctionCall{
			Function: pgsql.FunctionCoalesce,
			Parameters: []pgsql.Expression{
				&pgsql.ArraySlice{
					Expression: pgsql.NewParenthetical(argument),
					Lower:      pgsql.NewLiteral(2, pgsql.Int),
					CastType:   arrayType,
				},
				pgsql.ArrayLiteral{
					CastType: arrayType,
				},
			},
			CastType: arrayType,
		})
	}

	return nil
}

func cypherMinMaxFunction(function pgsql.Identifier, argument pgsql.Expression) pgsql.FunctionCall {
	if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(argument); isPropertyLookup {
		propertyLookup.Operator = pgsql.OperatorJSONField

		switch function {
		case pgsql.FunctionMin:
			return pgsql.FunctionCall{
				Function:   pgsql.FunctionCypherMin,
				Parameters: []pgsql.Expression{propertyLookup},
				CastType:   pgsql.JSONB,
			}

		case pgsql.FunctionMax:
			return pgsql.FunctionCall{
				Function:   pgsql.FunctionCypherMax,
				Parameters: []pgsql.Expression{propertyLookup},
				CastType:   pgsql.JSONB,
			}
		}
	}

	return pgsql.FunctionCall{
		Function:   function,
		Parameters: []pgsql.Expression{argument},
	}
}

func (s *Translator) translatePathComponentFunction(functionInvocation *cypher.FunctionInvocation, column pgsql.Identifier, castType pgsql.DataType) error {
	if functionInvocation.NumArguments() != 1 {
		return fmt.Errorf("expected only one argument for cypher function: %s", functionInvocation.Name)
	}

	if argument, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if literal, isLiteral := argument.(pgsql.Literal); isLiteral && literal.Null {
		s.treeTranslator.PushOperand(pgsql.NewTypeCast(literal, castType))
	} else {
		if column == pgsql.ColumnEdges {
			if identifier, isIdentifier := unwrapParenthetical(argument).(pgsql.Identifier); isIdentifier {
				binding, bound := s.scope.Lookup(identifier)
				if !bound {
					binding, bound = s.scope.AliasedLookup(identifier)
				}

				if !bound {
					return fmt.Errorf("unable to resolve path identifier %s", identifier)
				} else if binding.DataType != pgsql.PathComposite {
					return fmt.Errorf("expected path expression but received %s", binding.DataType)
				}

				s.treeTranslator.PushOperand(pgsql.NewTypeCast(pgsql.RowColumnReference{
					Identifier: argument,
					Column:     column,
				}, castType))
				return nil
			}
		}

		if pathExpression, err := s.expressionForPath(argument); err != nil {
			return err
		} else {
			s.treeTranslator.PushOperand(pgsql.NewTypeCast(pgsql.RowColumnReference{
				Identifier: pathExpression,
				Column:     column,
			}, castType))
		}
	}

	return nil
}

func prepareCollectExpression(scope *Scope, collectedExpression pgsql.Expression, functionName string) (pgsql.Expression, pgsql.DataType, error) {
	castType := pgsql.AnyArray

	switch typedArgument := unwrapParenthetical(collectedExpression).(type) {
	case pgsql.Identifier:
		if binding, bound := scope.Lookup(typedArgument); !bound {
			return nil, pgsql.UnsetDataType, fmt.Errorf("binding not found for %s function argument %s", functionName, typedArgument)
		} else if bindingArrayType, err := binding.DataType.ToArrayType(); err != nil {
			return nil, pgsql.UnsetDataType, err
		} else {
			castType = bindingArrayType
		}

	case pgsql.TypeHinted:
		typeHint := typedArgument.TypeHint()
		if typeHint.IsArrayType() {
			return pgsql.FunctionCall{
				Function:   pgsql.FunctionToJSONB,
				Parameters: []pgsql.Expression{collectedExpression},
				CastType:   pgsql.JSONB,
			}, pgsql.JSONBArray, nil
		}

		if arrayType, err := typeHint.ToArrayType(); err == nil {
			castType = arrayType
		}
	}

	return collectedExpression, castType, nil
}

func translateNodeLabelsExpression(identifier pgsql.Identifier) pgsql.TypeHinted {
	const (
		kindAlias      pgsql.Identifier = "_kind"
		kindIndexAlias pgsql.Identifier = "_kind_idx"
	)

	kindIDs := pgsql.CompoundIdentifier{identifier, pgsql.ColumnKindIDs}

	return pgsql.NewTypeCast(pgsql.ArrayExpression{
		Expression: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{
					pgsql.CompoundIdentifier{kindAlias, pgsql.ColumnName},
				},
				From: []pgsql.FromClause{
					{
						Source: pgsql.AliasedExpression{
							Expression: pgsql.FunctionCall{
								Function: pgsql.FunctionGenerateSubscripts,
								Parameters: []pgsql.Expression{
									kindIDs,
									pgsql.NewLiteral(1, pgsql.Int),
								},
							},
							Alias: pgsql.AsOptionalIdentifier(kindIndexAlias),
						},
					},
					{
						Source: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableKind},
							Binding: pgsql.AsOptionalIdentifier(kindAlias),
						},
					},
				},
				Where: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{kindAlias, pgsql.ColumnID},
					pgsql.OperatorEquals,
					&pgsql.ArrayIndex{
						Expression: pgsql.NewParenthetical(kindIDs),
						Indexes:    []pgsql.Expression{kindIndexAlias},
					},
				),
			},
			OrderBy: []*pgsql.OrderBy{{
				Expression: kindIndexAlias,
				Ascending:  true,
			}},
		},
	}, pgsql.TextArray)
}

func (s *Translator) translateFunction(typedExpression *cypher.FunctionInvocation) {
	switch formattedName := strings.ToLower(typedExpression.Name); formattedName {
	case cypher.DurationFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if referenceArgument, typeOK := argument.(pgsql.Literal); !typeOK {
			s.SetErrorf("expected a string literal for the cypher function: %s but received %T", typedExpression.Name, argument)
		} else if _, typeOK := referenceArgument.Value.(string); !typeOK {
			s.SetErrorf("expected a string literal for the cypher function: %s but received %T", typedExpression.Name, argument)
		} else {
			// Rewrite the cast type of the literal to the PgSQL interval type. The equivalent PgSQL form does not
			// require a function call
			referenceArgument.CastType = pgsql.Interval
			s.treeTranslator.PushOperand(referenceArgument)
		}

	case cypher.IdentityFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if referenceArgument, typeOK := argument.(pgsql.Identifier); !typeOK {
			s.SetErrorf("expected an identifier for the cypher function: %s but received %T", typedExpression.Name, argument)
		} else {
			s.treeTranslator.PushOperand(pgsql.CompoundIdentifier{referenceArgument, pgsql.ColumnID})
		}

	case cypher.LocalTimeFunction:
		if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.TimeWithoutTimeZone); err != nil {
			s.SetError(err)
		}

	case cypher.LocalDateTimeFunction:
		if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.TimestampWithoutTimeZone); err != nil {
			s.SetError(err)
		}

	case cypher.DateFunction:
		if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.Date); err != nil {
			s.SetError(err)
		}

	case cypher.DateTimeFunction:
		if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.TimestampWithTimeZone); err != nil {
			s.SetError(err)
		}

	case cypher.EdgeTypeFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if identifier, isIdentifier := argument.(pgsql.Identifier); !isIdentifier {
			s.SetErrorf("expected an identifier for the cypher function: %s but received %T", typedExpression.Name, argument)
		} else {
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function: pgsql.FunctionKindName,
				Parameters: []pgsql.Expression{
					pgsql.CompoundIdentifier{identifier, pgsql.ColumnKindID},
				},
				CastType: pgsql.Text,
			})
		}

	case cypher.StartNodeFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function:   pgsql.FunctionStartNode,
				Parameters: []pgsql.Expression{argument},
				CastType:   pgsql.NodeComposite,
			})
		}

	case cypher.EndNodeFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function:   pgsql.FunctionEndNode,
				Parameters: []pgsql.Expression{argument},
				CastType:   pgsql.NodeComposite,
			})
		}

	case cypher.NodeLabelsFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if identifier, isIdentifier := argument.(pgsql.Identifier); !isIdentifier {
			s.SetErrorf("expected an identifier for the cypher function: %s but received %T", typedExpression.Name, argument)
		} else {
			s.treeTranslator.PushOperand(translateNodeLabelsExpression(identifier))
		}

	case cypher.CountFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function:   pgsql.FunctionCount,
				Parameters: []pgsql.Expression{argument},
				CastType:   pgsql.Int8,
				Distinct:   typedExpression.Distinct,
			})
		}

	case cypher.StringSplitToArrayFunction:
		if typedExpression.NumArguments() != 2 {
			s.SetError(fmt.Errorf("expected two arguments for cypher function %s", typedExpression.Name))
		} else if delimiter, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if splitReference, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			if _, hasHint := GetTypeHint(splitReference); !hasHint {
				// Do our best to coerce the type into text
				if typedSplitRef, err := TypeCastExpression(splitReference, pgsql.Text); err != nil {
					s.SetError(err)
				} else {
					s.treeTranslator.PushOperand(pgsql.FunctionCall{
						Function:   pgsql.FunctionStringToArray,
						Parameters: []pgsql.Expression{typedSplitRef, delimiter},
						CastType:   pgsql.TextArray,
					})
				}
			} else {
				s.treeTranslator.PushOperand(pgsql.FunctionCall{
					Function:   pgsql.FunctionStringToArray,
					Parameters: []pgsql.Expression{splitReference, delimiter},
					CastType:   pgsql.TextArray,
				})
			}
		}

	case cypher.ToLowerFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(argument); isPropertyLookup {
				// Rewrite the property lookup operator with a JSON text field lookup
				propertyLookup.Operator = pgsql.OperatorJSONTextField
			}

			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function:   pgsql.FunctionToLower,
				Parameters: []pgsql.Expression{argument},
				CastType:   pgsql.Text,
			})
		}

	case cypher.ListSizeFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			var functionCall pgsql.FunctionCall

			if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(argument); isPropertyLookup {
				// Ensure that the JSONB array length function receives the JSONB type
				propertyLookup.Operator = pgsql.OperatorJSONField

				functionCall = pgsql.FunctionCall{
					Function:   pgsql.FunctionJSONBArrayLength,
					Parameters: []pgsql.Expression{argument},
					CastType:   pgsql.Int,
				}
			} else if isKnownEmptyArrayExpression(argument) {
				s.treeTranslator.PushOperand(pgsql.NewLiteral(0, pgsql.Int))
				return
			} else {
				functionCall = pgsql.FunctionCall{
					Function:   pgsql.FunctionCardinality,
					Parameters: []pgsql.Expression{argument},
					CastType:   pgsql.Int,
				}
			}

			s.treeTranslator.PushOperand(functionCall)
		}

	case cypher.HeadFunction:
		if err := s.translateHeadFunction(typedExpression); err != nil {
			s.SetError(err)
		}

	case cypher.TailFunction:
		if err := s.translateTailFunction(typedExpression); err != nil {
			s.SetError(err)
		}

	case cypher.NodesFunction:
		if err := s.translatePathComponentFunction(typedExpression, pgsql.ColumnNodes, pgsql.NodeCompositeArray); err != nil {
			s.SetError(err)
		}

	case cypher.RelationshipsFunction:
		if err := s.translatePathComponentFunction(typedExpression, pgsql.ColumnEdges, pgsql.EdgeCompositeArray); err != nil {
			s.SetError(err)
		}

	case cypher.ToUpperFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(argument); isPropertyLookup {
				// Rewrite the property lookup operator with a JSON text field lookup
				propertyLookup.Operator = pgsql.OperatorJSONTextField
			}

			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function:   pgsql.FunctionToUpper,
				Parameters: []pgsql.Expression{argument},
				CastType:   pgsql.Text,
			})
		}

	case cypher.ToStringFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(pgsql.NewTypeCast(argument, pgsql.Text))
		}

	case cypher.ToIntegerFunction, legacyToIntegerFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(pgsql.NewTypeCast(argument, pgsql.Int8))
		}

	case cypher.CoalesceFunction:
		if err := s.translateCoalesceFunction(typedExpression); err != nil {
			s.SetError(err)
		}

	case cypher.CollectFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if collectedExpression, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if preparedExpression, castType, err := prepareCollectExpression(s.scope, collectedExpression, typedExpression.Name); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(
				functionWrapCollectToArray(typedExpression.Distinct, preparedExpression, castType),
			)
		}

	case cypher.SumFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if typeCastNumericArg, err := TypeCastExpression(argument, pgsql.Float8); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function:   pgsql.FunctionSum,
				Parameters: []pgsql.Expression{typeCastNumericArg},
				CastType:   pgsql.Numeric,
			})
		}

	case cypher.AvgFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if typeCastNumericArg, err := TypeCastExpression(argument, pgsql.Float8); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function:   pgsql.FunctionAvg,
				Parameters: []pgsql.Expression{typeCastNumericArg},
				CastType:   pgsql.Numeric,
			})
		}

	case cypher.MinFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(cypherMinMaxFunction(pgsql.FunctionMin, argument))
		}

	case cypher.MaxFunction:
		if typedExpression.NumArguments() != 1 {
			s.SetError(fmt.Errorf("expected only one argument for cypher function: %s", typedExpression.Name))
		} else if argument, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(cypherMinMaxFunction(pgsql.FunctionMax, argument))
		}

	default:
		s.SetErrorf("unknown cypher function: %s", typedExpression.Name)
	}
}

// functionWrapCollectToArray performs a few wrapper function calls for the collect semantic functionality presented
// in Neo4j. The outer wrapper in an array_remove(...) call to prune null values from the aggregated column. This
// prevents null from tainting comparisons that may later be performed against the aggregated column.
//
// The next wrapper is a coalesce(...) call to ensure that if the aggregation results in a null return value that the
// aggregation still returns an empty array. As per documentation in
// https://www.postgresql.org/docs/current/functions-aggregate.html for versions 16.9 and 17.4:
// > It should be noted that except for count, these functions return a null value when no rows are selected. In
// > particular, sum of no rows returns null, not zero as one might expect, and array_agg returns null rather than
// > an empty array when there are no input rows. The coalesce function can be used to substitute zero or an empty
// > array for null when necessary.
//
// The final function call is the aggregation function itself.
//
// Current semantic issues:
// * `null` values are stripped during aggregation. This may not be the case in Neo4j's query execution pipeline.
func functionWrapCollectToArray(distinct bool, collectedExpression pgsql.Expression, castType pgsql.DataType) pgsql.FunctionCall {
	var coalesceType = pgsql.TextArray
	if castType != pgsql.AnyArray {
		coalesceType = castType
	}

	return pgsql.FunctionCall{
		Function: pgsql.FunctionArrayRemove,
		Parameters: []pgsql.Expression{
			pgsql.FunctionCall{
				Function: pgsql.FunctionCoalesce,
				Parameters: []pgsql.Expression{
					pgsql.FunctionCall{
						Function:   pgsql.FunctionArrayAggregate,
						Parameters: []pgsql.Expression{collectedExpression},
						Distinct:   distinct,
						CastType:   castType,
					},
					pgsql.ArrayLiteral{
						CastType: coalesceType,
					},
				},
				CastType: castType,
			},
			pgsql.NullLiteral(),
		},
		CastType: castType,
	}
}

func (s *Translator) translateDateTimeFunctionCall(cypherFunc *cypher.FunctionInvocation, dataType pgsql.DataType) error {
	// Ensure the local date time function uses the default precision
	const defaultTimestampPrecision = 6

	var functionIdentifier pgsql.Identifier

	switch dataType {
	case pgsql.Date:
		functionIdentifier = pgsql.FunctionCurrentDate

	case pgsql.TimeWithoutTimeZone:
		functionIdentifier = pgsql.FunctionLocalTime

	case pgsql.TimeWithTimeZone:
		functionIdentifier = pgsql.FunctionCurrentTime

	case pgsql.TimestampWithoutTimeZone:
		functionIdentifier = pgsql.FunctionLocalTimestamp

	case pgsql.TimestampWithTimeZone:
		functionIdentifier = pgsql.FunctionNow

	default:
		return fmt.Errorf("unable to convert date function with data type: %s", dataType)
	}

	// Apply defaults for this function
	if !cypherFunc.HasArguments() {
		switch functionIdentifier {
		case pgsql.FunctionCurrentDate:
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function: functionIdentifier,
				Bare:     true,
				CastType: dataType,
			})

		case pgsql.FunctionNow:
			s.treeTranslator.PushOperand(pgsql.FunctionCall{
				Function: functionIdentifier,
				Bare:     false,
				CastType: dataType,
			})

		default:
			if precisionLiteral, err := pgsql.AsLiteral(defaultTimestampPrecision); err != nil {
				return err
			} else {
				s.treeTranslator.PushOperand(pgsql.FunctionCall{
					Function: functionIdentifier,
					Parameters: []pgsql.Expression{
						precisionLiteral,
					},
					CastType: dataType,
				})
			}
		}
	} else if cypherFunc.NumArguments() > 1 {
		return fmt.Errorf("expected only one text argument for cypher function: %s", cypherFunc.Name)
	} else if specArgument, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else {
		s.treeTranslator.PushOperand(pgsql.NewTypeCast(specArgument, dataType))
	}

	return nil
}

func (s *Translator) translateCoalesceFunction(functionInvocation *cypher.FunctionInvocation) error {
	if numArgs := functionInvocation.NumArguments(); numArgs == 0 {
		s.SetError(fmt.Errorf("expected at least one argument for cypher function: %s", functionInvocation.Name))
	} else {
		var (
			arguments    = make([]pgsql.Expression, numArgs)
			expectedType = pgsql.UnsetDataType
		)

		// This loop is used to pop off the coalesce function arguments in the intended order (since they're
		// pushed onto the translator stack).
		for idx := range functionInvocation.Arguments {
			if argument, err := s.treeTranslator.PopOperand(); err != nil {
				return err
			} else {
				arguments[numArgs-idx-1] = argument
			}
		}

		// Find and validate types of the arguments
		for _, argument := range arguments {
			// Properties have no type information and should be skipped
			if argumentType, err := InferExpressionType(argument); err != nil {
				return err
			} else if argumentType.IsKnown() {
				// If the expected type isn't known yet then assign the known inferred type to it
				if !expectedType.IsKnown() {
					expectedType = argumentType
				} else if expectedType != argumentType {
					// All other inferrable argument types must match the first inferred type encountered
					return fmt.Errorf("types in coalesce function must match %s but got %s", expectedType, argumentType)
				}
			}
		}

		if expectedType == pgsql.AnyArray {
			expectedType = pgsql.TextArray
		}

		if expectedType.IsKnown() {
			// Rewrite any property lookup operators now that we have some type information
			for idx, argument := range arguments {
				if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(argument); isPropertyLookup {
					arguments[idx] = rewritePropertyLookupOperator(propertyLookup, expectedType)
				} else if arrayLiteral, isArrayLiteral := argument.(pgsql.ArrayLiteral); isArrayLiteral && arrayLiteral.CastType == pgsql.AnyArray {
					arrayLiteral.CastType = expectedType
					arguments[idx] = arrayLiteral
				}
			}
		}

		// Translate the function call to the expected SQL form
		s.treeTranslator.PushOperand(pgsql.FunctionCall{
			Function:   pgsql.FunctionCoalesce,
			Parameters: arguments,
			CastType:   expectedType,
		})
	}

	return nil
}
