package walk_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestWalk(t *testing.T) {
	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
	})

	// Walk through all positive test cases to ensure that the walker can visit the involved types
	for _, testCase := range test.LoadFixture(t, test.PositiveTestCases).RunnableCases() {
		if testCase.Type == test.TypeStringMatch {
			parseContext := frontend.NewContext()

			if details, err := test.UnmarshallTestCaseDetails[test.StringMatchTest](testCase); err != nil {
				t.Fatalf("Error unmarshalling test case details: %v", err)
			} else if queryModel, err := frontend.ParseCypher(parseContext, details.Query); err != nil {
				t.Fatalf("Parser errors: %s", err.Error())
			} else {
				require.Nil(t, walk.Cypher(queryModel, visitor))
			}
		}
	}
}

func TestCypherWalkConsumeLeafDoesNotSkipSibling(t *testing.T) {
	expression := cypher.NewDisjunction(
		cypher.NewVariableWithSymbol("first"),
		cypher.NewVariableWithSymbol("second"),
	)

	var visited []string
	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, handler walk.VisitorHandler) {
		variable, isVariable := node.(*cypher.Variable)
		if !isVariable {
			return
		}

		visited = append(visited, variable.Symbol)
		if variable.Symbol == "first" {
			handler.Consume()
		}
	})

	require.NoError(t, walk.Cypher(expression, visitor))
	require.Equal(t, []string{"first", "second"}, visited)
}

func TestCypherWalkVisitsExclusiveDisjunction(t *testing.T) {
	expression := cypher.NewExclusiveDisjunction(
		cypher.NewVariableWithSymbol("left"),
		cypher.NewVariableWithSymbol("right"),
	)

	var visited []string
	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if variable, isVariable := node.(*cypher.Variable); isVariable {
			visited = append(visited, variable.Symbol)
		}
	})

	require.NoError(t, walk.Cypher(expression, visitor))
	require.Equal(t, []string{"left", "right"}, visited)
}

func TestCypherWalkVisitsMapLiteralValuesInKeyOrder(t *testing.T) {
	mapLiteral := cypher.MapLiteral{
		"b": cypher.NewVariableWithSymbol("b_value"),
		"a": cypher.NewVariableWithSymbol("a_value"),
	}

	var (
		visitedKeys   []string
		visitedValues []string
	)

	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		switch typedNode := node.(type) {
		case *cypher.MapItem:
			visitedKeys = append(visitedKeys, typedNode.Key)

		case *cypher.Variable:
			visitedValues = append(visitedValues, typedNode.Symbol)
		}
	})

	require.NoError(t, walk.Cypher(mapLiteral, visitor))
	require.Equal(t, []string{"a", "b"}, visitedKeys)
	require.Equal(t, []string{"a_value", "b_value"}, visitedValues)
}

func TestCypherWalkSkipsNilBranches(t *testing.T) {
	testCases := map[string]cypher.SyntaxNode{
		"regular query":        &cypher.RegularQuery{},
		"single query":         &cypher.SingleQuery{},
		"multipart query":      &cypher.MultiPartQuery{},
		"return":               &cypher.Return{},
		"set item":             &cypher.SetItem{},
		"merge action":         &cypher.MergeAction{},
		"updating clause":      &cypher.UpdatingClause{},
		"projection item":      &cypher.ProjectionItem{},
		"pattern element":      &cypher.PatternElement{},
		"partial comparison":   &cypher.PartialComparison{},
		"partial arithmetic":   &cypher.PartialArithmeticExpression{},
		"unary add/subtract":   &cypher.UnaryAddOrSubtractExpression{},
		"relationship pattern": &cypher.RelationshipPattern{},
		"node pattern":         &cypher.NodePattern{},
	}

	for name, node := range testCases {
		t.Run(name, func(t *testing.T) {
			visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(cypher.SyntaxNode, walk.VisitorHandler) {})
			require.NoError(t, walk.Cypher(node, visitor))
		})
	}
}

func TestCypherWalkSemanticSkipsDeclarationOnlyFields(t *testing.T) {
	testCases := map[string]struct {
		node          cypher.SyntaxNode
		visited       []string
		notVisited    []string
		visitedRanges int
	}{
		"projection alias": {
			node: &cypher.ProjectionItem{
				Expression: cypher.NewVariableWithSymbol("value"),
				Alias:      cypher.NewVariableWithSymbol("alias"),
			},
			visited:    []string{"value"},
			notVisited: []string{"alias"},
		},
		"id in collection variable": {
			node: &cypher.IDInCollection{
				Variable:   cypher.NewVariableWithSymbol("item"),
				Expression: cypher.NewVariableWithSymbol("items"),
			},
			visited:    []string{"items"},
			notVisited: []string{"item"},
		},
		"pattern part variable": {
			node: &cypher.PatternPart{
				Variable: cypher.NewVariableWithSymbol("path"),
			},
			notVisited: []string{"path"},
		},
		"node pattern variable": {
			node: &cypher.NodePattern{
				Variable: cypher.NewVariableWithSymbol("node"),
			},
			notVisited: []string{"node"},
		},
		"relationship pattern metadata": {
			node: &cypher.RelationshipPattern{
				Variable: cypher.NewVariableWithSymbol("rel"),
				Range:    &cypher.PatternRange{},
			},
			notVisited:    []string{"rel"},
			visitedRanges: 0,
		},
		"remove kind matcher": {
			node: &cypher.RemoveItem{
				KindMatcher: &cypher.KindMatcher{
					Reference: cypher.NewVariableWithSymbol("node"),
					Kinds:     graph.Kinds{graph.StringKind("NodeKind")},
				},
			},
			notVisited: []string{"node"},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			var (
				visitedVariables []string
				visitedRanges    int
			)

			visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
				switch typedNode := node.(type) {
				case *cypher.Variable:
					visitedVariables = append(visitedVariables, typedNode.Symbol)

				case *cypher.PatternRange:
					visitedRanges++
				}
			})

			require.NoError(t, walk.Cypher(testCase.node, visitor))
			for _, symbol := range testCase.visited {
				require.Contains(t, visitedVariables, symbol)
			}
			for _, symbol := range testCase.notVisited {
				require.NotContains(t, visitedVariables, symbol)
			}
			if testCase.visitedRanges == 0 {
				require.Zero(t, visitedRanges)
			}
		})
	}
}

func TestCypherStructuralWalkVisitsDeclarationAndMetadataFields(t *testing.T) {
	testCases := map[string]struct {
		node       cypher.SyntaxNode
		variables  []string
		kinds      []string
		mapKeys    []string
		literals   []any
		operators  []cypher.Operator
		numRanges  int
		numMapNode int
	}{
		"remove kind matcher": {
			node: &cypher.RemoveItem{
				KindMatcher: &cypher.KindMatcher{
					Reference: cypher.NewVariableWithSymbol("node"),
					Kinds:     graph.Kinds{graph.StringKind("NodeKind")},
				},
			},
			variables: []string{"node"},
			kinds:     []string{"NodeKind"},
		},
		"node pattern": {
			node: &cypher.NodePattern{
				Variable: cypher.NewVariableWithSymbol("node"),
				Kinds:    graph.Kinds{graph.StringKind("User")},
				Properties: &cypher.Properties{
					Map: cypher.MapLiteral{
						"name": cypher.NewVariableWithSymbol("name"),
					},
				},
			},
			variables:  []string{"node", "name"},
			kinds:      []string{"User"},
			mapKeys:    []string{"name"},
			numMapNode: 1,
		},
		"relationship pattern": {
			node: &cypher.RelationshipPattern{
				Variable: cypher.NewVariableWithSymbol("rel"),
				Kinds:    graph.Kinds{graph.StringKind("MemberOf")},
				Range:    cypher.NewPatternRange(nil, nil),
				Properties: &cypher.Properties{
					Map: cypher.MapLiteral{
						"weight": cypher.NewVariableWithSymbol("weight"),
					},
				},
			},
			variables:  []string{"rel", "weight"},
			kinds:      []string{"MemberOf"},
			mapKeys:    []string{"weight"},
			numRanges:  1,
			numMapNode: 1,
		},
		"projection alias": {
			node: &cypher.ProjectionItem{
				Expression: cypher.NewVariableWithSymbol("value"),
				Alias:      cypher.NewVariableWithSymbol("alias"),
			},
			variables: []string{"value", "alias"},
		},
		"id in collection": {
			node: &cypher.IDInCollection{
				Variable:   cypher.NewVariableWithSymbol("item"),
				Expression: cypher.NewVariableWithSymbol("items"),
			},
			variables: []string{"item", "items"},
		},
		"pattern part variable": {
			node: &cypher.PatternPart{
				Variable: cypher.NewVariableWithSymbol("path"),
				PatternElements: []*cypher.PatternElement{{
					Element: &cypher.NodePattern{
						Variable: cypher.NewVariableWithSymbol("node"),
					},
				}},
			},
			variables: []string{"path", "node"},
		},
		"skip limit and operators": {
			node: &cypher.Projection{
				Skip: &cypher.Skip{
					Value: cypher.NewLiteral(5, false),
				},
				Limit: &cypher.Limit{
					Value: cypher.NewLiteral(10, false),
				},
				Items: []cypher.Expression{
					&cypher.ProjectionItem{
						Expression: &cypher.Comparison{
							Left: cypher.NewVariableWithSymbol("n"),
							Partials: []*cypher.PartialComparison{{
								Operator: cypher.OperatorEquals,
								Right:    cypher.NewLiteral(1, false),
							}},
						},
					},
				},
			},
			variables: []string{"n"},
			literals:  []any{1, 5, 10},
			operators: []cypher.Operator{
				cypher.OperatorEquals,
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			var (
				visitedVariables []string
				visitedKinds     []string
				visitedMapKeys   []string
				visitedLiterals  []any
				visitedOperators []cypher.Operator
				visitedRanges    int
				visitedMapNodes  int
			)

			visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
				switch typedNode := node.(type) {
				case *cypher.Variable:
					visitedVariables = append(visitedVariables, typedNode.Symbol)

				case graph.Kinds:
					for _, kind := range typedNode {
						visitedKinds = append(visitedKinds, kind.String())
					}

				case *cypher.MapItem:
					visitedMapKeys = append(visitedMapKeys, typedNode.Key)

				case cypher.MapLiteral:
					visitedMapNodes++

				case *cypher.Literal:
					visitedLiterals = append(visitedLiterals, typedNode.Value)

				case cypher.Operator:
					visitedOperators = append(visitedOperators, typedNode)

				case *cypher.PatternRange:
					visitedRanges++
				}
			})

			require.NoError(t, walk.CypherStructural(testCase.node, visitor))
			for _, symbol := range testCase.variables {
				require.Contains(t, visitedVariables, symbol)
			}
			for _, kind := range testCase.kinds {
				require.Contains(t, visitedKinds, kind)
			}
			for _, key := range testCase.mapKeys {
				require.Contains(t, visitedMapKeys, key)
			}
			for _, literal := range testCase.literals {
				require.Contains(t, visitedLiterals, literal)
			}
			for _, operator := range testCase.operators {
				require.Contains(t, visitedOperators, operator)
			}
			require.Equal(t, testCase.numRanges, visitedRanges)
			require.Equal(t, testCase.numMapNode, visitedMapNodes)
		})
	}
}

func TestCypherWalkSupportsKnownSyntaxNodeTypes(t *testing.T) {
	testCases := map[string]cypher.SyntaxNode{
		"arithmetic expression":         &cypher.ArithmeticExpression{},
		"comparison":                    &cypher.Comparison{},
		"conjunction":                   cypher.NewConjunction(),
		"create":                        &cypher.Create{},
		"delete":                        &cypher.Delete{},
		"disjunction":                   cypher.NewDisjunction(),
		"exclusive disjunction":         cypher.NewExclusiveDisjunction(),
		"filter expression":             &cypher.FilterExpression{},
		"function invocation":           &cypher.FunctionInvocation{},
		"graph kinds":                   graph.Kinds{graph.StringKind("NodeKind")},
		"id in collection":              &cypher.IDInCollection{},
		"kind matcher":                  &cypher.KindMatcher{},
		"limit":                         &cypher.Limit{},
		"list literal":                  cypher.NewListLiteral(),
		"literal":                       cypher.NewLiteral(1, false),
		"map item":                      &cypher.MapItem{},
		"map literal":                   cypher.MapLiteral{"value": cypher.NewLiteral(1, false)},
		"match":                         &cypher.Match{},
		"merge":                         &cypher.Merge{},
		"merge action":                  &cypher.MergeAction{},
		"multipart query":               &cypher.MultiPartQuery{},
		"multipart query part":          &cypher.MultiPartQueryPart{},
		"negation":                      &cypher.Negation{},
		"node pattern":                  &cypher.NodePattern{},
		"operator":                      cypher.Operator("="),
		"order":                         &cypher.Order{},
		"parameter":                     &cypher.Parameter{},
		"parenthetical":                 &cypher.Parenthetical{},
		"partial arithmetic":            &cypher.PartialArithmeticExpression{},
		"partial comparison":            &cypher.PartialComparison{},
		"pattern element":               &cypher.PatternElement{},
		"pattern part":                  &cypher.PatternPart{},
		"pattern predicate":             &cypher.PatternPredicate{},
		"pattern range":                 &cypher.PatternRange{},
		"projection":                    &cypher.Projection{},
		"projection item":               &cypher.ProjectionItem{},
		"properties map":                &cypher.Properties{Map: cypher.MapLiteral{"value": cypher.NewLiteral(1, false)}},
		"properties parameter":          &cypher.Properties{Parameter: cypher.NewParameter("props", map[string]any{})},
		"quantifier":                    &cypher.Quantifier{},
		"range quantifier":              &cypher.RangeQuantifier{},
		"reading clause":                &cypher.ReadingClause{},
		"regular query":                 &cypher.RegularQuery{},
		"relationship pattern":          &cypher.RelationshipPattern{},
		"remove":                        &cypher.Remove{},
		"remove item":                   &cypher.RemoveItem{},
		"return":                        &cypher.Return{},
		"set":                           &cypher.Set{},
		"set item":                      &cypher.SetItem{},
		"single part query":             &cypher.SinglePartQuery{},
		"single query":                  &cypher.SingleQuery{},
		"skip":                          &cypher.Skip{},
		"sort item":                     &cypher.SortItem{},
		"unary add/subtract expression": &cypher.UnaryAddOrSubtractExpression{},
		"unwind":                        &cypher.Unwind{},
		"updating clause":               &cypher.UpdatingClause{},
		"variable":                      &cypher.Variable{},
		"where":                         &cypher.Where{},
		"with":                          &cypher.With{},
	}

	for name, node := range testCases {
		t.Run(name, func(t *testing.T) {
			visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(cypher.SyntaxNode, walk.VisitorHandler) {})
			require.NoError(t, walk.Cypher(node, visitor))
		})
	}
}

func TestPgSQLWalkVisitsJoinTable(t *testing.T) {
	query := pgsql.Query{
		Body: pgsql.Select{
			Projection: []pgsql.SelectItem{
				pgsql.CompoundIdentifier{"outer_table", "id"},
			},
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{"outer_table"},
					Binding: pgsql.AsOptionalIdentifier("outer_table"),
				},
				Joins: []pgsql.Join{{
					Table: pgsql.LateralSubquery{
						Query: pgsql.Query{
							Body: pgsql.Select{
								Projection: []pgsql.SelectItem{
									pgsql.CompoundIdentifier{"inner_table", "id"},
								},
								From: []pgsql.FromClause{{
									Source: pgsql.TableReference{
										Name:    pgsql.CompoundIdentifier{"inner_table"},
										Binding: pgsql.AsOptionalIdentifier("inner_table"),
									},
								}},
							},
						},
						Binding: pgsql.AsOptionalIdentifier("inner_table"),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
					},
				}},
			}},
		},
	}

	var (
		visitedLateralSubquery bool
		visitedInnerProjection bool
		visitedJoinConstraint  bool
	)

	visitor := walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, _ walk.VisitorHandler) {
		switch typedNode := node.(type) {
		case pgsql.LateralSubquery:
			visitedLateralSubquery = true

		case pgsql.CompoundIdentifier:
			if typedNode.String() == "inner_table.id" {
				visitedInnerProjection = true
			}

		case pgsql.Literal:
			if typedNode.Value == true {
				visitedJoinConstraint = true
			}
		}
	})

	require.NoError(t, walk.PgSQL(query, visitor))
	require.True(t, visitedLateralSubquery)
	require.True(t, visitedInnerProjection)
	require.True(t, visitedJoinConstraint)
}
