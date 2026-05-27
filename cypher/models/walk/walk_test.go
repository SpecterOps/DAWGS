package walk_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"

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
