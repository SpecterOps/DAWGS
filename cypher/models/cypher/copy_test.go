package cypher_test

import (
	"testing"

	model2 "github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func validateCopy(t *testing.T, actual any) {
	require.Equal(t, actual, model2.Copy(actual))
}

func int64Pointer(value int64) *int64 {
	return &value
}

func TestCopy(t *testing.T) {
	validateCopy(t, &model2.RegularQuery{})
	validateCopy(t, &model2.SingleQuery{})
	validateCopy(t, &model2.SinglePartQuery{
		ReadingClauses: []*model2.ReadingClause{{
			Match: &model2.Match{
				Pattern: nil,
				Where:   nil,
			},
			Unwind: nil,
		}},
		UpdatingClauses: nil,
		Return:          nil,
	})

	validateCopy(t, &model2.MultiPartQuery{})
	validateCopy(t, &model2.MultiPartQuery{
		Parts: []*model2.MultiPartQueryPart{{
			ReadingClauses: []*model2.ReadingClause{{
				Match: &model2.Match{
					Optional: true,
					Pattern: []*model2.PatternPart{{
						Variable:                model2.NewVariableWithSymbol("p"),
						ShortestPathPattern:     true,
						AllShortestPathsPattern: true,
						PatternElements:         []*model2.PatternElement{},
					}},
				},
			}},
		}},
		SinglePartQuery: &model2.SinglePartQuery{
			ReadingClauses: []*model2.ReadingClause{{
				Match: &model2.Match{
					Pattern: nil,
					Where:   nil,
				},
				Unwind: nil,
			}},
			UpdatingClauses: nil,
			Return:          nil,
		},
	})

	validateCopy(t, &model2.IDInCollection{})
	validateCopy(t, &model2.FilterExpression{})
	validateCopy(t, &model2.Quantifier{})

	validateCopy(t, &model2.MultiPartQueryPart{})
	validateCopy(t, &model2.Remove{})
	validateCopy(t, &model2.ArithmeticExpression{})
	validateCopy(t, &model2.PartialArithmeticExpression{
		Operator: model2.OperatorAdd,
	})
	validateCopy(t, &model2.Parenthetical{})
	validateCopy(t, &model2.Comparison{})
	validateCopy(t, &model2.PartialComparison{
		Operator: model2.OperatorAdd,
	})
	validateCopy(t, &model2.SetItem{
		Operator: model2.OperatorAdditionAssignment,
	})
	validateCopy(t, &model2.Order{})
	validateCopy(t, &model2.Skip{})
	validateCopy(t, &model2.Limit{})
	validateCopy(t, &model2.RemoveItem{})
	validateCopy(t, &model2.Comparison{})
	validateCopy(t, &model2.FunctionInvocation{
		Distinct:  true,
		Namespace: []string{"a", "b", "c"},
		Name:      "d",
	})
	validateCopy(t, &model2.Variable{
		Symbol: "A",
	})
	validateCopy(t, &model2.Parameter{
		Symbol: "B",
	})
	validateCopy(t, &model2.Literal{
		Value: "1234",
		Null:  false,
	})
	validateCopy(t, &model2.Literal{
		Null: true,
	})
	validateCopy(t, &model2.Properties{
		Map: model2.MapLiteral{
			"name": model2.NewStringLiteral("value"),
		},
	})
	validateCopy(t, model2.MapLiteral{
		"name": model2.NewStringLiteral("value"),
	})
	validateCopy(t, &model2.ListLiteral{
		model2.NewLiteral(1, false),
		model2.NewLiteral(2, false),
	})
	validateCopy(t, &model2.MapItem{
		Key:   "name",
		Value: model2.NewStringLiteral("value"),
	})
	validateCopy(t, &model2.Projection{
		Distinct: true,
		All:      true,
	})
	validateCopy(t, &model2.Return{})
	validateCopy(t, &model2.ProjectionItem{})
	validateCopy(t, &model2.PropertyLookup{
		Symbol: "a",
	})
	validateCopy(t, &model2.Set{})
	validateCopy(t, &model2.Delete{
		Detach: true,
	})
	validateCopy(t, &model2.Create{
		Unique: true,
	})
	validateCopy(t, &model2.KindMatcher{})
	validateCopy(t, &model2.Conjunction{})
	validateCopy(t, &model2.Disjunction{})
	validateCopy(t, &model2.ExclusiveDisjunction{})
	validateCopy(t, &model2.PatternPart{
		Variable:                model2.NewVariableWithSymbol("p"),
		ShortestPathPattern:     true,
		AllShortestPathsPattern: true,
	})
	validateCopy(t, &model2.PatternElement{})
	validateCopy(t, &model2.Negation{})
	validateCopy(t, &model2.NodePattern{
		Variable: model2.NewVariableWithSymbol("n"),
	})
	validateCopy(t, &model2.RelationshipPattern{
		Variable:  model2.NewVariableWithSymbol("r"),
		Direction: graph.DirectionOutbound,
	})
	validateCopy(t, &model2.PatternRange{
		StartIndex: int64Pointer(1234),
	})
	validateCopy(t, &model2.UpdatingClause{})
	validateCopy(t, &model2.SortItem{
		Ascending: true,
	})
	validateCopy(t, []*model2.PatternPart{})
	validateCopy(t, &model2.PatternPredicate{
		PatternElements: []*model2.PatternElement{{}},
	})

	// External types
	validateCopy(t, []string{})
	validateCopy(t, graph.Kinds{})
}

func TestCopyPatternVariablesAreIndependent(t *testing.T) {
	original := &model2.PatternPart{
		Variable: model2.NewVariableWithSymbol("p"),
		PatternElements: []*model2.PatternElement{
			{
				Element: &model2.NodePattern{
					Variable: model2.NewVariableWithSymbol("n"),
				},
			},
			{
				Element: &model2.RelationshipPattern{
					Variable: model2.NewVariableWithSymbol("r"),
				},
			},
		},
	}

	copied := model2.Copy(original)
	copied.Variable.Symbol = "copied_path"
	copiedNode, _ := copied.PatternElements[0].AsNodePattern()
	copiedNode.Variable.Symbol = "copied_node"
	copiedRelationship, _ := copied.PatternElements[1].AsRelationshipPattern()
	copiedRelationship.Variable.Symbol = "copied_relationship"

	originalNode, _ := original.PatternElements[0].AsNodePattern()
	originalRelationship, _ := original.PatternElements[1].AsRelationshipPattern()

	require.Equal(t, "p", original.Variable.Symbol)
	require.Equal(t, "n", originalNode.Variable.Symbol)
	require.Equal(t, "r", originalRelationship.Variable.Symbol)
}

func TestNilPatternElementHelpers(t *testing.T) {
	var element *model2.PatternElement

	nodePattern, isNodePattern := element.AsNodePattern()
	require.Nil(t, nodePattern)
	require.False(t, isNodePattern)

	relationshipPattern, isRelationshipPattern := element.AsRelationshipPattern()
	require.Nil(t, relationshipPattern)
	require.False(t, isRelationshipPattern)
}
