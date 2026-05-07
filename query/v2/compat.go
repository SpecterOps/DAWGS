package v2

import (
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

func Variable(name string) *cypher.Variable {
	return cypher.NewVariableWithSymbol(name)
}

func Identity(reference any) *cypher.FunctionInvocation {
	expression, _ := projectionExpression(reference)

	return cypher.NewSimpleFunctionInvocation(cypher.IdentityFunction, expression)
}

func NodeID() *cypher.FunctionInvocation {
	return Identity(Identifiers.Node())
}

func RelationshipID() *cypher.FunctionInvocation {
	return Identity(Identifiers.Relationship())
}

func StartID() *cypher.FunctionInvocation {
	return Identity(Identifiers.Start())
}

func EndID() *cypher.FunctionInvocation {
	return Identity(Identifiers.End())
}

func Count(reference any) *cypher.FunctionInvocation {
	expression, _ := projectionExpression(reference)
	return cypher.NewSimpleFunctionInvocation(cypher.CountFunction, expression)
}

func CountDistinct(reference any) *cypher.FunctionInvocation {
	expression, _ := projectionExpression(reference)

	return &cypher.FunctionInvocation{
		Name:      cypher.CountFunction,
		Distinct:  true,
		Arguments: []cypher.Expression{expression},
	}
}

func Size(expression any) *cypher.FunctionInvocation {
	expr, _ := projectionExpression(expression)
	return cypher.NewSimpleFunctionInvocation(cypher.ListSizeFunction, expr)
}

func KindsOf(reference any) *cypher.FunctionInvocation {
	expression, _ := projectionExpression(reference)

	switch typedExpression := expression.(type) {
	case *cypher.Variable:
		switch typedExpression.Symbol {
		case Identifiers.node, Identifiers.start, Identifiers.end:
			return cypher.NewSimpleFunctionInvocation(cypher.NodeLabelsFunction, typedExpression)

		case Identifiers.relationship:
			return cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, typedExpression)
		}
	}

	return cypher.NewSimpleFunctionInvocation(cypher.NodeLabelsFunction, expression)
}

func Kind(reference any, kinds ...graph.Kind) *cypher.KindMatcher {
	expression, _ := projectionExpression(reference)

	return &cypher.KindMatcher{
		Reference: expression,
		Kinds:     kinds,
	}
}

func KindIn(reference any, kinds ...graph.Kind) *cypher.KindMatcher {
	return Kind(reference, kinds...)
}

func AddKind(reference any, kind graph.Kind) *cypher.SetItem {
	return AddKinds(reference, graph.Kinds{kind})
}

func AddKinds(reference any, kinds graph.Kinds) *cypher.SetItem {
	expression, _ := projectionExpression(reference)
	return cypher.NewSetItem(expression, cypher.OperatorLabelAssignment, kinds)
}

func DeleteKind(reference any, kind graph.Kind) *cypher.RemoveItem {
	return DeleteKinds(reference, graph.Kinds{kind})
}

func DeleteKinds(reference any, kinds graph.Kinds) *cypher.RemoveItem {
	expression, _ := projectionExpression(reference)
	return cypher.RemoveKindsByMatcher(cypher.NewKindMatcher(expression, kinds, false))
}

func SetProperty(reference any, value any) *cypher.SetItem {
	expression, _ := projectionExpression(reference)
	return cypher.NewSetItem(expression, cypher.OperatorAssignment, valueExpression(value))
}

func SetProperties(reference any, properties map[string]any) *cypher.Set {
	set := &cypher.Set{}
	expression, _ := projectionExpression(reference)
	variable, _ := expression.(*cypher.Variable)

	for key, value := range properties {
		set.Items = append(set.Items, cypher.NewSetItem(
			cypher.NewPropertyLookup(variable.Symbol, key),
			cypher.OperatorAssignment,
			valueExpression(value),
		))
	}

	return set
}

func DeleteProperty(reference any) *cypher.RemoveItem {
	expression, _ := projectionExpression(reference)
	return cypher.RemoveProperty(expression)
}

func DeleteProperties(reference any, propertyNames ...string) *cypher.Remove {
	remove := &cypher.Remove{}
	expression, _ := projectionExpression(reference)
	variable, _ := expression.(*cypher.Variable)

	for _, propertyName := range propertyNames {
		remove.Items = append(remove.Items, cypher.RemoveProperty(cypher.NewPropertyLookup(variable.Symbol, propertyName)))
	}

	return remove
}

func NodePattern(kinds graph.Kinds, properties cypher.Expression) *cypher.NodePattern {
	return &cypher.NodePattern{
		Variable:   Identifiers.Node(),
		Kinds:      kinds,
		Properties: properties,
	}
}

func StartNodePattern(kinds graph.Kinds, properties cypher.Expression) *cypher.NodePattern {
	return &cypher.NodePattern{
		Variable:   Identifiers.Start(),
		Kinds:      kinds,
		Properties: properties,
	}
}

func EndNodePattern(kinds graph.Kinds, properties cypher.Expression) *cypher.NodePattern {
	return &cypher.NodePattern{
		Variable:   Identifiers.End(),
		Kinds:      kinds,
		Properties: properties,
	}
}

func RelationshipPattern(kind graph.Kind, properties cypher.Expression, direction graph.Direction) *cypher.RelationshipPattern {
	return &cypher.RelationshipPattern{
		Variable:   Identifiers.Relationship(),
		Kinds:      graph.Kinds{kind},
		Direction:  direction,
		Properties: properties,
	}
}

func Equals(reference any, value any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorEquals, valueExpression(value))
}

func GreaterThan(reference any, value any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorGreaterThan, valueExpression(value))
}

func After(reference any, value any) cypher.Expression {
	return GreaterThan(reference, value)
}

func GreaterThanOrEqualTo(reference any, value any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorGreaterThanOrEqualTo, valueExpression(value))
}

func GreaterThanOrEquals(reference any, value any) cypher.Expression {
	return GreaterThanOrEqualTo(reference, value)
}

func LessThan(reference any, value any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorLessThan, valueExpression(value))
}

func LessThanGraphQuery(reference any, other any) cypher.Expression {
	return LessThan(reference, other)
}

func Before(reference any, value time.Time) cypher.Expression {
	return LessThan(reference, value)
}

func BeforeGraphQuery(reference any, other any) cypher.Expression {
	return LessThan(reference, other)
}

func LessThanOrEqualTo(reference any, value any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorLessThanOrEqualTo, valueExpression(value))
}

func LessThanOrEquals(reference any, value any) cypher.Expression {
	return LessThanOrEqualTo(reference, value)
}

func In(reference any, value any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorIn, valueExpression(value))
}

func InInverted(reference any, value any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(valueExpression(value), cypher.OperatorIn, expression)
}

func InIDs(reference any, ids ...graph.ID) cypher.Expression {
	expression, _ := projectionExpression(reference)

	if variable, typeOK := expression.(*cypher.Variable); typeOK {
		expression = Identity(variable)
	}

	return cypher.NewComparison(expression, cypher.OperatorIn, Parameter(ids))
}

func StringContains(reference any, value string) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorContains, Parameter(value))
}

func StringStartsWith(reference any, value string) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorStartsWith, Parameter(value))
}

func StringEndsWith(reference any, value string) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorEndsWith, Parameter(value))
}

func CaseInsensitiveStringContains(reference any, value string) cypher.Expression {
	expression, _ := projectionExpression(reference)

	return cypher.NewComparison(
		cypher.NewSimpleFunctionInvocation("toLower", expression),
		cypher.OperatorContains,
		Parameter(strings.ToLower(value)),
	)
}

func CaseInsensitiveStringStartsWith(reference any, value string) cypher.Expression {
	expression, _ := projectionExpression(reference)

	return cypher.NewComparison(
		cypher.NewSimpleFunctionInvocation("toLower", expression),
		cypher.OperatorStartsWith,
		Parameter(strings.ToLower(value)),
	)
}

func CaseInsensitiveStringEndsWith(reference any, value string) cypher.Expression {
	expression, _ := projectionExpression(reference)

	return cypher.NewComparison(
		cypher.NewSimpleFunctionInvocation("toLower", expression),
		cypher.OperatorEndsWith,
		Parameter(strings.ToLower(value)),
	)
}

func Exists(reference any) cypher.Expression {
	return IsNotNull(reference)
}

func IsNull(reference any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorIs, Literal(nil))
}

func IsNotNull(reference any) cypher.Expression {
	expression, _ := projectionExpression(reference)
	return cypher.NewComparison(expression, cypher.OperatorIsNot, Literal(nil))
}

func HasRelationships(reference any) *cypher.PatternPredicate {
	expression, _ := projectionExpression(reference)
	variable, _ := expression.(*cypher.Variable)

	patternPredicate := cypher.NewPatternPredicate()
	patternPredicate.AddElement(&cypher.NodePattern{
		Variable: cypher.NewVariableWithSymbol(variable.Symbol),
	})

	patternPredicate.AddElement(&cypher.RelationshipPattern{
		Direction: graph.DirectionBoth,
	})

	patternPredicate.AddElement(&cypher.NodePattern{})

	return patternPredicate
}
