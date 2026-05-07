package v2

import (
	"fmt"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

func Variable(name string) *cypher.Variable {
	return cypher.NewVariableWithSymbol(name)
}

func Identity(reference any) *cypher.FunctionInvocation {
	return cypher.NewSimpleFunctionInvocation(cypher.IdentityFunction, expressionOrError(reference))
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
	return cypher.NewSimpleFunctionInvocation(cypher.CountFunction, expressionOrError(reference))
}

func CountDistinct(reference any) *cypher.FunctionInvocation {
	return &cypher.FunctionInvocation{
		Name:      cypher.CountFunction,
		Distinct:  true,
		Arguments: []cypher.Expression{expressionOrError(reference)},
	}
}

func Size(expression any) *cypher.FunctionInvocation {
	return cypher.NewSimpleFunctionInvocation(cypher.ListSizeFunction, expressionOrError(expression))
}

func KindsOf(reference any) *cypher.FunctionInvocation {
	if scopedReference, typeOK := reference.(scopedExpression); typeOK {
		if variable, typeOK := scopedReference.qualifier().(*cypher.Variable); !typeOK {
			return invalidExpression(fmt.Errorf("expected variable reference, got %T", scopedReference.qualifier()))
		} else if expression, err := kindProjectionExpression(scopedReference.roleName(), variable); err != nil {
			return invalidExpression(err)
		} else if invocation, typeOK := expression.(*cypher.FunctionInvocation); !typeOK {
			return invalidExpression(fmt.Errorf("expected kind projection function, got %T", expression))
		} else {
			return invocation
		}
	}

	expression := expressionOrError(reference)

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
	return &cypher.KindMatcher{
		Reference: expressionOrError(reference),
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
	return cypher.NewSetItem(expressionOrError(reference), cypher.OperatorLabelAssignment, kinds)
}

func DeleteKind(reference any, kind graph.Kind) *cypher.RemoveItem {
	return DeleteKinds(reference, graph.Kinds{kind})
}

func DeleteKinds(reference any, kinds graph.Kinds) *cypher.RemoveItem {
	return cypher.RemoveKindsByMatcher(cypher.NewKindMatcher(expressionOrError(reference), kinds, false))
}

func SetProperty(reference any, value any) *cypher.SetItem {
	return cypher.NewSetItem(expressionOrError(reference), cypher.OperatorAssignment, valueExpression(value))
}

func SetProperties(reference any, properties map[string]any) *cypher.Set {
	set := &cypher.Set{}

	for key, value := range properties {
		set.Items = append(set.Items, cypher.NewSetItem(
			propertyLookupOrError(reference, key),
			cypher.OperatorAssignment,
			valueExpression(value),
		))
	}

	return set
}

func DeleteProperty(reference any) *cypher.RemoveItem {
	return cypher.RemoveProperty(expressionOrError(reference))
}

func DeleteProperties(reference any, propertyNames ...string) *cypher.Remove {
	remove := &cypher.Remove{}

	for _, propertyName := range propertyNames {
		remove.Items = append(remove.Items, cypher.RemoveProperty(propertyLookupOrError(reference, propertyName)))
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
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorEquals, valueExpression(value))
}

func GreaterThan(reference any, value any) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorGreaterThan, valueExpression(value))
}

func After(reference any, value any) cypher.Expression {
	return GreaterThan(reference, value)
}

func GreaterThanOrEqualTo(reference any, value any) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorGreaterThanOrEqualTo, valueExpression(value))
}

func GreaterThanOrEquals(reference any, value any) cypher.Expression {
	return GreaterThanOrEqualTo(reference, value)
}

func LessThan(reference any, value any) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorLessThan, valueExpression(value))
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
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorLessThanOrEqualTo, valueExpression(value))
}

func LessThanOrEquals(reference any, value any) cypher.Expression {
	return LessThanOrEqualTo(reference, value)
}

func In(reference any, value any) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorIn, valueExpression(value))
}

func InInverted(reference any, value any) cypher.Expression {
	return cypher.NewComparison(valueExpression(value), cypher.OperatorIn, expressionOrError(reference))
}

func InIDs(reference any, ids ...graph.ID) cypher.Expression {
	expression := expressionOrError(reference)

	if variable, typeOK := expression.(*cypher.Variable); typeOK {
		expression = Identity(variable)
	}

	return cypher.NewComparison(expression, cypher.OperatorIn, Parameter(ids))
}

func StringContains(reference any, value string) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorContains, Parameter(value))
}

func StringStartsWith(reference any, value string) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorStartsWith, Parameter(value))
}

func StringEndsWith(reference any, value string) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorEndsWith, Parameter(value))
}

func CaseInsensitiveStringContains(reference any, value string) cypher.Expression {
	return cypher.NewComparison(
		cypher.NewSimpleFunctionInvocation("toLower", expressionOrError(reference)),
		cypher.OperatorContains,
		Parameter(strings.ToLower(value)),
	)
}

func CaseInsensitiveStringStartsWith(reference any, value string) cypher.Expression {
	return cypher.NewComparison(
		cypher.NewSimpleFunctionInvocation("toLower", expressionOrError(reference)),
		cypher.OperatorStartsWith,
		Parameter(strings.ToLower(value)),
	)
}

func CaseInsensitiveStringEndsWith(reference any, value string) cypher.Expression {
	return cypher.NewComparison(
		cypher.NewSimpleFunctionInvocation("toLower", expressionOrError(reference)),
		cypher.OperatorEndsWith,
		Parameter(strings.ToLower(value)),
	)
}

func Exists(reference any) cypher.Expression {
	return IsNotNull(reference)
}

func IsNull(reference any) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorIs, Literal(nil))
}

func IsNotNull(reference any) cypher.Expression {
	return cypher.NewComparison(expressionOrError(reference), cypher.OperatorIsNot, Literal(nil))
}

func HasRelationships(reference any) *cypher.PatternPredicate {
	patternPredicate := cypher.NewPatternPredicate()

	if variable, err := variableReference(reference); err != nil {
		patternPredicate.AddElement(&cypher.NodePattern{
			Properties: invalidExpression(err),
		})
	} else {
		patternPredicate.AddElement(&cypher.NodePattern{
			Variable: cypher.NewVariableWithSymbol(variable.Symbol),
		})
	}

	patternPredicate.AddElement(&cypher.RelationshipPattern{
		Direction: graph.DirectionBoth,
	})

	patternPredicate.AddElement(&cypher.NodePattern{})

	return patternPredicate
}
