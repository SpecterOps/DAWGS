package format

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/graph"
)

const strippedLiteral = "$STRIPPED"

func writeJoinedKinds(output io.Writer, delimiter string, kinds graph.Kinds) error {
	for idx, kind := range kinds {
		if idx > 0 {
			if _, err := io.WriteString(output, delimiter); err != nil {
				return err
			}
		}

		if _, err := io.WriteString(output, kind.String()); err != nil {
			return err
		}
	}

	return nil
}

type Emitter struct {
	StripLiterals bool
}

func NewCypherEmitter(stripLiterals bool) Emitter {
	return Emitter{
		StripLiterals: stripLiterals,
	}
}

func (s Emitter) formatNodePattern(output io.Writer, nodePattern *cypher.NodePattern) error {
	if _, err := io.WriteString(output, "("); err != nil {
		return err
	}

	if nodePattern.Variable != nil {
		if err := s.WriteExpression(output, nodePattern.Variable); err != nil {
			return err
		}
	}

	if len(nodePattern.Kinds) > 0 {
		if _, err := io.WriteString(output, ":"); err != nil {
			return err
		}

		if err := writeJoinedKinds(output, ":", nodePattern.Kinds); err != nil {
			return err
		}
	}

	if nodePattern.Properties != nil {
		if _, err := io.WriteString(output, " "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, nodePattern.Properties); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(output, ")"); err != nil {
		return err
	}

	return nil
}

func (s Emitter) formatRelationshipPattern(output io.Writer, relationshipPattern *cypher.RelationshipPattern) error {
	switch relationshipPattern.Direction {
	case graph.DirectionOutbound:
		if _, err := io.WriteString(output, "-["); err != nil {
			return err
		}

	case graph.DirectionBoth:
		fallthrough

	case graph.DirectionInbound:
		if _, err := io.WriteString(output, "<-["); err != nil {
			return err
		}
	}

	if relationshipPattern.Variable != nil {
		if err := s.WriteExpression(output, relationshipPattern.Variable); err != nil {
			return err
		}
	}

	if len(relationshipPattern.Kinds) > 0 {
		if _, err := io.WriteString(output, ":"); err != nil {
			return err
		}

		if err := writeJoinedKinds(output, "|", relationshipPattern.Kinds); err != nil {
			return err
		}
	}

	if relationshipPattern.Range != nil {
		if _, err := io.WriteString(output, "*"); err != nil {
			return err
		}

		outputEllipsis := relationshipPattern.Range.StartIndex != nil || relationshipPattern.Range.EndIndex != nil

		if relationshipPattern.Range.StartIndex != nil {
			if _, err := io.WriteString(output, strconv.FormatInt(*relationshipPattern.Range.StartIndex, 10)); err != nil {
				return err
			}
		}

		if outputEllipsis {
			if _, err := io.WriteString(output, ".."); err != nil {
				return err
			}
		}

		if relationshipPattern.Range.EndIndex != nil {
			if _, err := io.WriteString(output, strconv.FormatInt(*relationshipPattern.Range.EndIndex, 10)); err != nil {
				return err
			}
		}
	}

	if relationshipPattern.Properties != nil {
		if _, err := io.WriteString(output, " "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, relationshipPattern.Properties); err != nil {
			return err
		}
	}

	switch relationshipPattern.Direction {
	case graph.DirectionInbound:
		if _, err := io.WriteString(output, "]-"); err != nil {
			return err
		}

	case graph.DirectionBoth:
		fallthrough

	case graph.DirectionOutbound:
		if _, err := io.WriteString(output, "]->"); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatPatternElements(output io.Writer, patternElements []*cypher.PatternElement) error {
	for idx, patternElement := range patternElements {
		if nodePattern, isNodePattern := patternElement.AsNodePattern(); isNodePattern {
			// If this is another node pattern then output a delimiter
			if idx >= 1 && patternElements[idx-1].IsNodePattern() {
				if _, err := io.WriteString(output, ", "); err != nil {
					return err
				}
			}

			if err := s.formatNodePattern(output, nodePattern); err != nil {
				return err
			}
		} else if relationshipPattern, isRelationshipPattern := patternElement.AsRelationshipPattern(); isRelationshipPattern {
			if err := s.formatRelationshipPattern(output, relationshipPattern); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("invalid pattern element: %T(%+v)", patternElement, patternElement)
		}
	}

	return nil
}

func (s Emitter) formatPatternPart(output io.Writer, patternPart *cypher.PatternPart) error {
	if patternPart.Variable != nil {
		if err := s.WriteExpression(output, patternPart.Variable); err != nil {
			return err
		}

		if _, err := io.WriteString(output, " = "); err != nil {
			return err
		}
	}

	if patternPart.ShortestPathPattern {
		if _, err := io.WriteString(output, "shortestPath("); err != nil {
			return err
		}
	}

	if patternPart.AllShortestPathsPattern {
		if _, err := io.WriteString(output, "allShortestPaths("); err != nil {
			return err
		}
	}

	if err := s.formatPatternElements(output, patternPart.PatternElements); err != nil {
		return err
	}

	if patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern {
		if _, err := io.WriteString(output, ")"); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatProjection(output io.Writer, projection *cypher.Projection) error {
	if projection.Distinct {
		if _, err := io.WriteString(output, "distinct "); err != nil {
			return err
		}
	}

	for idx, projectionItem := range projection.Items {
		if idx > 0 {
			if _, err := io.WriteString(output, ", "); err != nil {
				return err
			}
		}

		if err := s.WriteExpression(output, projectionItem); err != nil {
			return err
		}
	}

	if projection.Order != nil {
		if _, err := io.WriteString(output, " order by "); err != nil {
			return err
		}

		for idx := 0; idx < len(projection.Order.Items); idx++ {
			if idx > 0 {
				if _, err := io.WriteString(output, ", "); err != nil {
					return err
				}
			}

			nextItem := projection.Order.Items[idx]

			if err := s.WriteExpression(output, nextItem.Expression); err != nil {
				return err
			}

			if nextItem.Ascending {
				if _, err := io.WriteString(output, " asc"); err != nil {
					return err
				}
			} else if _, err := io.WriteString(output, " desc"); err != nil {
				return err
			}
		}
	}

	if projection.Skip != nil {
		if _, err := io.WriteString(output, " skip "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, projection.Skip); err != nil {
			return err
		}
	}

	if projection.Limit != nil {
		if _, err := io.WriteString(output, " limit "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, projection.Limit); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatReturn(output io.Writer, returnClause *cypher.Return) error {
	if _, err := io.WriteString(output, " return "); err != nil {
		return err
	}

	if returnClause.Projection != nil {
		return s.formatProjection(output, returnClause.Projection)
	}

	return nil
}

func (s Emitter) formatWhere(output io.Writer, whereClause *cypher.Where) error {
	if len(whereClause.Expressions) > 0 {
		if _, err := io.WriteString(output, " where "); err != nil {
			return err
		}
	}

	for _, expression := range whereClause.Expressions {
		if err := s.WriteExpression(output, expression); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatMapLiteral(output io.Writer, mapLiteral cypher.MapLiteral) error {
	if _, err := io.WriteString(output, "{"); err != nil {
		return err
	}

	first := true
	for key, subExpression := range mapLiteral {
		if !first {
			if _, err := io.WriteString(output, ", "); err != nil {
				return err
			}
		} else {
			first = false
		}

		if _, err := io.WriteString(output, key); err != nil {
			return err
		}

		if _, err := io.WriteString(output, ": "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, subExpression); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(output, "}"); err != nil {
		return err
	}

	return nil
}

func (s Emitter) formatLiteral(output io.Writer, literal *cypher.Literal) error {
	const literalNullToken = "null"

	// Check for a null literal first
	if literal.Null {
		if _, err := io.WriteString(output, literalNullToken); err != nil {
			return err
		}
		return nil
	}

	// Attempt to string format the literal value
	switch typedLiteral := literal.Value.(type) {
	case string:
		if _, err := io.WriteString(output, typedLiteral); err != nil {
			return err
		}

	case int8:
		if _, err := io.WriteString(output, strconv.FormatInt(int64(typedLiteral), 10)); err != nil {
			return err
		}

	case int16:
		if _, err := io.WriteString(output, strconv.FormatInt(int64(typedLiteral), 10)); err != nil {
			return err
		}

	case int32:
		if _, err := io.WriteString(output, strconv.FormatInt(int64(typedLiteral), 10)); err != nil {
			return err
		}

	case int64:
		if _, err := io.WriteString(output, strconv.FormatInt(typedLiteral, 10)); err != nil {
			return err
		}

	case int:
		if _, err := io.WriteString(output, strconv.FormatInt(int64(typedLiteral), 10)); err != nil {
			return err
		}

	case uint8:
		if _, err := io.WriteString(output, strconv.FormatUint(uint64(typedLiteral), 10)); err != nil {
			return err
		}

	case uint16:
		if _, err := io.WriteString(output, strconv.FormatUint(uint64(typedLiteral), 10)); err != nil {
			return err
		}

	case uint32:
		if _, err := io.WriteString(output, strconv.FormatUint(uint64(typedLiteral), 10)); err != nil {
			return err
		}

	case uint64:
		if _, err := io.WriteString(output, strconv.FormatUint(typedLiteral, 10)); err != nil {
			return err
		}

	case uint:
		if _, err := io.WriteString(output, strconv.FormatUint(uint64(typedLiteral), 10)); err != nil {
			return err
		}

	case bool:
		if _, err := io.WriteString(output, strconv.FormatBool(typedLiteral)); err != nil {
			return err
		}

	case float32:
		if _, err := io.WriteString(output, strconv.FormatFloat(float64(typedLiteral), 'f', -1, 64)); err != nil {
			return err
		}

	case float64:
		if _, err := io.WriteString(output, strconv.FormatFloat(typedLiteral, 'f', -1, 64)); err != nil {
			return err
		}

	default:
		return s.WriteExpression(output, literal.Value)
	}

	return nil
}

func (s Emitter) WriteExpression(output io.Writer, expression cypher.Expression) error {
	switch typedExpression := expression.(type) {
	case *cypher.ProjectionItem:
		if err := s.WriteExpression(output, typedExpression.Expression); err != nil {
			return err
		}

		if typedExpression.Alias != nil {
			if _, err := io.WriteString(output, " as "); err != nil {
				return err
			}

			if err := s.WriteExpression(output, typedExpression.Alias); err != nil {
				return err
			}
		}

	case *cypher.Negation:
		if _, err := io.WriteString(output, "not "); err != nil {
			return err
		}

		switch innerExpression := typedExpression.Expression.(type) {
		case *cypher.Parenthetical:
			if err := s.WriteExpression(output, innerExpression); err != nil {
				return err
			}

		default:
			if err := s.WriteExpression(output, innerExpression); err != nil {
				return err
			}
		}

	case *cypher.IDInCollection:
		if err := s.WriteExpression(output, typedExpression.Variable); err != nil {
			return err
		}

		if _, err := io.WriteString(output, " in "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, typedExpression.Expression); err != nil {
			return err
		}

	case *cypher.FilterExpression:
		if err := s.WriteExpression(output, typedExpression.Specifier); err != nil {
			return err
		}

		if typedExpression.Where != nil && len(typedExpression.Where.Expressions) > 0 {
			if err := s.formatWhere(output, typedExpression.Where); err != nil {
				return err
			}
		}

	case *cypher.Quantifier:
		if _, err := io.WriteString(output, typedExpression.Type.String()); err != nil {
			return err
		}

		if _, err := io.WriteString(output, "("); err != nil {
			return err
		}

		if err := s.WriteExpression(output, typedExpression.Filter); err != nil {
			return err
		}

		if _, err := io.WriteString(output, ")"); err != nil {
			return err
		}

	case *cypher.Parenthetical:
		if _, err := io.WriteString(output, "("); err != nil {
			return err
		}

		if err := s.WriteExpression(output, typedExpression.Expression); err != nil {
			return err
		}

		if _, err := io.WriteString(output, ")"); err != nil {
			return err
		}

	case *cypher.Disjunction:
		for idx, joinedExpression := range typedExpression.Expressions {
			if idx > 0 {
				if _, err := io.WriteString(output, " or "); err != nil {
					return err
				}
			}

			if err := s.WriteExpression(output, joinedExpression); err != nil {
				return err
			}
		}

	case *cypher.ExclusiveDisjunction:
		for idx, joinedExpression := range typedExpression.Expressions {
			if idx > 0 {
				if _, err := io.WriteString(output, " xor "); err != nil {
					return err
				}
			}

			if err := s.WriteExpression(output, joinedExpression); err != nil {
				return err
			}
		}

	case *cypher.Conjunction:
		for idx, joinedExpression := range typedExpression.Expressions {
			if idx > 0 {
				if _, err := io.WriteString(output, " and "); err != nil {
					return err
				}
			}

			if err := s.WriteExpression(output, joinedExpression); err != nil {
				return err
			}
		}

	case *cypher.Comparison:
		if err := s.WriteExpression(output, typedExpression.Left); err != nil {
			return err
		}

		for _, nextPart := range typedExpression.Partials {
			if err := s.WriteExpression(output, nextPart); err != nil {
				return err
			}
		}

	case *cypher.PartialComparison:
		if _, err := io.WriteString(output, " "); err != nil {
			return err
		}

		if _, err := io.WriteString(output, typedExpression.Operator.String()); err != nil {
			return err
		}

		if _, err := io.WriteString(output, " "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, typedExpression.Right); err != nil {
			return err
		}

	case *cypher.Properties:
		if typedExpression.Map != nil {
			if err := s.formatMapLiteral(output, typedExpression.Map); err != nil {
				return err
			}
		} else if err := s.WriteExpression(output, typedExpression.Parameter); err != nil {
			return err
		}

	case *cypher.Variable:
		if _, err := io.WriteString(output, typedExpression.Symbol); err != nil {
			return err
		}

	case *cypher.Parameter:
		if _, err := io.WriteString(output, "$"); err != nil {
			return err
		}

		if _, err := io.WriteString(output, typedExpression.Symbol); err != nil {
			return err
		}

	case *cypher.PropertyLookup:
		if err := s.WriteExpression(output, typedExpression.Atom); err != nil {
			return err
		}

		if _, err := io.WriteString(output, "."); err != nil {
			return err
		}

		if _, err := io.WriteString(output, typedExpression.Symbol); err != nil {
			return err
		}

	case *cypher.FunctionInvocation:
		if _, err := io.WriteString(output, strings.Join(typedExpression.Namespace, ".")); err != nil {
			return err
		}

		if _, err := io.WriteString(output, typedExpression.Name); err != nil {
			return err
		}

		if _, err := io.WriteString(output, "("); err != nil {
			return err
		}

		if typedExpression.Distinct {
			if _, err := io.WriteString(output, "distinct "); err != nil {
				return err
			}
		}

		for idx, subExpression := range typedExpression.Arguments {
			if idx > 0 {
				if _, err := io.WriteString(output, ", "); err != nil {
					return err
				}
			}

			if err := s.WriteExpression(output, subExpression); err != nil {
				return err
			}
		}

		if _, err := io.WriteString(output, ")"); err != nil {
			return err
		}

	case graph.Kind:
		if _, err := io.WriteString(output, ":"); err != nil {
			return err
		}

		if _, err := io.WriteString(output, typedExpression.String()); err != nil {
			return err
		}

	case graph.Kinds:
		if _, err := io.WriteString(output, ":"); err != nil {
			return err
		}

		if err := writeJoinedKinds(output, ":", typedExpression); err != nil {
			return err
		}

	case *cypher.KindMatcher:
		if len(typedExpression.Kinds) > 1 {
			if _, err := io.WriteString(output, "("); err != nil {
				return err
			}
		}

		for idx, matcher := range typedExpression.Kinds {
			if idx > 0 {
				if _, err := io.WriteString(output, " or "); err != nil {
					return err
				}
			}

			if err := s.WriteExpression(output, typedExpression.Reference); err != nil {
				return err
			}

			if _, err := io.WriteString(output, ":"); err != nil {
				return err
			}

			if _, err := io.WriteString(output, matcher.String()); err != nil {
				return err
			}
		}

		if len(typedExpression.Kinds) > 1 {
			if _, err := io.WriteString(output, ")"); err != nil {
				return err
			}
		}

	case *cypher.RangeQuantifier:
		if _, err := io.WriteString(output, typedExpression.Value); err != nil {
			return err
		}

	case cypher.Operator:
		if _, err := io.WriteString(output, typedExpression.String()); err != nil {
			return err
		}

	case *cypher.Skip:
		return s.WriteExpression(output, typedExpression.Value)

	case *cypher.Limit:
		return s.WriteExpression(output, typedExpression.Value)

	case *cypher.Literal:
		if !s.StripLiterals {
			return s.formatLiteral(output, typedExpression)
		} else {
			_, err := io.WriteString(output, strippedLiteral)
			return err
		}

	case cypher.MapLiteral:
		if err := s.formatMapLiteral(output, typedExpression); err != nil {
			return err
		}

	case *cypher.ListLiteral:
		if !s.StripLiterals {
			if _, err := io.WriteString(output, "["); err != nil {
				return err
			}

			for idx, subExpression := range *typedExpression {
				if idx > 0 {
					if _, err := io.WriteString(output, ", "); err != nil {
						return err
					}
				}

				if err := s.WriteExpression(output, subExpression); err != nil {
					return err
				}
			}

			if _, err := io.WriteString(output, "]"); err != nil {
				return err
			}
		} else {
			_, err := io.WriteString(output, strippedLiteral)
			return err
		}

	case *cypher.PatternPredicate:
		return s.formatPatternElements(output, typedExpression.PatternElements)

	case *cypher.ArithmeticExpression:
		if err := s.WriteExpression(output, typedExpression.Left); err != nil {
			return err
		}

		for _, part := range typedExpression.Partials {
			if err := s.WriteExpression(output, part); err != nil {
				return err
			}
		}

	case *cypher.PartialArithmeticExpression:
		if _, err := io.WriteString(output, " "); err != nil {
			return err
		}

		if _, err := io.WriteString(output, typedExpression.Operator.String()); err != nil {
			return err
		}

		if _, err := io.WriteString(output, " "); err != nil {
			return err
		}

		return s.WriteExpression(output, typedExpression.Right)

	case *cypher.UnaryAddOrSubtractExpression:
		if _, err := io.WriteString(output, typedExpression.Operator.String()); err != nil {
			return err
		}

		return s.WriteExpression(output, typedExpression.Right)

	default:
		return fmt.Errorf("unexpected expression type for string formatting: %T", expression)
	}

	return nil
}

func (s Emitter) formatRemove(output io.Writer, remove *cypher.Remove) error {
	if _, err := io.WriteString(output, "remove "); err != nil {
		return err
	}

	for idx, removeItem := range remove.Items {
		if idx > 0 {
			if _, err := io.WriteString(output, ", "); err != nil {
				return err
			}
		}

		if removeItem.KindMatcher != nil {
			if err := s.WriteExpression(output, removeItem.KindMatcher.Reference); err != nil {
				return err
			}

			if err := s.WriteExpression(output, removeItem.KindMatcher.Kinds); err != nil {
				return err
			}
		} else if removeItem.Property != nil {
			if err := s.WriteExpression(output, removeItem.Property); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("empty remove item")
		}
	}

	return nil
}

func (s Emitter) formatSet(output io.Writer, set *cypher.Set) error {
	if _, err := io.WriteString(output, "set "); err != nil {
		return err
	}

	for idx, setItem := range set.Items {
		if idx > 0 {
			if _, err := io.WriteString(output, ", "); err != nil {
				return err
			}
		}

		if err := s.WriteExpression(output, setItem.Left); err != nil {
			return err
		}

		switch setItem.Operator {
		case cypher.OperatorLabelAssignment:
		default:
			if _, err := io.WriteString(output, " "); err != nil {
				return err
			}

			if _, err := io.WriteString(output, setItem.Operator.String()); err != nil {
				return err
			}

			if _, err := io.WriteString(output, " "); err != nil {
				return err
			}
		}

		if err := s.WriteExpression(output, setItem.Right); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatDelete(output io.Writer, delete *cypher.Delete) error {
	if delete.Detach {
		if _, err := io.WriteString(output, "detach delete "); err != nil {
			return err
		}
	} else if _, err := io.WriteString(output, "delete "); err != nil {
		return err
	}

	for idx, expression := range delete.Expressions {
		if idx > 0 {
			if _, err := io.WriteString(output, ", "); err != nil {
				return err
			}
		}

		if err := s.WriteExpression(output, expression); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatPattern(output io.Writer, pattern []*cypher.PatternPart) error {
	for idx, patternPart := range pattern {
		if idx > 0 {
			if _, err := io.WriteString(output, ", "); err != nil {
				return err
			}
		}

		if err := s.formatPatternPart(output, patternPart); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatCreate(output io.Writer, create *cypher.Create) error {
	if _, err := io.WriteString(output, "create "); err != nil {
		return err
	}

	return s.formatPattern(output, create.Pattern)
}

func (s Emitter) formatMerge(output io.Writer, merge *cypher.Merge) error {
	if _, err := io.WriteString(output, "merge "); err != nil {
		return err
	}

	if err := s.formatPatternPart(output, merge.PatternPart); err != nil {
		return err
	}

	if err := s.formatMergeActions(output, merge.MergeActions); err != nil {
		return err
	}

	return nil
}

func (s Emitter) formatMergeActions(output io.Writer, mergeActions []*cypher.MergeAction) error {
	for _, mergeAction := range mergeActions {
		if _, err := io.WriteString(output, " "); err != nil {
			return err
		}

		if mergeAction.OnCreate {
			if _, err := io.WriteString(output, "on create "); err != nil {
				return err
			}
		}

		if mergeAction.OnMatch {
			if _, err := io.WriteString(output, "on match "); err != nil {
				return err
			}
		}

		if err := s.formatSet(output, mergeAction.Set); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatUpdatingClause(output io.Writer, updatingClause *cypher.UpdatingClause) error {
	switch typedClause := updatingClause.Clause.(type) {
	case *cypher.Create:
		return s.formatCreate(output, typedClause)

	case *cypher.Remove:
		return s.formatRemove(output, typedClause)

	case *cypher.Set:
		return s.formatSet(output, typedClause)

	case *cypher.Delete:
		return s.formatDelete(output, typedClause)

	case *cypher.Merge:
		return s.formatMerge(output, typedClause)

	default:
		return fmt.Errorf("unsupported updating clause type: %T", updatingClause)
	}
}

func (s Emitter) formatReadingClause(output io.Writer, readingClause *cypher.ReadingClause) error {
	if readingClause.Match != nil {
		if readingClause.Match.Optional {
			if _, err := io.WriteString(output, "optional "); err != nil {
				return err
			}
		}

		if _, err := io.WriteString(output, "match "); err != nil {
			return err
		}

		for idx, patternPart := range readingClause.Match.Pattern {
			if idx > 0 {
				if _, err := io.WriteString(output, ", "); err != nil {
					return err
				}
			}

			if err := s.formatPatternPart(output, patternPart); err != nil {
				return err
			}
		}

		if readingClause.Match.Where != nil && len(readingClause.Match.Where.Expressions) > 0 {
			if err := s.formatWhere(output, readingClause.Match.Where); err != nil {
				return err
			}
		}
	} else if readingClause.Unwind != nil {
		if _, err := io.WriteString(output, "unwind "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, readingClause.Unwind.Expression); err != nil {
			return err
		}

		if _, err := io.WriteString(output, " as "); err != nil {
			return err
		}

		if err := s.WriteExpression(output, readingClause.Unwind.Variable); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("reading clause has no match or unwind statement")
	}

	return nil
}

func (s Emitter) formatSinglePartQuery(writer io.Writer, singlePartQuery *cypher.SinglePartQuery) error {
	for idx, readingClause := range singlePartQuery.ReadingClauses {
		if idx > 0 {
			if _, err := io.WriteString(writer, " "); err != nil {
				return err
			}
		}

		if err := s.formatReadingClause(writer, readingClause); err != nil {
			return err
		}
	}

	if len(singlePartQuery.UpdatingClauses) > 0 {
		if len(singlePartQuery.ReadingClauses) > 0 {
			if _, err := io.WriteString(writer, " "); err != nil {
				return err
			}
		}

		for idx, updatingClause := range singlePartQuery.UpdatingClauses {
			if idx > 0 {
				if _, err := io.WriteString(writer, " "); err != nil {
					return err
				}
			}

			if typedUpdatingClause, typeOK := updatingClause.(*cypher.UpdatingClause); !typeOK {
				return fmt.Errorf("unexpected updating clause type %T", updatingClause)
			} else if err := s.formatUpdatingClause(writer, typedUpdatingClause); err != nil {
				return err
			}
		}
	}

	if singlePartQuery.Return != nil {
		return s.formatReturn(writer, singlePartQuery.Return)
	}

	return nil
}

func (s Emitter) formatWith(output io.Writer, with *cypher.With) error {
	if _, err := io.WriteString(output, "with "); err != nil {
		return err
	}

	if err := s.formatProjection(output, with.Projection); err != nil {
		return err
	}

	if with.Where != nil && len(with.Where.Expressions) > 0 {
		if err := s.formatWhere(output, with.Where); err != nil {
			return err
		}
	}

	return nil
}

func (s Emitter) formatMultiPartQuery(output io.Writer, multiPartQuery *cypher.MultiPartQuery) error {
	for idx, multiPartQueryPart := range multiPartQuery.Parts {
		var (
			numReadingClauses  = len(multiPartQueryPart.ReadingClauses)
			numUpdatingClauses = len(multiPartQueryPart.UpdatingClauses)
		)

		if idx > 0 {
			if _, err := io.WriteString(output, " "); err != nil {
				return err
			}
		}

		for idx, readingClause := range multiPartQueryPart.ReadingClauses {
			if idx > 0 {
				if _, err := io.WriteString(output, " "); err != nil {
					return err
				}
			}

			if err := s.formatReadingClause(output, readingClause); err != nil {
				return err
			}
		}

		if len(multiPartQueryPart.UpdatingClauses) > 0 {
			if numReadingClauses > 0 {
				if _, err := io.WriteString(output, " "); err != nil {
					return err
				}
			}

			for idx, updatingClause := range multiPartQueryPart.UpdatingClauses {
				if idx > 0 {
					if _, err := io.WriteString(output, " "); err != nil {
						return err
					}
				}

				if err := s.formatUpdatingClause(output, updatingClause); err != nil {
					return err
				}
			}
		}

		if multiPartQueryPart.With != nil {
			if numReadingClauses+numUpdatingClauses > 0 {
				if _, err := io.WriteString(output, " "); err != nil {
					return err
				}
			}

			if err := s.formatWith(output, multiPartQueryPart.With); err != nil {
				return err
			}
		}
	}

	if multiPartQuery.SinglePartQuery != nil {
		if len(multiPartQuery.Parts) > 0 {
			if _, err := io.WriteString(output, " "); err != nil {
				return err
			}
		}

		return s.formatSinglePartQuery(output, multiPartQuery.SinglePartQuery)
	}

	return nil
}

func (s Emitter) Write(regularQuery *cypher.RegularQuery, writer io.Writer) error {
	if regularQuery.SingleQuery != nil {
		if regularQuery.SingleQuery.MultiPartQuery != nil {
			if err := s.formatMultiPartQuery(writer, regularQuery.SingleQuery.MultiPartQuery); err != nil {
				return err
			}
		}

		if regularQuery.SingleQuery.SinglePartQuery != nil {
			if err := s.formatSinglePartQuery(writer, regularQuery.SingleQuery.SinglePartQuery); err != nil {
				return err
			}
		}
	}

	return nil
}

func RegularQuery(query *cypher.RegularQuery, stripLiterals bool) (string, error) {
	buffer := &bytes.Buffer{}

	if err := NewCypherEmitter(stripLiterals).Write(query, buffer); err != nil {
		return "", err
	} else {
		return buffer.String(), nil
	}
}
