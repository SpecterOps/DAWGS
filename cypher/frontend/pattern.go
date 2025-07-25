package frontend

import (
	"fmt"
	"strconv"

	"github.com/antlr4-go/antlr/v4"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/parser"
	"github.com/specterops/dawgs/graph"
)

type WhereVisitor struct {
	BaseVisitor

	Where *cypher.Where
}

func NewWhereVisitor() *WhereVisitor {
	return &WhereVisitor{
		Where: cypher.NewWhere(),
	}
}

func (s *WhereVisitor) EnterOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.ctx.Enter(NewExpressionVisitor())
}

func (s *WhereVisitor) ExitOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.Where.Add(s.ctx.Exit().(*ExpressionVisitor).Expression)
}

type NodePatternVisitor struct {
	BaseVisitor

	NodePattern *cypher.NodePattern
}

func (s *NodePatternVisitor) EnterOC_Variable(ctx *parser.OC_VariableContext) {
	s.ctx.Enter(NewVariableVisitor())
}

func (s *NodePatternVisitor) ExitOC_Variable(ctx *parser.OC_VariableContext) {
	s.NodePattern.Variable = s.ctx.Exit().(*VariableVisitor).Variable
}

func (s *NodePatternVisitor) EnterOC_LabelName(ctx *parser.OC_LabelNameContext) {
	s.ctx.Enter(&SymbolicNameOrReservedWordVisitor{})
}

func (s *NodePatternVisitor) ExitOC_LabelName(ctx *parser.OC_LabelNameContext) {
	kind := graph.StringKind(s.ctx.Exit().(*SymbolicNameOrReservedWordVisitor).Name)
	s.NodePattern.Kinds = append(s.NodePattern.Kinds, kind)
}

func (s *NodePatternVisitor) EnterOC_Properties(ctx *parser.OC_PropertiesContext) {
	s.ctx.Enter(NewPropertiesVisitor())
}

func (s *NodePatternVisitor) ExitOC_Properties(ctx *parser.OC_PropertiesContext) {
	s.NodePattern.Properties = s.ctx.Exit().(*PropertiesVisitor).Properties
}

type RelationshipPatternVisitor struct {
	BaseVisitor

	RelationshipPattern *cypher.RelationshipPattern
}

func (s *RelationshipPatternVisitor) EnterOC_RelTypeName(ctx *parser.OC_RelTypeNameContext) {
	s.ctx.Enter(&SymbolicNameOrReservedWordVisitor{})
}

func (s *RelationshipPatternVisitor) ExitOC_RelTypeName(ctx *parser.OC_RelTypeNameContext) {
	relationshipType := graph.StringKind(ctx.GetText())

	s.RelationshipPattern.Kinds = s.RelationshipPattern.Kinds.Add(relationshipType)
	s.ctx.Exit()
}

func (s *RelationshipPatternVisitor) EnterOC_Variable(ctx *parser.OC_VariableContext) {
	s.ctx.Enter(NewVariableVisitor())
}

func (s *RelationshipPatternVisitor) ExitOC_Variable(ctx *parser.OC_VariableContext) {
	s.RelationshipPattern.Variable = s.ctx.Exit().(*VariableVisitor).Variable
}

func (s *RelationshipPatternVisitor) EnterOC_LeftArrowHead(ctx *parser.OC_LeftArrowHeadContext) {
	s.RelationshipPattern.Direction = graph.DirectionInbound
}

func (s *RelationshipPatternVisitor) ExitOC_LeftArrowHead(ctx *parser.OC_LeftArrowHeadContext) {
}

func (s *RelationshipPatternVisitor) EnterOC_RightArrowHead(ctx *parser.OC_RightArrowHeadContext) {
	if s.RelationshipPattern.Direction == graph.DirectionInbound {
		s.RelationshipPattern.Direction = graph.DirectionBoth
	} else {
		s.RelationshipPattern.Direction = graph.DirectionOutbound
	}
}

func (s *RelationshipPatternVisitor) EnterOC_RangeLiteral(ctx *parser.OC_RangeLiteralContext) {
	const (
		stateStart int = iota
		stateFirstIndex
		stateSecondIndex
	)

	// Create a new relationship pattern range for the relationship pattern being built
	s.RelationshipPattern.Range = &cypher.PatternRange{}

	// Start at the start state for the mini-parser below
	state := stateStart

	for _, tokenLeaf := range ctx.GetChildren() {
		switch typedTokenLeaf := tokenLeaf.(type) {
		case *antlr.TerminalNodeImpl:
			switch typedTokenLeaf.GetSymbol().GetTokenType() {
			case TokenTypeAsterisk:
				state = stateFirstIndex

			case TokenTypeRange:
				state = stateSecondIndex

			default:
				s.ctx.AddErrors(fmt.Errorf("unexpected token in pattern range: %s", typedTokenLeaf.GetText()))
			}

		case *parser.OC_IntegerLiteralContext:
			if value, err := strconv.ParseInt(typedTokenLeaf.GetText(), 10, 64); err != nil {
				s.ctx.AddErrors(fmt.Errorf("failed parsing range literal: %w", err))
			} else {
				switch state {
				case stateFirstIndex:
					s.RelationshipPattern.Range.StartIndex = &value

				case stateSecondIndex:
					s.RelationshipPattern.Range.EndIndex = &value

				default:
					s.ctx.AddErrors(fmt.Errorf("invalid integer literal state: %d", state))
				}
			}
		}
	}
}

func (s *RelationshipPatternVisitor) EnterOC_Properties(ctx *parser.OC_PropertiesContext) {
	s.ctx.Enter(NewPropertiesVisitor())
}

func (s *RelationshipPatternVisitor) ExitOC_Properties(ctx *parser.OC_PropertiesContext) {
	s.RelationshipPattern.Properties = s.ctx.Exit().(*PropertiesVisitor).Properties
}

type PatternPredicateVisitor struct {
	BaseVisitor

	PatternPredicate *cypher.PatternPredicate
}

func NewPatternPredicateVisitor() *PatternPredicateVisitor {
	return &PatternPredicateVisitor{
		PatternPredicate: cypher.NewPatternPredicate(),
	}
}

func (s *PatternPredicateVisitor) EnterOC_NodePattern(ctx *parser.OC_NodePatternContext) {
	s.ctx.Enter(&NodePatternVisitor{
		NodePattern: &cypher.NodePattern{},
	})
}

func (s *PatternPredicateVisitor) ExitOC_NodePattern(ctx *parser.OC_NodePatternContext) {
	s.PatternPredicate.AddElement(s.ctx.Exit().(*NodePatternVisitor).NodePattern)
}

func (s *PatternPredicateVisitor) EnterOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
	s.ctx.Enter(&RelationshipPatternVisitor{
		RelationshipPattern: &cypher.RelationshipPattern{
			Direction: graph.DirectionBoth,
		},
	})
}

func (s *PatternPredicateVisitor) ExitOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
	s.PatternPredicate.AddElement(s.ctx.Exit().(*RelationshipPatternVisitor).RelationshipPattern)
}

type PatternVisitor struct {
	BaseVisitor

	PatternParts []*cypher.PatternPart
}

func (s *PatternVisitor) EnterOC_PatternPart(ctx *parser.OC_PatternPartContext) {
	s.ctx.Enter(&PatternPartVisitor{
		PatternPart: &cypher.PatternPart{},
	})
}

func (s *PatternVisitor) ExitOC_PatternPart(ctx *parser.OC_PatternPartContext) {
	s.PatternParts = append(s.PatternParts, s.ctx.Exit().(*PatternPartVisitor).PatternPart)
}

type PatternPartVisitor struct {
	BaseVisitor

	PatternPart *cypher.PatternPart
}

func (s *PatternPartVisitor) EnterOC_PatternPart(ctx *parser.OC_PatternPartContext) {
	s.PatternPart = &cypher.PatternPart{}
}

func (s *PatternPartVisitor) EnterOC_ShortestPathPattern(ctx *parser.OC_ShortestPathPatternContext) {
	if HasTokens(ctx, parser.CypherLexerSHORTESTPATH) {
		s.PatternPart.ShortestPathPattern = true
	} else if HasTokens(ctx, parser.CypherLexerALLSHORTESTPATHS) {
		s.PatternPart.AllShortestPathsPattern = true
	}
}

func (s *PatternPartVisitor) EnterOC_Variable(ctx *parser.OC_VariableContext) {
	s.ctx.Enter(NewVariableVisitor())
}

func (s *PatternPartVisitor) ExitOC_Variable(ctx *parser.OC_VariableContext) {
	s.PatternPart.Variable = s.ctx.Exit().(*VariableVisitor).Variable
}

func (s *PatternPartVisitor) EnterOC_NodePattern(ctx *parser.OC_NodePatternContext) {
	s.ctx.Enter(&NodePatternVisitor{
		NodePattern: &cypher.NodePattern{},
	})
}

func (s *PatternPartVisitor) ExitOC_NodePattern(ctx *parser.OC_NodePatternContext) {
	s.PatternPart.AddPatternElements(s.ctx.Exit().(*NodePatternVisitor).NodePattern)
}

func (s *PatternPartVisitor) EnterOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
	s.ctx.Enter(&RelationshipPatternVisitor{
		RelationshipPattern: &cypher.RelationshipPattern{
			Direction: graph.DirectionBoth,
		},
	})
}

func (s *PatternPartVisitor) ExitOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
	s.PatternPart.AddPatternElements(s.ctx.Exit().(*RelationshipPatternVisitor).RelationshipPattern)
}
