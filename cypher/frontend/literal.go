package frontend

import (
	"fmt"
	"strconv"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/cypher/parser"
)

type SymbolicNameOrReservedWordVisitor struct {
	BaseVisitor

	Name string
}

func (s *SymbolicNameOrReservedWordVisitor) EnterOC_SchemaName(ctx *parser.OC_SchemaNameContext) {
	s.Name = ctx.GetText()
}

func (s *SymbolicNameOrReservedWordVisitor) EnterOC_SymbolicName(ctx *parser.OC_SymbolicNameContext) {
	s.Name = ctx.GetText()
}

func (s *SymbolicNameOrReservedWordVisitor) EnterOC_ReservedWord(ctx *parser.OC_ReservedWordContext) {
	s.Name = ctx.GetText()
}

type MapLiteralVisitor struct {
	BaseVisitor

	nextPropertyKey string
	Map             cypher.MapLiteral
}

func NewMapLiteralVisitor() *MapLiteralVisitor {
	return &MapLiteralVisitor{
		Map: cypher.MapLiteral{},
	}
}

func (s *MapLiteralVisitor) EnterOC_PropertyKeyName(ctx *parser.OC_PropertyKeyNameContext) {
	s.ctx.Enter(&SymbolicNameOrReservedWordVisitor{})
}

func (s *MapLiteralVisitor) ExitOC_PropertyKeyName(ctx *parser.OC_PropertyKeyNameContext) {
	s.nextPropertyKey = s.ctx.Exit().(*SymbolicNameOrReservedWordVisitor).Name
}

func (s *MapLiteralVisitor) EnterOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.ctx.Enter(NewExpressionVisitor())
}

func (s *MapLiteralVisitor) ExitOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.Map[s.nextPropertyKey] = s.ctx.Exit().(*ExpressionVisitor).Expression
}

type ListLiteralVisitor struct {
	BaseVisitor

	List *cypher.ListLiteral
}

func NewListLiteralVisitor() *ListLiteralVisitor {
	return &ListLiteralVisitor{
		List: cypher.NewListLiteral(),
	}
}

func (s *ListLiteralVisitor) EnterOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.ctx.Enter(NewExpressionVisitor())
}

func (s *ListLiteralVisitor) ExitOC_Expression(ctx *parser.OC_ExpressionContext) {
	*s.List = append(*s.List, s.ctx.Exit().(*ExpressionVisitor).Expression)
}

type LiteralVisitor struct {
	BaseVisitor

	Literal     *cypher.Literal
	MapLiteral  cypher.MapLiteral
	ListLiteral *cypher.ListLiteral
}

func NewLiteralVisitor() *LiteralVisitor {
	return &LiteralVisitor{}
}

func (s *LiteralVisitor) GetLiteral() cypher.Expression {
	if s.ListLiteral != nil {
		return s.ListLiteral
	}

	if s.MapLiteral != nil {
		return s.MapLiteral
	}

	return s.Literal
}

func (s *LiteralVisitor) EnterOC_IntegerLiteral(ctx *parser.OC_IntegerLiteralContext) {
	text := ctx.GetText()

	if parsed, err := strconv.ParseInt(ctx.GetText(), 10, 64); err != nil {
		s.ctx.AddErrors(fmt.Errorf("invalid integer literal: %s - %w", text, err))
	} else {
		s.Literal = cypher.NewLiteral(parsed, false)
	}
}

func (s *LiteralVisitor) EnterOC_BooleanLiteral(ctx *parser.OC_BooleanLiteralContext) {
	text := ctx.GetText()

	if parsed, err := strconv.ParseBool(text); err != nil {
		s.ctx.AddErrors(fmt.Errorf("invalid boolean literal: %s - %w", text, err))
	} else {
		s.Literal = cypher.NewLiteral(parsed, false)
	}
}

func (s *LiteralVisitor) EnterOC_DoubleLiteral(ctx *parser.OC_DoubleLiteralContext) {
	text := ctx.GetText()

	if parsed, err := strconv.ParseFloat(text, 64); err != nil {
		s.ctx.AddErrors(fmt.Errorf("invalid double literal: %s - %w", text, err))
	} else {
		s.Literal = cypher.NewLiteral(parsed, false)
	}
}

func (s *LiteralVisitor) EnterOC_MapLiteral(ctx *parser.OC_MapLiteralContext) {
	s.ctx.Enter(NewMapLiteralVisitor())
}

func (s *LiteralVisitor) ExitOC_MapLiteral(ctx *parser.OC_MapLiteralContext) {
	s.MapLiteral = s.ctx.Exit().(*MapLiteralVisitor).Map
}

func (s *LiteralVisitor) EnterOC_ListLiteral(ctx *parser.OC_ListLiteralContext) {
	s.ctx.Enter(NewListLiteralVisitor())
}

func (s *LiteralVisitor) ExitOC_ListLiteral(ctx *parser.OC_ListLiteralContext) {
	s.ListLiteral = s.ctx.Exit().(*ListLiteralVisitor).List
}
