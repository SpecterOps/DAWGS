package frontend

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/parser"
)

type ParenthesizedExpressionVisitor struct {
	BaseVisitor

	Parenthetical *cypher.Parenthetical
}

func NewParenthesizedExpressionVisitor() *ParenthesizedExpressionVisitor {
	return &ParenthesizedExpressionVisitor{}
}

func (s *ParenthesizedExpressionVisitor) EnterOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.ctx.Enter(NewExpressionVisitor())
}

func (s *ParenthesizedExpressionVisitor) ExitOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.Parenthetical = &cypher.Parenthetical{
		Expression: s.ctx.Exit().(*ExpressionVisitor).Expression,
	}
}

type NegationVisitor struct {
	BaseVisitor

	Negation *cypher.Negation
}

func (s *NegationVisitor) EnterOC_ComparisonExpression(ctx *parser.OC_ComparisonExpressionContext) {
	if len(ctx.AllOC_PartialComparisonExpression()) > 0 {
		s.ctx.Enter(&ComparisonVisitor{})
	}
}

func (s *NegationVisitor) ExitOC_ComparisonExpression(ctx *parser.OC_ComparisonExpressionContext) {
	if len(ctx.AllOC_PartialComparisonExpression()) > 0 {
		result := s.ctx.Exit().(*ComparisonVisitor).Comparison
		s.Negation.Expression = result
	}
}

func (s *NegationVisitor) EnterOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	s.ctx.Enter(&StringListNullPredicateExpressionVisitor{})
}

func (s *NegationVisitor) ExitOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	result := s.ctx.Exit().(*StringListNullPredicateExpressionVisitor).Expression
	s.Negation.Expression = result
}

type JoiningVisitor struct {
	BaseVisitor

	Joined cypher.ExpressionList
}

func (s *JoiningVisitor) EnterOC_NotExpression(ctx *parser.OC_NotExpressionContext) {
	if len(ctx.AllNOT()) > 0 {
		s.ctx.Enter(&NegationVisitor{
			Negation: &cypher.Negation{},
		})
	}
}

func (s *JoiningVisitor) ExitOC_NotExpression(ctx *parser.OC_NotExpressionContext) {
	if len(ctx.AllNOT()) > 0 {
		visitor := s.ctx.Exit().(*NegationVisitor)
		s.Joined.Add(visitor.Negation)
	}
}

func (s *JoiningVisitor) EnterOC_OrExpression(ctx *parser.OC_OrExpressionContext) {
	if len(ctx.AllOR()) > 0 {
		s.ctx.Enter(&JoiningVisitor{
			Joined: &cypher.Disjunction{},
		})
	}
}

func (s *JoiningVisitor) ExitOC_OrExpression(ctx *parser.OC_OrExpressionContext) {
	if len(ctx.AllOR()) > 0 {
		visitor := s.ctx.Exit().(*JoiningVisitor)
		s.Joined.Add(visitor.Joined)
	}
}

func (s *JoiningVisitor) EnterOC_XorExpression(ctx *parser.OC_XorExpressionContext) {
	if len(ctx.AllXOR()) > 0 {
		s.ctx.Enter(&JoiningVisitor{
			Joined: &cypher.ExclusiveDisjunction{},
		})
	}
}

func (s *JoiningVisitor) ExitOC_XorExpression(ctx *parser.OC_XorExpressionContext) {
	if len(ctx.AllXOR()) > 0 {
		visitor := s.ctx.Exit().(*JoiningVisitor)
		s.Joined.Add(visitor.Joined)
	}
}

func (s *JoiningVisitor) EnterOC_AndExpression(ctx *parser.OC_AndExpressionContext) {
	if len(ctx.AllAND()) > 0 {
		s.ctx.Enter(&JoiningVisitor{
			Joined: &cypher.Conjunction{},
		})
	}
}

func (s *JoiningVisitor) ExitOC_AndExpression(ctx *parser.OC_AndExpressionContext) {
	if len(ctx.AllAND()) > 0 {
		visitor := s.ctx.Exit().(*JoiningVisitor)
		s.Joined.Add(visitor.Joined)
	}
}

func (s *JoiningVisitor) EnterOC_ComparisonExpression(ctx *parser.OC_ComparisonExpressionContext) {
	if len(ctx.AllOC_PartialComparisonExpression()) > 0 {
		s.ctx.Enter(&ComparisonVisitor{})
	}
}

func (s *JoiningVisitor) ExitOC_ComparisonExpression(ctx *parser.OC_ComparisonExpressionContext) {
	if len(ctx.AllOC_PartialComparisonExpression()) > 0 {
		result := s.ctx.Exit().(*ComparisonVisitor).Comparison
		s.Joined.Add(result)
	}
}

func (s *JoiningVisitor) EnterOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	s.ctx.Enter(&StringListNullPredicateExpressionVisitor{})
}

func (s *JoiningVisitor) ExitOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	result := s.ctx.Exit().(*StringListNullPredicateExpressionVisitor).Expression
	s.Joined.Add(result)
}
