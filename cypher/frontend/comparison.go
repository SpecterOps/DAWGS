package frontend

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/antlr4-go/antlr/v4"
	"github.com/specterops/dawgs/cypher/parser"
)

// oC_PartialComparisonExpression
//                           :  ( '=' SP? oC_StringListNullPredicateExpression )
//                               | ( '<>' SP? oC_StringListNullPredicateExpression )
//                               | ( '<' SP? oC_StringListNullPredicateExpression )
//                               | ( '>' SP? oC_StringListNullPredicateExpression )
//                               | ( '<=' SP? oC_StringListNullPredicateExpression )
//                               | ( '>=' SP? oC_StringListNullPredicateExpression )
//                               ;

// PartialComparisonVisitor
type PartialComparisonVisitor struct {
	BaseVisitor

	PartialComparison *cypher.PartialComparison
}

func NewPartialComparisonVisitor() *PartialComparisonVisitor {
	return &PartialComparisonVisitor{
		PartialComparison: &cypher.PartialComparison{},
	}
}

func (s *PartialComparisonVisitor) EnterOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	s.ctx.Enter(&StringListNullPredicateExpressionVisitor{})
}

func (s *PartialComparisonVisitor) ExitOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	result := s.ctx.Exit().(*StringListNullPredicateExpressionVisitor).Expression
	s.PartialComparison.Right = result
}

// oC_StringListNullPredicateExpression ( SP? oC_PartialComparisonExpression )*
type ComparisonVisitor struct {
	BaseVisitor

	Comparison *cypher.Comparison
}

func (s *ComparisonVisitor) EnterOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	s.ctx.Enter(&StringListNullPredicateExpressionVisitor{})
}

func (s *ComparisonVisitor) ExitOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	result := s.ctx.Exit().(*StringListNullPredicateExpressionVisitor).Expression

	s.Comparison = &cypher.Comparison{
		Left: result,
	}
}

func (s *ComparisonVisitor) EnterOC_PartialComparisonExpression(ctx *parser.OC_PartialComparisonExpressionContext) {
	partialComparisonVisitor := NewPartialComparisonVisitor()

	switch operatorChild := ctx.GetChild(0).(type) {
	case *antlr.TerminalNodeImpl:
		if operator, err := cypher.ParseOperator(operatorChild.GetText()); err != nil {
			s.ctx.AddErrors(err)
		} else {
			partialComparisonVisitor.PartialComparison.Operator = operator
		}

	default:
		s.ctx.AddErrors(fmt.Errorf("expected oC_PartialComparisonExpression to contain an operator token at branch position 0 but saw: %T %+v", operatorChild, operatorChild))
	}

	s.ctx.Enter(partialComparisonVisitor)
}

func (s *ComparisonVisitor) ExitOC_PartialComparisonExpression(ctx *parser.OC_PartialComparisonExpressionContext) {
	s.Comparison.AddPartialComparison(s.ctx.Exit().(*PartialComparisonVisitor).PartialComparison)
}
