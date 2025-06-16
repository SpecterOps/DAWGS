package frontend

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/parser"
)

type NamespaceVisitor struct {
	BaseVisitor

	Namespace []string
}

func (s *NamespaceVisitor) EnterOC_SymbolicName(ctx *parser.OC_SymbolicNameContext) {
	s.Namespace = append(s.Namespace, ctx.GetText())
}

type FunctionInvocationVisitor struct {
	BaseVisitor

	FunctionInvocation *cypher.FunctionInvocation
}

func NewFunctionInvocationVisitor(ctx *parser.OC_FunctionInvocationContext) *FunctionInvocationVisitor {
	return &FunctionInvocationVisitor{
		FunctionInvocation: &cypher.FunctionInvocation{
			Distinct: HasTokens(ctx, parser.CypherLexerDISTINCT),
		},
	}
}

func (s *FunctionInvocationVisitor) EnterOC_Namespace(ctx *parser.OC_NamespaceContext) {
	s.ctx.Enter(&NamespaceVisitor{})
}

func (s *FunctionInvocationVisitor) ExitOC_Namespace(ctx *parser.OC_NamespaceContext) {
	s.FunctionInvocation.Namespace = s.ctx.Exit().(*NamespaceVisitor).Namespace
}

func (s *FunctionInvocationVisitor) EnterOC_SymbolicName(ctx *parser.OC_SymbolicNameContext) {
	s.FunctionInvocation.Name = ctx.GetText()
}

func (s *FunctionInvocationVisitor) EnterOC_Expression(ctx *parser.OC_ExpressionContext) {
	s.ctx.Enter(&ExpressionVisitor{})
}

func (s *FunctionInvocationVisitor) ExitOC_Expression(ctx *parser.OC_ExpressionContext) {
	result := s.ctx.Exit().(*ExpressionVisitor).Expression
	s.FunctionInvocation.Arguments = append(s.FunctionInvocation.Arguments, result)
}
