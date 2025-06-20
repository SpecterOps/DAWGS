package frontend

import (
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/antlr4-go/antlr/v4"
	"github.com/specterops/dawgs/cypher/parser"
	"github.com/specterops/dawgs/graph"
)

// PropertiesVisitor
//
// oC_Properties: oC_MapLiteral | oC_Parameter | oC_LegacyParameter
type PropertiesVisitor struct {
	BaseVisitor

	Properties *cypher.Properties
}

func NewPropertiesVisitor() *PropertiesVisitor {
	return &PropertiesVisitor{
		Properties: cypher.NewProperties(),
	}
}

func (s *PropertiesVisitor) EnterOC_MapLiteral(ctx *parser.OC_MapLiteralContext) {
	s.ctx.Enter(NewMapLiteralVisitor())
}

func (s *PropertiesVisitor) ExitOC_MapLiteral(ctx *parser.OC_MapLiteralContext) {
	s.Properties.Map = s.ctx.Exit().(*MapLiteralVisitor).Map
}

func (s *PropertiesVisitor) EnterOC_Parameter(ctx *parser.OC_ParameterContext) {
	s.ctx.Enter(&SymbolicNameOrReservedWordVisitor{})
}

func (s *PropertiesVisitor) ExitOC_Parameter(ctx *parser.OC_ParameterContext) {
	s.Properties.Parameter = &cypher.Parameter{
		Symbol: extractParameterSymbol(s.ctx, ctx),
	}
}

type ExpressionVisitor struct {
	BaseVisitor

	Expression cypher.Expression
}

func NewExpressionVisitor() Visitor {
	return &ExpressionVisitor{}
}

func (s *ExpressionVisitor) EnterOC_NonArithmeticOperatorExpression(ctx *parser.OC_NonArithmeticOperatorExpressionContext) {
	s.ctx.Enter(&NonArithmeticOperatorExpressionVisitor{})
}

func (s *ExpressionVisitor) ExitOC_NonArithmeticOperatorExpression(ctx *parser.OC_NonArithmeticOperatorExpressionContext) {
	s.Expression = s.ctx.Exit().(*NonArithmeticOperatorExpressionVisitor).Expression
}

func (s *ExpressionVisitor) EnterOC_OrExpression(ctx *parser.OC_OrExpressionContext) {
	if len(ctx.AllOR()) > 0 {
		s.ctx.Enter(&JoiningVisitor{
			Joined: &cypher.Disjunction{},
		})
	}
}

func (s *ExpressionVisitor) ExitOC_OrExpression(ctx *parser.OC_OrExpressionContext) {
	if len(ctx.AllOR()) > 0 {
		visitor := s.ctx.Exit().(*JoiningVisitor)
		s.Expression = visitor.Joined
	}
}

func (s *ExpressionVisitor) EnterOC_XorExpression(ctx *parser.OC_XorExpressionContext) {
	if len(ctx.AllXOR()) > 0 {
		s.ctx.Enter(&JoiningVisitor{
			Joined: &cypher.ExclusiveDisjunction{},
		})
	}
}

func (s *ExpressionVisitor) ExitOC_XorExpression(ctx *parser.OC_XorExpressionContext) {
	if len(ctx.AllXOR()) > 0 {
		visitor := s.ctx.Exit().(*JoiningVisitor)
		s.Expression = visitor.Joined
	}
}

func (s *ExpressionVisitor) EnterOC_AndExpression(ctx *parser.OC_AndExpressionContext) {
	if len(ctx.AllAND()) > 0 {
		s.ctx.Enter(&JoiningVisitor{
			Joined: &cypher.Conjunction{},
		})
	}
}

func (s *ExpressionVisitor) ExitOC_AndExpression(ctx *parser.OC_AndExpressionContext) {
	if len(ctx.AllAND()) > 0 {
		visitor := s.ctx.Exit().(*JoiningVisitor)
		s.Expression = visitor.Joined
	}
}

func (s *ExpressionVisitor) EnterOC_ComparisonExpression(ctx *parser.OC_ComparisonExpressionContext) {
	if len(ctx.AllOC_PartialComparisonExpression()) > 0 {
		s.ctx.Enter(&ComparisonVisitor{})
	}
}

func (s *ExpressionVisitor) ExitOC_ComparisonExpression(ctx *parser.OC_ComparisonExpressionContext) {
	if len(ctx.AllOC_PartialComparisonExpression()) > 0 {
		result := s.ctx.Exit().(*ComparisonVisitor).Comparison
		s.Expression = result
	}
}

func (s *ExpressionVisitor) EnterOC_NotExpression(ctx *parser.OC_NotExpressionContext) {
	if len(ctx.AllNOT()) > 0 {
		s.ctx.Enter(&NegationVisitor{
			Negation: &cypher.Negation{},
		})
	}
}

func (s *ExpressionVisitor) ExitOC_NotExpression(ctx *parser.OC_NotExpressionContext) {
	if len(ctx.AllNOT()) > 0 {
		visitor := s.ctx.Exit().(*NegationVisitor)
		s.Expression = visitor.Negation
	}
}

func (s *ExpressionVisitor) EnterOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	s.ctx.Enter(&StringListNullPredicateExpressionVisitor{})
}

func (s *ExpressionVisitor) ExitOC_StringListNullPredicateExpression(ctx *parser.OC_StringListNullPredicateExpressionContext) {
	s.Expression = s.ctx.Exit().(*StringListNullPredicateExpressionVisitor).Expression
}

type tokenLiteralIterator struct {
	tokens []string
	index  int
}

func newTokenLiteralIterator(astNode TokenProvider) *tokenLiteralIterator {
	var tokens []string

	for idx := 0; idx < astNode.GetChildCount(); idx++ {
		nextChild := astNode.GetChild(idx)

		if terminalNode, typeOK := nextChild.(*antlr.TerminalNodeImpl); typeOK {
			formattedTerminalNodeText := strings.TrimSpace(terminalNode.GetText())

			if len(formattedTerminalNodeText) > 0 {
				tokens = append(tokens, formattedTerminalNodeText)
			}
		}
	}

	return &tokenLiteralIterator{
		tokens: tokens,
		index:  0,
	}
}

func (s *tokenLiteralIterator) HasTokens() bool {
	numTokens := len(s.tokens)
	return numTokens > 0 && s.index < numTokens
}

func (s *tokenLiteralIterator) NextToken() string {
	var nextToken string

	if s.index < len(s.tokens) {
		nextToken = s.tokens[s.index]
		s.index++
	}

	return nextToken
}

func (s *tokenLiteralIterator) NextOperator() cypher.Operator {
	nextOperator, _ := cypher.ParseOperator(s.NextToken())
	return nextOperator
}

type ArithmeticExpressionVisitor struct {
	BaseVisitor

	operatorTokens *tokenLiteralIterator
	Expression     cypher.Expression
}

func NewArithmeticExpressionVisitor(operatorTokens *tokenLiteralIterator) *ArithmeticExpressionVisitor {
	return &ArithmeticExpressionVisitor{
		operatorTokens: operatorTokens,
	}
}

func (s *ArithmeticExpressionVisitor) assignExpression(expression cypher.Expression) {
	if s.operatorTokens.HasTokens() {
		// If there's no expression in this visitor but there are collected operators then this AST node
		// is the left-most expression of the arithmetic operator expression
		if s.Expression == nil {
			s.Expression = &cypher.ArithmeticExpression{
				Left: expression,
			}
		} else {
			// This is a subsequent component of the arithmetic operator expression
			s.Expression.(*cypher.ArithmeticExpression).AddArithmeticExpressionPart(&cypher.PartialArithmeticExpression{
				Operator: s.operatorTokens.NextOperator(),
				Right:    expression,
			})
		}
	} else {
		// With no collected operators then it's assumed that this is purely a non-arithmetic operator expression
		s.Expression = expression
	}
}

func (s *ArithmeticExpressionVisitor) enterSubArithmeticExpression(astNode TokenProvider) {
	if operators := newTokenLiteralIterator(astNode); operators.HasTokens() {
		s.ctx.Enter(NewArithmeticExpressionVisitor(operators))
	}
}

func (s *ArithmeticExpressionVisitor) exitSubArithmeticExpression(astNode TokenProvider) {
	if operators := newTokenLiteralIterator(astNode); operators.HasTokens() {
		s.assignExpression(s.ctx.Exit().(*ArithmeticExpressionVisitor).Expression)
	}
}

func (s *ArithmeticExpressionVisitor) EnterOC_MultiplyDivideModuloExpression(ctx *parser.OC_MultiplyDivideModuloExpressionContext) {
	s.enterSubArithmeticExpression(ctx)
}

func (s *ArithmeticExpressionVisitor) ExitOC_MultiplyDivideModuloExpression(ctx *parser.OC_MultiplyDivideModuloExpressionContext) {
	s.exitSubArithmeticExpression(ctx)
}

func (s *ArithmeticExpressionVisitor) EnterOC_PowerOfExpression(ctx *parser.OC_PowerOfExpressionContext) {
	s.enterSubArithmeticExpression(ctx)
}

func (s *ArithmeticExpressionVisitor) ExitOC_PowerOfExpression(ctx *parser.OC_PowerOfExpressionContext) {
	s.exitSubArithmeticExpression(ctx)
}

func (s *ArithmeticExpressionVisitor) EnterOC_UnaryAddOrSubtractExpression(ctx *parser.OC_UnaryAddOrSubtractExpressionContext) {
	s.enterSubArithmeticExpression(ctx)
}

func (s *ArithmeticExpressionVisitor) ExitOC_UnaryAddOrSubtractExpression(ctx *parser.OC_UnaryAddOrSubtractExpressionContext) {
	if operators := newTokenLiteralIterator(ctx); operators.HasTokens() {
		s.assignExpression(&cypher.UnaryAddOrSubtractExpression{
			Operator: operators.NextOperator(),
			Right:    s.ctx.Exit().(*ArithmeticExpressionVisitor).Expression,
		})
	}
}

func (s *ArithmeticExpressionVisitor) EnterOC_NonArithmeticOperatorExpression(ctx *parser.OC_NonArithmeticOperatorExpressionContext) {
	s.ctx.Enter(&NonArithmeticOperatorExpressionVisitor{})
}

func (s *ArithmeticExpressionVisitor) ExitOC_NonArithmeticOperatorExpression(ctx *parser.OC_NonArithmeticOperatorExpressionContext) {
	s.assignExpression(s.ctx.Exit().(*NonArithmeticOperatorExpressionVisitor).Expression)
}

// StringListNullPredicateExpressionVisitor
// oC_AddOrSubtractExpression ( oC_StringPredicateExpression | oC_ListPredicateExpression | oC_NullPredicateExpression )*
type StringListNullPredicateExpressionVisitor struct {
	BaseVisitor

	Expression cypher.Expression
}

func (s *StringListNullPredicateExpressionVisitor) EnterOC_AddOrSubtractExpression(ctx *parser.OC_AddOrSubtractExpressionContext) {
	s.ctx.Enter(NewArithmeticExpressionVisitor(newTokenLiteralIterator(ctx)))
}

func (s *StringListNullPredicateExpressionVisitor) ExitOC_AddOrSubtractExpression(ctx *parser.OC_AddOrSubtractExpressionContext) {
	expression := s.ctx.Exit().(*ArithmeticExpressionVisitor).Expression

	switch typedExpression := s.Expression.(type) {
	case *cypher.Comparison:
		typedExpression.LastPartial().Right = expression

	default:
		s.Expression = expression
	}
}

func (s *StringListNullPredicateExpressionVisitor) EnterOC_RegularExpression(ctx *parser.OC_RegularExpressionContext) {
	s.Expression = &cypher.Comparison{
		Left: s.Expression,
		Partials: []*cypher.PartialComparison{{
			Operator: cypher.OperatorRegexMatch,
		}},
	}
}

func (s *StringListNullPredicateExpressionVisitor) EnterOC_ListPredicateExpression(ctx *parser.OC_ListPredicateExpressionContext) {
	if ctx.GetToken(parser.CypherLexerIN, 0) != nil {
		s.Expression = &cypher.Comparison{
			Left: s.Expression,
			Partials: []*cypher.PartialComparison{{
				Operator: cypher.OperatorIn,
			}},
		}
	}
}

func (s *StringListNullPredicateExpressionVisitor) EnterOC_NullPredicateExpression(ctx *parser.OC_NullPredicateExpressionContext) {
	literalNull := &cypher.Literal{
		Null: true,
	}

	if HasTokens(ctx, parser.CypherLexerIS) {
		if HasTokens(ctx, parser.CypherLexerNOT) {
			s.Expression = &cypher.Comparison{
				Left: s.Expression,
				Partials: []*cypher.PartialComparison{{
					Operator: cypher.OperatorIsNot,
					Right:    literalNull,
				}},
			}
		} else {
			s.Expression = &cypher.Comparison{
				Left: s.Expression,
				Partials: []*cypher.PartialComparison{{
					Operator: cypher.OperatorIs,
					Right:    literalNull,
				}},
			}
		}
	}
}

func (s *StringListNullPredicateExpressionVisitor) EnterOC_StringPredicateExpression(ctx *parser.OC_StringPredicateExpressionContext) {
	if HasTokens(ctx, parser.CypherLexerSTARTS, parser.CypherLexerWITH) {
		s.Expression = &cypher.Comparison{
			Left: s.Expression,
			Partials: []*cypher.PartialComparison{{
				Operator: cypher.OperatorStartsWith,
			}},
		}
	} else if HasTokens(ctx, parser.CypherLexerENDS, parser.CypherLexerWITH) {
		s.Expression = &cypher.Comparison{
			Left: s.Expression,
			Partials: []*cypher.PartialComparison{{
				Operator: cypher.OperatorEndsWith,
			}},
		}
	} else if HasTokens(ctx, parser.CypherLexerCONTAINS) {
		s.Expression = &cypher.Comparison{
			Left: s.Expression,
			Partials: []*cypher.PartialComparison{{
				Operator: cypher.OperatorContains,
			}},
		}
	}
}

// oC_Atom ( ( SP? oC_ListOperatorExpression ) | ( SP? oC_PropertyLookup ) )* ( SP? oC_NodeLabels )?
type NonArithmeticOperatorExpressionVisitor struct {
	BaseVisitor

	Expression      cypher.Expression
	PropertyKeyName string
}

func (s *NonArithmeticOperatorExpressionVisitor) EnterOC_NodeLabels(ctx *parser.OC_NodeLabelsContext) {
	s.Expression = &cypher.KindMatcher{
		Reference: s.Expression,
	}
}

func (s *NonArithmeticOperatorExpressionVisitor) EnterOC_NodeLabel(ctx *parser.OC_NodeLabelContext) {
	s.ctx.Enter(&SymbolicNameOrReservedWordVisitor{})
}

func (s *NonArithmeticOperatorExpressionVisitor) ExitOC_NodeLabel(ctx *parser.OC_NodeLabelContext) {
	matcher := s.Expression.(*cypher.KindMatcher)
	matcher.Kinds = append(matcher.Kinds, graph.StringKind(s.ctx.Exit().(*SymbolicNameOrReservedWordVisitor).Name))
}

func (s *NonArithmeticOperatorExpressionVisitor) EnterOC_Atom(ctx *parser.OC_AtomContext) {
	if !HasTokens(ctx, parser.CypherLexerCOUNT) {
		s.ctx.Enter(NewAtomVisitor())
	}
}

func (s *NonArithmeticOperatorExpressionVisitor) ExitOC_Atom(ctx *parser.OC_AtomContext) {
	if HasTokens(ctx, parser.CypherLexerCOUNT) {
		s.Expression = &cypher.FunctionInvocation{
			Name:      "count",
			Arguments: []cypher.Expression{cypher.GreedyRangeQuantifier},
		}
	} else {
		s.Expression = s.ctx.Exit().(*AtomVisitor).Atom
	}
}

func (s *NonArithmeticOperatorExpressionVisitor) EnterOC_PropertyLookup(ctx *parser.OC_PropertyLookupContext) {
	s.Expression = &cypher.PropertyLookup{
		Atom: s.Expression,
	}
}

func (s *NonArithmeticOperatorExpressionVisitor) ExitOC_PropertyLookup(ctx *parser.OC_PropertyLookupContext) {
	s.Expression.(*cypher.PropertyLookup).SetSymbol(s.PropertyKeyName)
}

func (s *NonArithmeticOperatorExpressionVisitor) EnterOC_PropertyKeyName(ctx *parser.OC_PropertyKeyNameContext) {
	s.ctx.Enter(&SymbolicNameOrReservedWordVisitor{})
}

func (s *NonArithmeticOperatorExpressionVisitor) ExitOC_PropertyKeyName(ctx *parser.OC_PropertyKeyNameContext) {
	s.PropertyKeyName = s.ctx.Exit().(*SymbolicNameOrReservedWordVisitor).Name
}
