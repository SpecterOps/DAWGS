package neo4j

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/query"
)

// ExpressionListRewriter contains rewriting logic related to folding Cypher syntax nodes along with additional
// guards for certain comparison checks.
type ExpressionListRewriter struct {
	walk.Visitor[cypher.SyntaxNode]

	descentStack []cypher.SyntaxNode
}

func NewExpressionListRewriter() walk.Visitor[cypher.SyntaxNode] {
	return &ExpressionListRewriter{
		Visitor: walk.NewVisitor[cypher.SyntaxNode](),
	}
}

func (s *ExpressionListRewriter) pushExpression(expression cypher.SyntaxNode) {
	s.descentStack = append(s.descentStack, expression)
}

func (s *ExpressionListRewriter) peekExpression() (cypher.SyntaxNode, bool) {
	if len(s.descentStack) == 0 {
		return nil, false
	}

	return s.descentStack[len(s.descentStack)-1], true
}

func (s *ExpressionListRewriter) peekExpressionList() (cypher.ExpressionList, bool) {
	if ancestorNode, hasPrevious := s.peekExpression(); hasPrevious {
		ancestorExpressionList, isExpressionList := ancestorNode.(cypher.ExpressionList)
		return ancestorExpressionList, isExpressionList
	}

	return nil, false
}

func (s *ExpressionListRewriter) hasNegationAncestor() bool {
	for idx := len(s.descentStack) - 1; idx >= 0; idx-- {
		if _, isNegation := s.descentStack[idx].(*cypher.Negation); isNegation {
			return true
		}
	}

	return false
}

func (s *ExpressionListRewriter) popExpression() {
	s.descentStack = s.descentStack[:len(s.descentStack)-1]
}

func unwrapParenthetical(expression cypher.SyntaxNode) cypher.SyntaxNode {
	cursor := expression

	for cursor != nil {
		switch typedCursor := cursor.(type) {
		case *cypher.Parenthetical:
			cursor = typedCursor.Expression
			continue
		}

		break
	}

	return cursor
}

func (s *ExpressionListRewriter) rewriteStringNegation(negation *cypher.Negation) {
	if ancestorExpressionList, isExpressionList := s.peekExpressionList(); isExpressionList {
		var (
			reference cypher.Expression
			operator  cypher.Operator
		)

		switch typedNegatedExpression := unwrapParenthetical(negation.Expression).(type) {
		case *cypher.Comparison:
			firstPartial := typedNegatedExpression.FirstPartial()
			if firstPartial == nil {
				return
			}

			reference = typedNegatedExpression.Left
			operator = firstPartial.Operator

		case *cypher.IndexableCaseInsensitiveStringPredicate:
			reference = typedNegatedExpression.Reference
			operator = typedNegatedExpression.Operator
		}

		// Neo4j comparison semantics for strings regarding `null` has edge cases that must be accounted for.
		switch operator {
		case cypher.OperatorStartsWith, cypher.OperatorEndsWith, cypher.OperatorContains:
			ancestorExpressionList.Replace(ancestorExpressionList.IndexOf(negation), &cypher.Parenthetical{
				Expression: cypher.NewDisjunction(
					negation,
					cypher.NewComparison(reference, cypher.OperatorIs, query.Literal(nil)),
				),
			})
		}
	}
}

func (s *ExpressionListRewriter) peekLastMatch() (*cypher.Match, bool) {
	for idx := len(s.descentStack) - 1; idx >= 0; idx-- {
		if lastMatch, typeOK := s.descentStack[idx].(*cypher.Match); typeOK {
			return lastMatch, typeOK
		}
	}

	return nil, false
}

func (s *ExpressionListRewriter) Enter(node cypher.SyntaxNode) {
	// Push after visiting the node to avoid having ancestor references pointing to the currently visited node
	s.pushExpression(node)
}

func (s *ExpressionListRewriter) Exit(node cypher.SyntaxNode) {
	attemptSelfRemoval := func() {
		if ancestorNode, hasPrevious := s.peekExpression(); hasPrevious {
			if ancestorExpressionList, isExpressionList := ancestorNode.(cypher.ExpressionList); isExpressionList {
				ancestorExpressionList.Remove(node)
			}
		}
	}

	s.popExpression()

	switch typedNode := node.(type) {
	case cypher.ExpressionList:
		if typedNode.Len() == 0 {
			// Remove emtpy cypher expression lists
			attemptSelfRemoval()
		}

	case *cypher.KindMatcher:
		if variable, typeOK := typedNode.Reference.(*cypher.Variable); !typeOK {
			s.SetErrorf("expected a variable as the reference for a kind matcher but received: %T", node)
		} else if variable.Symbol == query.EdgeSymbol {
			if s.hasNegationAncestor() {
				return
			}

			// We need to remove this expression from the most recent expression list and tack it onto the
			// relationship of the last match
			if lastMatch, hasLastMatch := s.peekLastMatch(); !hasLastMatch {
				s.SetErrorf("expected a match AST node")
			} else if ancestorExpressionList, isExpressionList := s.peekExpressionList(); !isExpressionList {
				s.SetErrorf("expected an expression list AST node")
			} else {
				firstRelationshipPattern := lastMatch.FirstRelationshipPattern()
				firstRelationshipPattern.Kinds = append(firstRelationshipPattern.Kinds, typedNode.Kinds...)

				ancestorExpressionList.Remove(node)
			}
		}

	case *cypher.Negation:
		s.rewriteStringNegation(typedNode)

	case *cypher.Parenthetical:
		switch typedParentheticalElement := typedNode.Expression.(type) {
		case cypher.ExpressionList:
			if numExpressions := typedParentheticalElement.Len(); numExpressions == 0 {
				attemptSelfRemoval()
			} else if numExpressions == 1 {
				// If the expression list has only one element, make it the only expression present in the
				// parenthetical expression
				typedNode.Expression = typedParentheticalElement.Get(0)
			}
		}
	}
}
