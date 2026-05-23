package translate

import (
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type collectIDMembershipUsage struct {
	membershipReferences int
	otherReferences      int
}

type collectIDMembershipCollector struct {
	walk.VisitorHandler
	candidates map[pgsql.Identifier]struct{}
	usages     map[pgsql.Identifier]*collectIDMembershipUsage
	stack      []cypher.SyntaxNode
}

func collectIDMembershipAliases(root *cypher.RegularQuery) (map[pgsql.Identifier]struct{}, error) {
	candidates, err := collectIDMembershipCandidates(root)
	if err != nil || len(candidates) == 0 {
		return nil, err
	}

	collector := &collectIDMembershipCollector{
		VisitorHandler: walk.NewCancelableErrorHandler(),
		candidates:     candidates,
		usages:         map[pgsql.Identifier]*collectIDMembershipUsage{},
	}
	if err := walk.Cypher(root, collector); err != nil {
		return nil, err
	}

	aliases := map[pgsql.Identifier]struct{}{}
	for alias := range candidates {
		usage := collector.usages[alias]
		if usage != nil && usage.membershipReferences > 0 && usage.otherReferences == 0 {
			aliases[alias] = struct{}{}
		}
	}
	return aliases, nil
}

func collectIDMembershipCandidates(root *cypher.RegularQuery) (map[pgsql.Identifier]struct{}, error) {
	candidates := map[pgsql.Identifier]struct{}{}

	err := walk.Cypher(root, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, handler walk.VisitorHandler) {
		projectionItem, isProjectionItem := node.(*cypher.ProjectionItem)
		if !isProjectionItem || projectionItem.Alias == nil {
			return
		}

		function, isFunction := projectionItem.Expression.(*cypher.FunctionInvocation)
		if !isFunction || !strings.EqualFold(function.Name, cypher.CollectFunction) || len(function.Arguments) != 1 {
			return
		}

		if _, isVariable := function.Arguments[0].(*cypher.Variable); isVariable {
			candidates[pgsql.Identifier(projectionItem.Alias.Symbol)] = struct{}{}
		}
	}))
	return candidates, err
}

func (s *collectIDMembershipCollector) usage(alias pgsql.Identifier) *collectIDMembershipUsage {
	usage := s.usages[alias]
	if usage == nil {
		usage = &collectIDMembershipUsage{}
		s.usages[alias] = usage
	}
	return usage
}

func (s *collectIDMembershipCollector) Enter(node cypher.SyntaxNode) {
	variable, isVariable := node.(*cypher.Variable)
	if !isVariable {
		s.stack = append(s.stack, node)
		return
	}

	alias := pgsql.Identifier(variable.Symbol)
	if _, isCandidate := s.candidates[alias]; isCandidate {
		usage := s.usage(alias)
		if s.isProjectionAliasDeclaration(variable) {
			// The alias declaration is not a read.
		} else if s.isMembershipCollectionOperand(variable) {
			usage.membershipReferences++
		} else {
			usage.otherReferences++
		}
	}

	s.stack = append(s.stack, node)
}

func (s *collectIDMembershipCollector) Visit(cypher.SyntaxNode) {}

func (s *collectIDMembershipCollector) Exit(cypher.SyntaxNode) {
	s.stack = s.stack[:len(s.stack)-1]
}

func (s *collectIDMembershipCollector) isProjectionAliasDeclaration(variable *cypher.Variable) bool {
	if len(s.stack) == 0 {
		return false
	}
	projectionItem, isProjectionItem := s.stack[len(s.stack)-1].(*cypher.ProjectionItem)
	return isProjectionItem && projectionItem.Alias == variable
}

func (s *collectIDMembershipCollector) isMembershipCollectionOperand(variable *cypher.Variable) bool {
	if len(s.stack) == 0 {
		return false
	}
	partial, isPartialComparison := s.stack[len(s.stack)-1].(*cypher.PartialComparison)
	return isPartialComparison && partial.Operator == cypher.OperatorIn && partial.Right == variable
}
