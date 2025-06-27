package walk

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

type VisitorHandler interface {
	Consume()
	WasConsumed() bool
	Done() bool
	Error() error
	SetDone()
	SetError(err error)
	SetErrorf(format string, args ...any)
}

type Visitor[N any] interface {
	VisitorHandler

	Enter(node N)
	Visit(node N)
	Exit(node N)
}

type cancelableVisitorHandler struct {
	currentSyntaxNodeConsumed bool
	done                      bool
	errs                      []error
}

func (s *cancelableVisitorHandler) Done() bool {
	return s.done
}

func (s *cancelableVisitorHandler) SetDone() {
	s.done = true
}

func (s *cancelableVisitorHandler) SetError(err error) {
	if err != nil {
		s.errs = append(s.errs, err)
		s.done = true
	}
}

func (s *cancelableVisitorHandler) SetErrorf(format string, args ...any) {
	s.SetError(fmt.Errorf(format, args...))
}

func (s *cancelableVisitorHandler) Error() error {
	return errors.Join(s.errs...)
}

func (s *cancelableVisitorHandler) Consume() {
	s.currentSyntaxNodeConsumed = true
}

func (s *cancelableVisitorHandler) WasConsumed() bool {
	consumed := s.currentSyntaxNodeConsumed
	s.currentSyntaxNodeConsumed = false

	return consumed
}

func NewCancelableErrorHandler() VisitorHandler {
	return &cancelableVisitorHandler{}
}

type composableVisitor[N any] struct {
	VisitorHandler
}

func (s *composableVisitor[N]) Enter(node N) {
}

func (s *composableVisitor[N]) Visit(node N) {
}

func (s *composableVisitor[N]) Exit(node N) {
}

func NewVisitor[E any]() Visitor[E] {
	return &composableVisitor[E]{
		VisitorHandler: NewCancelableErrorHandler(),
	}
}

type Order int

const (
	OrderPrefix Order = iota
	OrderInfix
	OrderPostfix
)

type SimpleVisitorFunc[N any] func(node N, visitorHandler VisitorHandler)

type simpleVisitor[N any] struct {
	Visitor[N]

	order       Order
	visitorFunc SimpleVisitorFunc[N]
}

func NewSimpleVisitor[N any](visitorFunc SimpleVisitorFunc[N]) Visitor[N] {
	return &simpleVisitor[N]{
		Visitor:     NewVisitor[N](),
		visitorFunc: visitorFunc,
	}
}

func (s *simpleVisitor[N]) Enter(node N) {
	if s.order == OrderPrefix {
		s.visitorFunc(node, s)
	}
}

func (s *simpleVisitor[N]) Visit(node N) {
	if s.order == OrderInfix {
		s.visitorFunc(node, s)
	}
}

func (s *simpleVisitor[N]) Exit(node N) {
	if s.order == OrderPostfix {
		s.visitorFunc(node, s)
	}
}

type Cursor[N any] struct {
	Node        N
	Branches    []N
	BranchIndex int
}

func (s *Cursor[N]) AddBranches(branches ...N) {
	s.Branches = append(s.Branches, branches...)
}

func (s *Cursor[N]) NumBranchesRemaining() int {
	return len(s.Branches) - s.BranchIndex
}

func (s *Cursor[N]) IsFirstVisit() bool {
	return s.BranchIndex == 0
}

func (s *Cursor[N]) HasNext() bool {
	return s.BranchIndex < len(s.Branches)
}

func (s *Cursor[N]) NextBranch() N {
	nextBranch := s.Branches[s.BranchIndex]
	s.BranchIndex += 1

	return nextBranch
}

func Generic[E any](node E, visitor Visitor[E], cursorConstructor func(node E) (*Cursor[E], error)) error {
	var stack []*Cursor[E]

	if cursor, err := cursorConstructor(node); err != nil {
		return err
	} else {
		stack = append(stack, cursor)
	}

	for len(stack) > 0 && !visitor.Done() {
		var (
			nextNode     = stack[len(stack)-1]
			isFirstVisit = nextNode.IsFirstVisit()
		)

		if isFirstVisit {
			visitor.Enter(nextNode.Node)

			if err := visitor.Error(); err != nil {
				return err
			}
		}

		if nextNode.HasNext() && !visitor.WasConsumed() {
			if !isFirstVisit {
				visitor.Visit(nextNode.Node)

				if err := visitor.Error(); err != nil {
					return err
				}
			}

			if cursor, err := cursorConstructor(nextNode.NextBranch()); err != nil {
				return err
			} else {
				stack = append(stack, cursor)
			}
		} else {
			visitor.Exit(nextNode.Node)

			if err := visitor.Error(); err != nil {
				return err
			}

			stack = stack[0 : len(stack)-1]
		}
	}

	return nil
}

func PgSQL(node pgsql.SyntaxNode, visitor Visitor[pgsql.SyntaxNode]) error {
	return Generic(node, visitor, newSQLWalkCursor)
}

func Cypher(node cypher.SyntaxNode, visitor Visitor[cypher.SyntaxNode]) error {
	return Generic(node, visitor, newCypherWalkCursor)
}
