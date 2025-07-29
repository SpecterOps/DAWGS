package v1compat

import (
	"context"
	"errors"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
)

type KindsResult struct {
	ID    graph.ID
	Kinds graph.Kinds
}

type RelationshipTripleResult struct {
	ID      graph.ID
	StartID graph.ID
	EndID   graph.ID
}

type RelationshipKindsResult struct {
	RelationshipTripleResult
	Kind graph.Kind
}

type DirectionalResult struct {
	Direction    graph.Direction
	Relationship *graph.Relationship
	Node         *graph.Node
}

func NewDirectionalResult(direction graph.Direction, relationship *graph.Relationship, node *graph.Node) DirectionalResult {
	return DirectionalResult{
		Direction:    direction,
		Relationship: relationship,
		Node:         node,
	}
}

// Cursor is an interface that represents an active database operation. Cursors must be closed to prevent resource
// leaks.
type Cursor[T any] interface {
	// Error returns the error reference captured by this Result. When DAWGS database calls fail this value must be
	// populated by the underlying driver.
	Error() error

	// Close releases any active resources bound to this cursor
	Close()

	// Chan returns the type channel backed by this database cursor
	Chan() chan T
}

type ResultMarshaller[T any] func(scanner database.Result) (T, error)

type ResultIterator[T any] struct {
	ctx        context.Context
	result     database.Result
	cancelFunc func()
	valueC     chan T
	marshaller ResultMarshaller[T]
	error      error
}

func NewResultIterator[T any](ctx context.Context, result database.Result, marshaller ResultMarshaller[T]) Cursor[T] {
	var (
		cursorCtx, cancelFunc = context.WithCancel(ctx)
		resultIterator        = &ResultIterator[T]{
			ctx:        cursorCtx,
			cancelFunc: cancelFunc,
			result:     result,
			valueC:     make(chan T),
			marshaller: marshaller,
		}
	)

	resultIterator.start()
	return resultIterator
}

func (s *ResultIterator[T]) start() {
	go func() {
		defer close(s.valueC)

		for s.result.HasNext(s.ctx) {
			if nextValue, err := s.marshaller(s.result); err != nil {
				s.error = err
				break
			} else if !channels.Submit(s.ctx, s.valueC, nextValue) {
				s.error = ErrContextTimedOut
				break
			}
		}
	}()
}

func (s *ResultIterator[T]) Error() error {
	if resultErr := s.result.Error(); resultErr != nil {
		if s.error != nil {
			return errors.Join(resultErr, s.error)
		}

		return resultErr
	}

	return s.error
}

func (s *ResultIterator[T]) Close() {
	s.cancelFunc()
	s.result.Close(s.ctx)
}

func (s *ResultIterator[T]) Chan() chan T {
	return s.valueC
}
