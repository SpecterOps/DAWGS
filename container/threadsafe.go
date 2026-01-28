package container

import (
	"sync"

	"github.com/gammazero/deque"
)

type ThreadSafeDeque[T any] struct {
	lock      *sync.RWMutex
	container deque.Deque[T]
}

func (s *ThreadSafeDeque[T]) PushFront(elem T) {
	s.lock.Lock()
	s.container.PushFront(elem)
	s.lock.Unlock()
}

func (s *ThreadSafeDeque[T]) PushBack(elem T) {
	s.lock.Lock()
	s.container.PushBack(elem)
	s.lock.Unlock()
}

func (s *ThreadSafeDeque[T]) PopFront() T {
	s.lock.Lock()
	value := s.container.PopFront()
	s.lock.Unlock()
	return value
}

func (s *ThreadSafeDeque[T]) PopBack() T {
	s.lock.Lock()
	value := s.container.PopBack()
	s.lock.Unlock()
	return value
}

func (s *ThreadSafeDeque[T]) Len() int {
	s.lock.RLock()
	numElements := s.container.Len()
	s.lock.RUnlock()
	return numElements
}
