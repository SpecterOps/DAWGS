package cardinality

import (
	"sync"
)

type threadSafeDuplex[T uint32 | uint64] struct {
	provider Duplex[T]
	lock     *sync.RWMutex
}

func ThreadSafeDuplex[T uint32 | uint64](provider Duplex[T]) Duplex[T] {
	return threadSafeDuplex[T]{
		provider: provider,
		lock:     &sync.RWMutex{},
	}
}

func (s threadSafeDuplex[T]) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Clear()
}

func (s threadSafeDuplex[T]) Add(values ...T) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Add(values...)
}

func (s threadSafeDuplex[T]) AndNot(other Provider[T]) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.AndNot(other)
}

func (s threadSafeDuplex[T]) Remove(value T) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Remove(value)
}

func (s threadSafeDuplex[T]) Xor(other Provider[T]) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Xor(other)
}

func (s threadSafeDuplex[T]) And(other Provider[T]) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.And(other)
}

func (s threadSafeDuplex[T]) Or(other Provider[T]) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Or(other)
}

func (s threadSafeDuplex[T]) Cardinality() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.provider.Cardinality()
}

func (s threadSafeDuplex[T]) Slice() []T {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.provider.Slice()
}

func (s threadSafeDuplex[T]) Contains(value T) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.provider.Contains(value)
}

func (s threadSafeDuplex[T]) Each(delegate func(value T) bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	s.provider.Each(delegate)
}

func (s threadSafeDuplex[T]) CheckedAdd(value T) bool {
	if s.Contains(value) {
		return false
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	return s.provider.CheckedAdd(value)
}

func (s threadSafeDuplex[T]) Clone() Duplex[T] {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return ThreadSafeDuplex(s.provider.Clone())
}

type threadSafeSimplex[T uint32 | uint64] struct {
	provider Simplex[T]
	lock     *sync.RWMutex
}

func ThreadSafeSimplex[T uint32 | uint64](provider Simplex[T]) Simplex[T] {
	return threadSafeSimplex[T]{
		provider: provider,
		lock:     &sync.RWMutex{},
	}
}

func (s threadSafeSimplex[T]) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Clear()
}

func (s threadSafeSimplex[T]) Add(values ...T) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Add(values...)
}

func (s threadSafeSimplex[T]) Or(other Provider[T]) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Or(other)
}

func (s threadSafeSimplex[T]) Cardinality() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.provider.Cardinality()
}

func (s threadSafeSimplex[T]) Clone() Simplex[T] {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return ThreadSafeSimplex(s.provider.Clone())
}
