package cardinality

import (
	"sync"
)

type threadSafeDuplex[T uint32 | uint64] struct {
	provider Duplex[T]
	lock     *sync.Mutex
}

// ThreadSafeDuplex synchronizes all operations of the wrapped Duplex with a
// single lock. Binary operations (Or, And, Xor, AndNot) snapshot another
// thread-safe wrapper before acquiring the receiver's lock, so opposing
// cross-wrapper operations and self-operations cannot form a lock-order cycle.
// Each operation is atomic for its receiver; there is no cross-operand
// atomicity, and a snapshot reflects the other wrapper only at the moment it
// was taken. Delegates passed to Each must not call back into the same wrapper.
func ThreadSafeDuplex[T uint32 | uint64](provider Duplex[T]) Duplex[T] {
	return threadSafeDuplex[T]{
		provider: provider,
		lock:     &sync.Mutex{},
	}
}

// detachProvider returns a lock-free snapshot of other when it is another
// thread-safe wrapper. Reading a wrapped provider directly would acquire its
// lock; doing that while holding the receiver's lock reintroduces the
// cross-wrapper lock-order cycle, and self-operations would acquire the same
// lock twice. The snapshot is taken before the receiver's lock is held, so
// wrapper locks are only ever acquired one at a time. Unwrapped providers are
// returned unchanged because they coordinate no locks with the receiver.
func detachProvider[T uint32 | uint64](other Provider[T]) Provider[T] {
	switch typedOther := other.(type) {
	case threadSafeDuplex[T]:
		typedOther.lock.Lock()
		defer typedOther.lock.Unlock()

		return typedOther.provider.Clone()

	case threadSafeSimplex[T]:
		typedOther.lock.Lock()
		defer typedOther.lock.Unlock()

		return typedOther.provider.Clone()

	default:
		return other
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
	snapshot := detachProvider(other)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.AndNot(snapshot)
}

func (s threadSafeDuplex[T]) Remove(value T) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Remove(value)
}

func (s threadSafeDuplex[T]) Xor(other Provider[T]) {
	snapshot := detachProvider(other)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Xor(snapshot)
}

func (s threadSafeDuplex[T]) And(other Provider[T]) {
	snapshot := detachProvider(other)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.And(snapshot)
}

func (s threadSafeDuplex[T]) Or(other Provider[T]) {
	snapshot := detachProvider(other)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Or(snapshot)
}

func (s threadSafeDuplex[T]) Cardinality() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.provider.Cardinality()
}

func (s threadSafeDuplex[T]) Slice() []T {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.provider.Slice()
}

func (s threadSafeDuplex[T]) Contains(value T) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.provider.Contains(value)
}

func (s threadSafeDuplex[T]) Each(delegate func(value T) bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Each(delegate)
}

func (s threadSafeDuplex[T]) CheckedAdd(value T) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.provider.CheckedAdd(value)
}

func (s threadSafeDuplex[T]) Clone() Duplex[T] {
	s.lock.Lock()
	defer s.lock.Unlock()

	return ThreadSafeDuplex(s.provider.Clone())
}

type threadSafeSimplex[T uint32 | uint64] struct {
	provider Simplex[T]
	lock     *sync.Mutex
}

// ThreadSafeSimplex synchronizes all operations of the wrapped Simplex with a
// single lock. Or snapshots another thread-safe wrapper before acquiring the
// receiver's lock, so cross-wrapper merges cannot form a lock-order cycle and
// merge with a wrapped sketch reaches its implementation instead of being
// silently dropped by provider dispatch. There is no cross-operand atomicity.
func ThreadSafeSimplex[T uint32 | uint64](provider Simplex[T]) Simplex[T] {
	return threadSafeSimplex[T]{
		provider: provider,
		lock:     &sync.Mutex{},
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
	snapshot := detachProvider(other)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.provider.Or(snapshot)
}

func (s threadSafeSimplex[T]) Cardinality() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.provider.Cardinality()
}

func (s threadSafeSimplex[T]) Clone() Simplex[T] {
	s.lock.Lock()
	defer s.lock.Unlock()

	return ThreadSafeSimplex(s.provider.Clone())
}
