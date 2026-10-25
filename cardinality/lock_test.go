package cardinality

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The DAWGS-099 regressions pin cross-wrapper lock-order behavior for
// ThreadSafeDuplex and ThreadSafeSimplex. Before the detach repair, binary
// operations acquired the receiver's lock and then read the other provider
// through its own lock (Each/Contains), so opposing operations on two wrappers
// deadlocked and self-operations re-acquired the receiver's lock. Wrapped
// HyperLogLog merges also dispatched to no implementation branch and silently
// dropped the other sketch.

const deadlockGuard = 10 * time.Second

// runWithDeadlockGuard executes operation in a goroutine and fails the test if
// it does not complete before the guard expires. The goroutine is intentionally
// left blocked when the operation deadlocks; a failing run exits through
// t.Fatal while a passing run always completes the operation.
func runWithDeadlockGuard(t *testing.T, name string, operation func()) {
	t.Helper()

	completed := make(chan struct{})

	go func() {
		defer close(completed)
		operation()
	}()

	select {
	case <-completed:
	case <-time.After(deadlockGuard):
		t.Fatalf("%s did not complete within %s: lock-order deadlock", name, deadlockGuard)
	}
}

// gatedDuplex wraps a real Bitmap64 and gates its Or implementation only when
// the argument is still another threadSafeDuplex wrapper. Before the repair
// that is exactly what the receiver's inner provider sees, so the gate
// deterministically positions both opposing Or calls inside the window where
// each wrapper holds its own lock and is about to acquire the other's. After
// the repair the argument is already a detached lock-free snapshot, the gate
// never fires and both operations complete.
type gatedDuplex struct {
	inner   Duplex[uint64]
	arrived chan struct{}
	release chan struct{}
}

func (g gatedDuplex) Clear() {
	g.inner.Clear()
}

func (g gatedDuplex) Add(values ...uint64) {
	g.inner.Add(values...)
}

func (g gatedDuplex) Remove(value uint64) {
	g.inner.Remove(value)
}

func (g gatedDuplex) Contains(value uint64) bool {
	return g.inner.Contains(value)
}

func (g gatedDuplex) CheckedAdd(value uint64) bool {
	return g.inner.CheckedAdd(value)
}

func (g gatedDuplex) Cardinality() uint64 {
	return g.inner.Cardinality()
}

func (g gatedDuplex) Slice() []uint64 {
	return g.inner.Slice()
}

func (g gatedDuplex) Each(delegate func(value uint64) bool) {
	g.inner.Each(delegate)
}

func (g gatedDuplex) Clone() Duplex[uint64] {
	// Snapshots of this provider are ungated real bitmaps.
	return g.inner.Clone()
}

func (g gatedDuplex) Or(other Provider[uint64]) {
	if _, isWrapper := other.(threadSafeDuplex[uint64]); isWrapper {
		close(g.arrived)
		<-g.release
	}

	if typedOther, ok := other.(Duplex[uint64]); ok {
		typedOther.Each(func(nextValue uint64) bool {
			g.inner.Add(nextValue)
			return true
		})
	}
}

func (g gatedDuplex) And(other Provider[uint64]) {
	g.inner.And(other)
}

func (g gatedDuplex) AndNot(other Provider[uint64]) {
	g.inner.AndNot(other)
}

func (g gatedDuplex) Xor(other Provider[uint64]) {
	g.inner.Xor(other)
}

func newGatedDuplexPair(t *testing.T) (a, b Duplex[uint64], arrivedA, arrivedB, releaseA, releaseB chan struct{}) {
	t.Helper()

	arrivedA = make(chan struct{})
	arrivedB = make(chan struct{})
	releaseA = make(chan struct{})
	releaseB = make(chan struct{})

	a = ThreadSafeDuplex(gatedDuplex{
		inner:   NewBitmap64With(1, 2, 3),
		arrived: arrivedA,
		release: releaseA,
	})

	b = ThreadSafeDuplex(gatedDuplex{
		inner:   NewBitmap64With(3, 4, 5),
		arrived: arrivedB,
		release: releaseB,
	})

	return a, b, arrivedA, arrivedB, releaseA, releaseB
}

func TestThreadSafeDuplexOpposingOrCompletes(t *testing.T) {
	a, b, arrivedA, arrivedB, releaseA, releaseB := newGatedDuplexPair(t)

	var (
		completions = make(chan struct{}, 2)
		operations  = sync.WaitGroup{}
	)

	operations.Add(2)

	go func() {
		defer operations.Done()
		a.Or(b)
		completions <- struct{}{}
	}()

	go func() {
		defer operations.Done()
		b.Or(a)
		completions <- struct{}{}
	}()

	var (
		aArrived bool
		bArrived bool
		released bool
		finished int
		timer    = time.NewTimer(deadlockGuard)
	)

	defer timer.Stop()

	for finished < 2 {
		select {
		case <-arrivedA:
			aArrived = true
		case <-arrivedB:
			bArrived = true
		case <-completions:
			finished++
		case <-timer.C:
			t.Fatalf("opposing cross-wrapper Or operations did not complete within %s", deadlockGuard)
		}

		if aArrived && bArrived && !released {
			// Both wrappers hold their own locks inside the gated window;
			// release them together so each demands the other's lock at once.
			released = true
			close(releaseA)
			close(releaseB)
		}
	}

	operations.Wait()

	// The unions are interleaving independent: each wrapper always contains its
	// own values plus everything the other side ever held.
	require.ElementsMatch(t, []uint64{1, 2, 3, 4, 5}, a.Slice())
	require.ElementsMatch(t, []uint64{1, 2, 3, 4, 5}, b.Slice())
}

func TestThreadSafeDuplexSelfOperationCompletes(t *testing.T) {
	tests := []struct {
		name      string
		operation func(duplex Duplex[uint64])
		expected  []uint64
	}{
		{
			name:      "Or",
			operation: func(duplex Duplex[uint64]) { duplex.Or(duplex) },
			expected:  []uint64{1, 2, 3},
		},
		{
			name:      "And",
			operation: func(duplex Duplex[uint64]) { duplex.And(duplex) },
			expected:  []uint64{1, 2, 3},
		},
		{
			name:      "Xor",
			operation: func(duplex Duplex[uint64]) { duplex.Xor(duplex) },
			expected:  []uint64{},
		},
		{
			name:      "AndNot",
			operation: func(duplex Duplex[uint64]) { duplex.AndNot(duplex) },
			expected:  []uint64{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			duplex := ThreadSafeDuplex(NewBitmap64With(1, 2, 3))

			runWithDeadlockGuard(t, "self "+test.name, func() {
				test.operation(duplex)
			})

			require.ElementsMatch(t, test.expected, duplex.Slice())
		})
	}
}

func TestThreadSafeDuplexSelfOperationCompletes32(t *testing.T) {
	duplex := ThreadSafeDuplex(NewBitmap32With(1, 2, 3))

	runWithDeadlockGuard(t, "self Or 32-bit", func() {
		duplex.Or(duplex)
	})

	require.ElementsMatch(t, []uint32{1, 2, 3}, duplex.Slice())

	duplex = ThreadSafeDuplex(NewBitmap32With(1, 2, 3))

	runWithDeadlockGuard(t, "self AndNot 32-bit", func() {
		duplex.AndNot(duplex)
	})

	require.Empty(t, duplex.Slice())
}

func TestThreadSafeDuplexWrappedBinaryOperationResults(t *testing.T) {
	tests := []struct {
		name      string
		operation func(left, right Duplex[uint64])
		expected  []uint64
	}{
		{
			name:      "Or",
			operation: func(left, right Duplex[uint64]) { left.Or(right) },
			expected:  []uint64{1, 2, 3, 4},
		},
		{
			name:      "And",
			operation: func(left, right Duplex[uint64]) { left.And(right) },
			expected:  []uint64{2, 3},
		},
		{
			name:      "Xor",
			operation: func(left, right Duplex[uint64]) { left.Xor(right) },
			expected:  []uint64{1, 4},
		},
		{
			name:      "AndNot",
			operation: func(left, right Duplex[uint64]) { left.AndNot(right) },
			expected:  []uint64{1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left := ThreadSafeDuplex(NewBitmap64With(1, 2, 3))
			right := ThreadSafeDuplex(NewBitmap64With(2, 3, 4))

			test.operation(left, right)

			require.ElementsMatch(t, test.expected, left.Slice())
			require.ElementsMatch(t, []uint64{2, 3, 4}, right.Slice())
		})
	}
}

func TestThreadSafeDuplexUnwrappedArgumentsUnchanged(t *testing.T) {
	left := ThreadSafeDuplex(NewBitmap64With(1, 2, 3))

	// Unwrapped bitmap providers take their existing native paths.
	left.Or(NewBitmap64With(3, 4))
	require.ElementsMatch(t, []uint64{1, 2, 3, 4}, left.Slice())

	left.And(NewBitmap64With(2, 3, 4))
	require.ElementsMatch(t, []uint64{2, 3, 4}, left.Slice())

	left.AndNot(NewBitmap64With(3))
	require.ElementsMatch(t, []uint64{2, 4}, left.Slice())

	left.Xor(NewBitmap64With(2, 5))
	require.ElementsMatch(t, []uint64{4, 5}, left.Slice())
}

func TestThreadSafeDuplexCloneIsolation(t *testing.T) {
	duplex := ThreadSafeDuplex(NewBitmap64With(1, 2, 3))
	clone := duplex.Clone()

	duplex.Add(4)

	require.ElementsMatch(t, []uint64{1, 2, 3}, clone.Slice())
	require.ElementsMatch(t, []uint64{1, 2, 3, 4}, duplex.Slice())

	clone.Remove(1)
	require.ElementsMatch(t, []uint64{2, 3}, clone.Slice())

	var visited []uint64
	clone.Each(func(value uint64) bool {
		visited = append(visited, value)
		return true
	})
	require.ElementsMatch(t, []uint64{2, 3}, visited)

	clone.Clear()
	require.Empty(t, clone.Slice())

	require.True(t, clone.CheckedAdd(2))
	require.False(t, clone.CheckedAdd(2))
	require.ElementsMatch(t, []uint64{2}, clone.Slice())
}

func TestThreadSafeSimplexCloneIsolation(t *testing.T) {
	simplex := ThreadSafeSimplex(NewHyperLogLog64())
	simplex.Add(1, 2, 3)

	clone := simplex.Clone()
	simplex.Add(4)

	require.Equal(t, uint64(3), clone.Cardinality())

	clone.Clear()
	require.Equal(t, uint64(0), clone.Cardinality())
}

func TestThreadSafeDuplexConcurrentMixedOperations(t *testing.T) {
	const (
		goroutines = 8
		iterations = 100
	)

	var (
		left     = ThreadSafeDuplex(NewBitmap64With(1, 2, 3))
		right    = ThreadSafeDuplex(NewBitmap64With(2, 3, 4))
		deadline = sync.WaitGroup{}
	)

	deadline.Add(goroutines)

	runWithDeadlockGuard(t, "concurrent mixed wrapped operations", func() {
		for idx := 0; idx < goroutines; idx++ {
			go func(idx int) {
				defer deadline.Done()

				for iteration := 0; iteration < iterations; iteration++ {
					switch (idx + iteration) % 6 {
					case 0:
						left.Or(right)
					case 1:
						right.Or(left)
					case 2:
						left.Contains(uint64(iteration))
					case 3:
						right.Cardinality()
					case 4:
						left.Slice()
					case 5:
						right.Clone()
					}
				}
			}(idx)
		}

		deadline.Wait()
	})
}

func TestThreadSafeDuplexNestedWrapperSelfOperation(t *testing.T) {
	nested := ThreadSafeDuplex(ThreadSafeDuplex(NewBitmap64With(1, 2, 3)))

	runWithDeadlockGuard(t, "nested wrapper self Or", func() {
		nested.Or(nested)
	})

	require.ElementsMatch(t, []uint64{1, 2, 3}, nested.Slice())
}

func TestDetachProvider(t *testing.T) {
	t.Run("nil passthrough", func(t *testing.T) {
		require.Nil(t, detachProvider[uint64](nil))
	})

	t.Run("unwrapped provider identity", func(t *testing.T) {
		raw := NewBitmap64With(1)
		detached := detachProvider(raw)

		// Unwrapped providers are returned unchanged: later mutations remain
		// visible through the returned reference.
		raw.Add(2)
		require.True(t, detached.(Duplex[uint64]).Contains(2))
	})

	t.Run("wrapped duplex snapshot", func(t *testing.T) {
		wrapped := ThreadSafeDuplex(NewBitmap64With(1))
		detached := detachProvider(wrapped)

		require.True(t, detached.(Duplex[uint64]).Contains(1))

		// The snapshot is isolated from later mutation of the wrapper.
		wrapped.Add(2)
		require.False(t, detached.(Duplex[uint64]).Contains(2))
	})

	t.Run("wrapped duplex snapshot is native", func(t *testing.T) {
		wrapped := ThreadSafeDuplex(NewBitmap64With(1, 2, 3))
		detached := detachProvider(wrapped)

		// The snapshot is a plain bitmap provider, so subsequent binary
		// operations take native roaring paths instead of locked fallbacks.
		_, isNative := detached.(bitmap64)
		require.True(t, isNative)
	})

	t.Run("wrapped simplex snapshot", func(t *testing.T) {
		wrapped := ThreadSafeSimplex(NewHyperLogLog64())
		wrapped.Add(1, 2, 3)

		detached := detachProvider(wrapped)
		require.Equal(t, uint64(3), detached.Cardinality())

		wrapped.Add(4)
		require.Equal(t, uint64(3), detached.Cardinality())
	})

	t.Run("nested wrapped duplex self operation", func(t *testing.T) {
		nested := ThreadSafeDuplex(ThreadSafeDuplex(NewBitmap64With(1, 2, 3)))

		runWithDeadlockGuard(t, "nested wrapper self Or", func() {
			nested.Or(nested)
		})

		require.ElementsMatch(t, []uint64{1, 2, 3}, nested.Slice())
	})
}

func TestThreadSafeSimplexWrappedMerge(t *testing.T) {
	var (
		left  = ThreadSafeSimplex(NewHyperLogLog64())
		right = ThreadSafeSimplex(NewHyperLogLog64())
	)

	for value := uint64(0); value < 20000; value++ {
		if value < 10000 {
			left.Add(value)
		} else {
			right.Add(value)
		}
	}

	require.InDelta(t, uint64(10000), left.Cardinality(), 500)
	require.InDelta(t, uint64(10000), right.Cardinality(), 500)

	// Before the repair the wrapped sketches dispatched to no implementation
	// branch and the merge silently dropped the entire right-hand sketch.
	left.Or(right)

	require.InDelta(t, uint64(20000), left.Cardinality(), 500)
	require.InDelta(t, uint64(10000), right.Cardinality(), 500)
}

func TestThreadSafeSimplexWrappedMerge32(t *testing.T) {
	var (
		left  = ThreadSafeSimplex(NewHyperLogLog32())
		right = ThreadSafeSimplex(NewHyperLogLog32())
	)

	for value := uint32(0); value < 20000; value++ {
		if value < 10000 {
			left.Add(value)
		} else {
			right.Add(value)
		}
	}

	left.Or(right)

	require.InDelta(t, uint64(20000), left.Cardinality(), 500)
	require.InDelta(t, uint64(10000), right.Cardinality(), 500)
}

func TestThreadSafeSimplexUnwrappedMergeUnchanged(t *testing.T) {
	var (
		wrapped = ThreadSafeSimplex(NewHyperLogLog64())
		raw     = NewHyperLogLog64()
	)

	for value := uint64(0); value < 10000; value++ {
		wrapped.Add(value)
		raw.Add(value + 10000)
	}

	wrapped.Or(raw)

	require.InDelta(t, uint64(20000), wrapped.Cardinality(), 500)
}
