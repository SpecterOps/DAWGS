package cardinality_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/specterops/dawgs/cardinality"
	"github.com/stretchr/testify/require"
)

// The DAWGS-076 regressions pin the non-native Duplex fallbacks of bitmap
// And/AndNot. Before the repair, both operations iterated the receiver with
// Each and called Remove on it inside the delegate. Roaring iterators skip
// elements when the current value is removed mid-iteration, so
// {0..9}.And(ThreadSafeDuplex(empty)) left [1 3 5 7] and AndNot with a wrapped
// full set left the same values. The fallbacks now copy the operand into a
// native bitmap and apply the native operation, which never mutates the
// receiver during its own iteration.

// mapDuplex is a custom non-native Duplex implementation backed by a map. It
// exercises the generic fallback branches for user-supplied providers that are
// neither native bitmaps nor thread-safe wrappers.
type mapDuplex struct {
	values map[uint64]struct{}
}

func newMapDuplex(values ...uint64) *mapDuplex {
	duplex := &mapDuplex{values: make(map[uint64]struct{}, len(values))}
	duplex.Add(values...)
	return duplex
}

func (s *mapDuplex) Add(values ...uint64) {
	for _, value := range values {
		s.values[value] = struct{}{}
	}
}

func (s *mapDuplex) Or(other cardinality.Provider[uint64]) {
	if typedOther, ok := other.(cardinality.Duplex[uint64]); ok {
		typedOther.Each(func(value uint64) bool {
			s.values[value] = struct{}{}
			return true
		})
	}
}

func (s *mapDuplex) Clear() {
	s.values = make(map[uint64]struct{})
}

func (s *mapDuplex) Cardinality() uint64 {
	return uint64(len(s.values))
}

func (s *mapDuplex) Xor(other cardinality.Provider[uint64]) {
	if typedOther, ok := other.(cardinality.Duplex[uint64]); ok {
		typedOther.Each(func(value uint64) bool {
			if _, exists := s.values[value]; exists {
				delete(s.values, value)
			} else {
				s.values[value] = struct{}{}
			}
			return true
		})
	}
}

func (s *mapDuplex) And(other cardinality.Provider[uint64]) {
	if typedOther, ok := other.(cardinality.Duplex[uint64]); ok {
		kept := make(map[uint64]struct{}, len(s.values))

		typedOther.Each(func(value uint64) bool {
			if _, exists := s.values[value]; exists {
				kept[value] = struct{}{}
			}
			return true
		})

		s.values = kept
	}
}

func (s *mapDuplex) AndNot(other cardinality.Provider[uint64]) {
	if typedOther, ok := other.(cardinality.Duplex[uint64]); ok {
		typedOther.Each(func(value uint64) bool {
			delete(s.values, value)
			return true
		})
	}
}

func (s *mapDuplex) Remove(value uint64) {
	delete(s.values, value)
}

func (s *mapDuplex) Slice() []uint64 {
	slice := make([]uint64, 0, len(s.values))
	for value := range s.values {
		slice = append(slice, value)
	}
	return slice
}

func (s *mapDuplex) Contains(value uint64) bool {
	_, exists := s.values[value]
	return exists
}

func (s *mapDuplex) Each(delegate func(value uint64) bool) {
	for value := range s.values {
		if !delegate(value) {
			break
		}
	}
}

func (s *mapDuplex) CheckedAdd(value uint64) bool {
	if _, exists := s.values[value]; exists {
		return false
	}

	s.values[value] = struct{}{}
	return true
}

func (s *mapDuplex) Clone() cardinality.Duplex[uint64] {
	clone := newMapDuplex()
	for value := range s.values {
		clone.values[value] = struct{}{}
	}
	return clone
}

// valuesThrough returns the half-open range [0, stop).
func valuesThrough(stop uint64) []uint64 {
	values := make([]uint64, 0, stop)
	for value := uint64(0); value < stop; value++ {
		values = append(values, value)
	}
	return values
}

// boundaryValues returns values that cross roaring container boundaries
// (multiples of 2^16), the 32/64-bit range split, and high 64-bit ranges.
func boundaryValues() []uint64 {
	return []uint64{
		0, 1, 2,
		65534, 65535, 65536, 65537,
		131070, 131071, 131072, 131073,
		1 << 20, (1 << 20) + 1,
		1 << 32, (1 << 32) + 1, (1 << 32) + 65535, (1 << 32) + 65536,
		math.MaxUint64 - 1, math.MaxUint64,
	}
}

func wrapped(values ...uint64) cardinality.Duplex[uint64] {
	return cardinality.ThreadSafeDuplex(cardinality.NewBitmap64With(values...))
}

func TestBitmap64AndNonNativeFallback(t *testing.T) {
	tests := []struct {
		name          string
		receiver      []uint64
		operand       func() cardinality.Duplex[uint64]
		operandValues []uint64
		expectedAnd   []uint64
		expectedNot   []uint64
	}{
		{
			name:          "wrapped empty operand",
			receiver:      valuesThrough(10),
			operand:       func() cardinality.Duplex[uint64] { return wrapped() },
			operandValues: nil,
			expectedAnd:   []uint64{},
			expectedNot:   []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:          "wrapped full operand",
			receiver:      valuesThrough(10),
			operand:       func() cardinality.Duplex[uint64] { return wrapped(valuesThrough(10)...) },
			operandValues: valuesThrough(10),
			expectedAnd:   []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			expectedNot:   []uint64{},
		},
		{
			name:          "wrapped partial overlap",
			receiver:      valuesThrough(8),
			operand:       func() cardinality.Duplex[uint64] { return wrapped(5, 6, 7, 8, 9, 10) },
			operandValues: []uint64{5, 6, 7, 8, 9, 10},
			expectedAnd:   []uint64{5, 6, 7},
			expectedNot:   []uint64{0, 1, 2, 3, 4},
		},
		{
			name:          "wrapped disjoint operand",
			receiver:      valuesThrough(5),
			operand:       func() cardinality.Duplex[uint64] { return wrapped(10, 20, 30) },
			operandValues: []uint64{10, 20, 30},
			expectedAnd:   []uint64{},
			expectedNot:   []uint64{0, 1, 2, 3, 4},
		},
		{
			name:          "empty receiver with wrapped full operand",
			receiver:      nil,
			operand:       func() cardinality.Duplex[uint64] { return wrapped(valuesThrough(10)...) },
			operandValues: valuesThrough(10),
			expectedAnd:   []uint64{},
			expectedNot:   []uint64{},
		},
		{
			name:          "container boundary values",
			receiver:      boundaryValues(),
			operand:       func() cardinality.Duplex[uint64] { return wrapped(boundaryValues()[:len(boundaryValues())-3]...) },
			operandValues: boundaryValues()[:len(boundaryValues())-3],
			expectedAnd:   boundaryValues()[:len(boundaryValues())-3],
			expectedNot:   boundaryValues()[len(boundaryValues())-3:],
		},
		{
			name:          "custom non-native operand",
			receiver:      valuesThrough(10),
			operand:       func() cardinality.Duplex[uint64] { return newMapDuplex(4, 5, 6, 7, 20) },
			operandValues: []uint64{4, 5, 6, 7, 20},
			expectedAnd:   []uint64{4, 5, 6, 7},
			expectedNot:   []uint64{0, 1, 2, 3, 8, 9},
		},
	}

	for _, test := range tests {
		t.Run(test.name+" And", func(t *testing.T) {
			receiver := cardinality.NewBitmap64With(test.receiver...)
			operand := test.operand()

			receiver.And(operand)

			require.ElementsMatch(t, test.expectedAnd, receiver.Slice())
			require.ElementsMatch(t, test.operandValues, operand.Slice())
		})

		t.Run(test.name+" AndNot", func(t *testing.T) {
			receiver := cardinality.NewBitmap64With(test.receiver...)
			operand := test.operand()

			receiver.AndNot(operand)

			require.ElementsMatch(t, test.expectedNot, receiver.Slice())
			require.ElementsMatch(t, test.operandValues, operand.Slice())
		})
	}
}

func TestBitmap64NonNativeFallbackMatchesNative(t *testing.T) {
	const (
		valueCount = 100_000
		seed       = 1
	)

	// Two deterministic pseudo-random value sets with partial overlap, dense
	// enough to exercise bitmap containers and spanning multiple roaring
	// containers (100,000 values cross the 2^16 container boundary).
	leftValues := make([]uint64, 0, valueCount)
	rightValues := make([]uint64, 0, valueCount)

	state := seed
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return uint64(state>>16) % (valueCount * 2)
	}

	seenLeft := map[uint64]struct{}{}
	for len(leftValues) < valueCount {
		value := next()
		if _, exists := seenLeft[value]; !exists {
			seenLeft[value] = struct{}{}
			leftValues = append(leftValues, value)
		}
	}

	seenRight := map[uint64]struct{}{}
	for len(rightValues) < valueCount {
		value := next()
		if _, exists := seenRight[value]; !exists {
			seenRight[value] = struct{}{}
			rightValues = append(rightValues, value)
		}
	}

	nativeAnd := cardinality.NewBitmap64With(leftValues...)
	nativeAnd.And(cardinality.NewBitmap64With(rightValues...))

	nativeAndNot := cardinality.NewBitmap64With(leftValues...)
	nativeAndNot.AndNot(cardinality.NewBitmap64With(rightValues...))

	wrappedAnd := cardinality.NewBitmap64With(leftValues...)
	wrappedAnd.And(wrapped(rightValues...))
	require.ElementsMatch(t, nativeAnd.Slice(), wrappedAnd.Slice())

	wrappedAndNot := cardinality.NewBitmap64With(leftValues...)
	wrappedAndNot.AndNot(wrapped(rightValues...))
	require.ElementsMatch(t, nativeAndNot.Slice(), wrappedAndNot.Slice())

	customAnd := cardinality.NewBitmap64With(leftValues...)
	customAnd.And(newMapDuplex(rightValues...))
	require.ElementsMatch(t, nativeAnd.Slice(), customAnd.Slice())

	customAndNot := cardinality.NewBitmap64With(leftValues...)
	customAndNot.AndNot(newMapDuplex(rightValues...))
	require.ElementsMatch(t, nativeAndNot.Slice(), customAndNot.Slice())

	require.NotEmpty(t, nativeAnd.Slice())
	require.NotEmpty(t, nativeAndNot.Slice())
	require.Len(t, nativeAnd.Slice(), len(leftValues)-len(nativeAndNot.Slice()))
}

func TestBitmap64NativeOperandsUnchanged(t *testing.T) {
	receiver := cardinality.NewBitmap64With(1, 2, 3, 4)

	receiver.And(cardinality.NewBitmap64With(3, 4, 5))
	require.ElementsMatch(t, []uint64{3, 4}, receiver.Slice())

	receiver.AndNot(cardinality.NewBitmap64With(4))
	require.ElementsMatch(t, []uint64{3}, receiver.Slice())

	receiver.And(receiver)
	require.ElementsMatch(t, []uint64{3}, receiver.Slice())

	receiver.AndNot(receiver)
	require.Empty(t, receiver.Slice())
}

func TestBitmap32AndNonNativeFallback(t *testing.T) {
	values32 := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	full32 := cardinality.NewBitmap32With(values32...)

	tests := []struct {
		name        string
		operand     func() cardinality.Duplex[uint32]
		expectedAnd []uint32
		expectedNot []uint32
	}{
		{
			name:        "wrapped empty operand",
			operand:     func() cardinality.Duplex[uint32] { return cardinality.ThreadSafeDuplex(cardinality.NewBitmap32()) },
			expectedAnd: []uint32{},
			expectedNot: values32,
		},
		{
			name:        "wrapped full operand",
			operand:     func() cardinality.Duplex[uint32] { return cardinality.ThreadSafeDuplex(full32.Clone()) },
			expectedAnd: values32,
			expectedNot: []uint32{},
		},
		{
			name: "wrapped partial overlap",
			operand: func() cardinality.Duplex[uint32] {
				return cardinality.ThreadSafeDuplex(cardinality.NewBitmap32With(5, 6, 7, 8, 20))
			},
			expectedAnd: []uint32{5, 6, 7, 8},
			expectedNot: []uint32{0, 1, 2, 3, 4, 9},
		},
	}

	for _, test := range tests {
		t.Run(test.name+" And", func(t *testing.T) {
			receiver := cardinality.NewBitmap32With(values32...)

			receiver.And(test.operand())

			require.ElementsMatch(t, test.expectedAnd, receiver.Slice())
			require.Equal(t, uint64(len(test.expectedAnd)), receiver.Cardinality())

			for _, value := range test.expectedAnd {
				require.True(t, receiver.Contains(value))
			}
		})

		t.Run(test.name+" AndNot", func(t *testing.T) {
			receiver := cardinality.NewBitmap32With(values32...)

			receiver.AndNot(test.operand())

			require.ElementsMatch(t, test.expectedNot, receiver.Slice())
			require.Equal(t, uint64(len(test.expectedNot)), receiver.Cardinality())
		})
	}

	// Direct mutation and early-stopping iteration on the 32-bit bitmap.
	receiver := cardinality.NewBitmap32With(1, 2, 3)
	require.True(t, receiver.Contains(2))
	require.False(t, receiver.Contains(4))

	receiver.Remove(2)
	require.ElementsMatch(t, []uint32{1, 3}, receiver.Slice())

	var visited []uint32
	receiver.Each(func(value uint32) bool {
		visited = append(visited, value)
		return len(visited) < 1
	})
	require.Equal(t, []uint32{1}, visited)
}

func TestBitmap32NonNativeFallbackContainerBoundaries(t *testing.T) {
	// Values crossing roaring 32-bit container boundaries and the upper
	// 32-bit range. Before the repair the And fallback skipped every other
	// remaining element; the repaired fallback matches the native result.
	boundary := []uint32{
		0, 1, 2,
		65534, 65535, 65536, 65537,
		131070, 131071, 131072, 131073,
		1 << 20, (1 << 20) + 1,
		4294967294, 4294967295,
	}

	operand := cardinality.ThreadSafeDuplex(cardinality.NewBitmap32With(boundary[:len(boundary)-2]...))

	nativeAnd := cardinality.NewBitmap32With(boundary...)
	nativeAnd.And(cardinality.NewBitmap32With(boundary[:len(boundary)-2]...))

	nativeAndNot := cardinality.NewBitmap32With(boundary...)
	nativeAndNot.AndNot(cardinality.NewBitmap32With(boundary[:len(boundary)-2]...))

	fallbackAnd := cardinality.NewBitmap32With(boundary...)
	fallbackAnd.And(operand)
	require.ElementsMatch(t, nativeAnd.Slice(), fallbackAnd.Slice())

	fallbackAndNot := cardinality.NewBitmap32With(boundary...)
	fallbackAndNot.AndNot(operand)
	require.ElementsMatch(t, nativeAndNot.Slice(), fallbackAndNot.Slice())

	require.Len(t, fallbackAndNot.Slice(), 2)
}

func TestBitmap64OperandIsolatedFromReceiverMutation(t *testing.T) {
	// The wrapped operand must observe no mutation from the receiver's
	// operation: the fallback only reads the operand while building its copy.
	operand := wrapped(2, 3, 4)
	receiver := cardinality.NewBitmap64With(1, 2, 3, 4, 5)

	receiver.AndNot(operand)

	require.ElementsMatch(t, []uint64{1, 5}, receiver.Slice())
	require.ElementsMatch(t, []uint64{2, 3, 4}, operand.Slice())
	require.True(t, operand.Contains(3))

	operand.Add(6)
	require.True(t, operand.Contains(6))
}

func TestBitmap64FallbackLargeDenseReceiver(t *testing.T) {
	// A dense receiver spanning multiple containers must be fully consumed:
	// the pre-repair fallback left roughly every second element behind.
	values := valuesThrough(200_000)

	receiver := cardinality.NewBitmap64With(values...)
	receiver.And(wrapped())

	require.Empty(t, receiver.Slice())
	require.Equal(t, uint64(0), receiver.Cardinality())

	receiver = cardinality.NewBitmap64With(values...)
	receiver.AndNot(wrapped(values...))

	require.Empty(t, receiver.Slice())

	receiver = cardinality.NewBitmap64With(values...)
	receiver.And(wrapped(values...))

	require.Equal(t, uint64(200_000), receiver.Cardinality())
}

func Example_bitmap64AndWrappedEmpty() {
	bitmap := cardinality.NewBitmap64With(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	bitmap.And(cardinality.ThreadSafeDuplex(cardinality.NewBitmap64With(2, 4, 6)))

	fmt.Println(bitmap.Slice())
	// Output: [2 4 6]
}
