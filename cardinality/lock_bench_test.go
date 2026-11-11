package cardinality_test

import (
	"testing"

	"github.com/specterops/dawgs/cardinality"
)

// The DAWGS-099 snapshot repair adds a locked clone of wrapped binary
// operation arguments. These benchmarks record the cost of that snapshot
// against the previous direct fallback reads for wrapped operands of several
// sizes. Unwrapped bitmap operands take the native roaring paths in both
// versions and are included as an unchanged control.

func benchValues(size uint64) []uint64 {
	values := make([]uint64, size)
	for i := uint64(0); i < size; i++ {
		values[i] = i * 2
	}

	return values
}

func benchmarkWrappedOr(b *testing.B, size uint64) {
	values := benchValues(size)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		left := cardinality.ThreadSafeDuplex(cardinality.NewBitmap64With(1))
		right := cardinality.ThreadSafeDuplex(cardinality.NewBitmap64With(values...))
		left.Or(right)
	}
}

func benchmarkWrappedOrSelf(b *testing.B, size uint64) {
	values := benchValues(size)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		duplex := cardinality.ThreadSafeDuplex(cardinality.NewBitmap64With(values...))
		duplex.Or(duplex)
	}
}

func benchmarkUnwrappedOr(b *testing.B, size uint64) {
	values := benchValues(size)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		left := cardinality.ThreadSafeDuplex(cardinality.NewBitmap64With(1))
		right := cardinality.NewBitmap64With(values...)
		left.Or(right)
	}
}

func BenchmarkThreadSafeDuplexOrWrapped_10(b *testing.B) {
	benchmarkWrappedOr(b, 10)
}

func BenchmarkThreadSafeDuplexOrWrapped_1000(b *testing.B) {
	benchmarkWrappedOr(b, 1000)
}

func BenchmarkThreadSafeDuplexOrWrapped_100000(b *testing.B) {
	benchmarkWrappedOr(b, 100000)
}

func BenchmarkThreadSafeDuplexOrWrappedSelf_1000(b *testing.B) {
	benchmarkWrappedOrSelf(b, 1000)
}

func BenchmarkThreadSafeDuplexOrUnwrapped_1000(b *testing.B) {
	benchmarkUnwrappedOr(b, 1000)
}
