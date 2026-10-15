package cardinality

import (
	"runtime"
	"testing"
	"weak"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/require"
)

// iterateAndRelease builds a bitmap, traverses it with Each and returns only a weak reference so that the caller holds
// no strong reference to the bitmap.
func iterateAndRelease(delegate func(uint64) bool) weak.Pointer[roaring64.Bitmap] {
	duplex := NewBitmap64With(0, 1, 1<<16, 1<<32, (1<<32)+1).(bitmap64)
	duplex.Each(delegate)

	return weak.Make(duplex.bitmap)
}

func TestBitmap64EachDoesNotRetainBitmap(t *testing.T) {
	// Full traversal and early termination both return the iterator to the pool.
	fullTraversal := iterateAndRelease(func(uint64) bool { return true })
	earlyTermination := iterateAndRelease(func(uint64) bool { return false })

	// A single GC cycle must reclaim the bitmaps. A pooled iterator that still references a bitmap keeps it reachable
	// until the pool's victim cache is cleared on a second GC cycle.
	runtime.GC()

	require.Nil(t, fullTraversal.Value())
	require.Nil(t, earlyTermination.Value())
}
