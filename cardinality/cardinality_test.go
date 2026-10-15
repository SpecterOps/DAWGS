package cardinality_test

import (
	"sort"
	"sync"
	"testing"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuplexToGraphIDs(t *testing.T) {
	uintIDs := []uint64{1, 2, 3, 4, 5}
	duplex := cardinality.NewBitmap64()
	duplex.Add(uintIDs...)

	ids := graph.DuplexToGraphIDs(duplex)

	for _, uintID := range uintIDs {
		found := false

		for _, id := range ids {
			if id.Uint64() == uintID {
				found = true
				break
			}
		}

		require.True(t, found)
	}
}

func TestNodeSetToDuplex(t *testing.T) {
	nodes := graph.NodeSet{
		1: &graph.Node{
			ID: 1,
		},
		2: &graph.Node{
			ID: 2,
		},
	}

	duplex := graph.NodeSetToDuplex(nodes)

	require.True(t, duplex.Contains(1))
	require.True(t, duplex.Contains(2))
}

// collectEach drains a Duplex via Each into a sorted slice.
func collectEach(duplex cardinality.Duplex[uint64]) []uint64 {
	var collected []uint64

	duplex.Each(func(value uint64) bool {
		collected = append(collected, value)
		return true
	})

	sort.Slice(collected, func(i, j int) bool { return collected[i] < collected[j] })

	return collected
}

func TestBitmap64Each(t *testing.T) {
	// Empty bitmap yields nothing.
	require.Empty(t, collectEach(cardinality.NewBitmap64()))

	// Values spanning multiple roaring containers are all returned, and repeated
	// calls return identical results (the iterator is pooled and reset per call).
	values := []uint64{0, 1, 5, 1 << 16, (1 << 16) + 3, 1 << 32, (1 << 32) + 7}
	duplex := cardinality.NewBitmap64With(values...)

	require.Equal(t, values, collectEach(duplex))
	require.Equal(t, values, collectEach(duplex))

	// Early termination stops iteration.
	var seen int
	duplex.Each(func(uint64) bool {
		seen++
		return false
	})
	require.Equal(t, 1, seen)
}

func TestBitmap64EachConcurrent(t *testing.T) {
	values := []uint64{0, 2, 4, 1 << 16, 1 << 32, (1 << 32) + 9}
	duplex := cardinality.NewBitmap64With(values...)

	var waitGroup sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 100; iteration++ {
				assert.Equal(t, values, collectEach(duplex))
			}
		}()
	}

	waitGroup.Wait()
}

func BenchmarkBitmap64Each(b *testing.B) {
	values := make([]uint64, 0, 4096)
	for value := uint64(0); value < 4096; value++ {
		values = append(values, value*3)
	}

	duplex := cardinality.NewBitmap64With(values...)

	var sink uint64
	delegate := func(value uint64) bool {
		sink += value
		return true
	}

	b.ReportAllocs()
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		duplex.Each(delegate)
	}

	_ = sink
}
