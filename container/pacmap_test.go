package container_test

import (
	"testing"
	"time"

	"github.com/specterops/dawgs/container"
	"github.com/stretchr/testify/assert"
)

const pacmapFindGuard = 10 * time.Second

// getWithGuard executes pacmap.Get(key) and fails the test if the call does
// not return before the guard expires, mirroring the EFSet Find guard for
// lookups that previously underflowed the bucket's binary search.
func getWithGuard(t *testing.T, pacmap *container.PackedMap, key uint64) (uint64, bool) {
	t.Helper()

	type getResult struct {
		value uint64
		found bool
	}

	results := make(chan getResult, 1)

	go func() {
		value, found := pacmap.Get(key)
		results <- getResult{value: value, found: found}
	}()

	select {
	case result := <-results:
		return result.value, result.found
	case <-time.After(pacmapFindGuard):
		t.Fatalf("PackedMap.Get(%d) did not return within %s", key, pacmapFindGuard)
		return 0, false
	}
}

// varianceProbeParameters matches the recorded DAWGS-077 reproduction
// fixture: ten buckets with a key stride of one hundred.
func varianceProbeParameters() container.Parameters {
	return container.Parameters{
		BucketKeyStride: 100,
		NumMaxEntries:   100,
		NumBuckets:      10,
	}
}

func TestPacmap(t *testing.T) {
	pacmap := container.NewPacMap(container.DefaultParameters())

	for kv := uint64(0); kv < 10; kv++ {
		pacmap.Put(kv, kv)
	}

	pacmap.Compact()

	for kv := uint64(0); kv < 10; kv++ {
		fetched, hasValue := pacmap.Get(kv)

		assert.True(t, hasValue)
		assert.Equal(t, kv, fetched)
	}
}

// The DAWGS-077 merged-value regressions: merge() previously built a new
// values slice but assigned a reslice of the old bucket values instead, so
// pending values were lost and ranks from the merged key set indexed stale
// values.
func TestPacmapMergePreservesValuesAcrossCompactions(t *testing.T) {
	pacmap := container.NewPacMap(varianceProbeParameters())

	// The recorded trigger: Put (2,102),(3,103), Compact, Put (1,101),
	// Compact. Get(1) previously returned 102 and Get(3) indexed beyond the
	// stale values slice.
	pacmap.Put(2, 102)
	pacmap.Put(3, 103)
	pacmap.Compact()

	pacmap.Put(1, 101)
	pacmap.Compact()

	value, found := getWithGuard(t, pacmap, 1)
	assert.True(t, found)
	assert.Equal(t, uint64(101), value)

	value, found = getWithGuard(t, pacmap, 2)
	assert.True(t, found)
	assert.Equal(t, uint64(102), value)

	value, found = getWithGuard(t, pacmap, 3)
	assert.True(t, found)
	assert.Equal(t, uint64(103), value)

	// A further compaction inserting keys below, between and above the
	// compacted keys must retain every association.
	pacmap.Put(0, 100)
	pacmap.Put(5, 105)
	pacmap.Put(9, 109)
	pacmap.Compact()

	expected := map[uint64]uint64{
		0: 100,
		1: 101,
		2: 102,
		3: 103,
		5: 105,
		9: 109,
	}

	for key, wantValue := range expected {
		value, found := getWithGuard(t, pacmap, key)
		assert.True(t, found, "key %d must be found", key)
		assert.Equal(t, wantValue, value, "key %d must retain its value", key)
	}

	for _, missingKey := range []uint64{4, 6, 7, 8, 10, 11} {
		_, found := getWithGuard(t, pacmap, uint64(missingKey))
		assert.False(t, found, "key %d was never put", missingKey)
	}
}

func TestPacmapMergePreservesValuesAcrossBuckets(t *testing.T) {
	pacmap := container.NewPacMap(varianceProbeParameters())

	// Keys 0-99 share bucket zero; 100-199 share bucket one; a key at the
	// stride boundary lands in the following bucket.
	pacmap.Put(150, 250)
	pacmap.Put(190, 290)
	pacmap.Compact()

	pacmap.Put(100, 200)
	pacmap.Put(199, 299)
	pacmap.CompactParallel(2)

	expected := map[uint64]uint64{
		100: 200,
		150: 250,
		190: 290,
		199: 299,
	}

	for key, wantValue := range expected {
		value, found := getWithGuard(t, pacmap, key)
		assert.True(t, found, "key %d must be found", key)
		assert.Equal(t, wantValue, value, "key %d must retain its value", key)
	}

	_, found := getWithGuard(t, pacmap, 99)
	assert.False(t, found)
}

func TestPacmapGetBelowCompactedMinimumTerminates(t *testing.T) {
	pacmap := container.NewPacMap(varianceProbeParameters())

	pacmap.Put(10, 110)
	pacmap.Put(20, 120)
	pacmap.Put(30, 130)
	pacmap.Compact()

	// A bucket miss below the compacted minimum previously underflowed the
	// bucket's EFSet binary search and never returned.
	value, found := getWithGuard(t, pacmap, 5)
	assert.False(t, found)
	assert.Zero(t, value)

	_, found = getWithGuard(t, pacmap, 0)
	assert.False(t, found)

	// A Get on a bucket that was never allocated also terminates.
	_, found = getWithGuard(t, pacmap, 500)
	assert.False(t, found)
}
