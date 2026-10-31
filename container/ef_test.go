package container_test

import (
	"testing"
	"time"

	"github.com/specterops/dawgs/container"
	"github.com/stretchr/testify/assert"
)

// The DAWGS-077 regressions pin EFSet binary-search termination and rank
// correctness. Before the repair, a target below the stored minimum (or any
// lookup against an empty set) underflowed the unsigned high-entry bound to
// MaxUint64; Lookup then received out-of-range ranks and the loop never
// converged, so Find never returned.

const findGuard = 10 * time.Second

// findWithGuard executes set.Find(target) and fails the test if the call does
// not return before the guard expires. The goroutine is intentionally left
// blocked when the search fails to terminate; a failing run exits through
// t.Fatal while a passing run always collects the result.
func findWithGuard(t *testing.T, set *container.EFSet, target uint64) (uint, bool) {
	t.Helper()

	type findResult struct {
		rank  uint
		found bool
	}

	results := make(chan findResult, 1)

	go func() {
		rank, found := set.Find(target)
		results <- findResult{rank: rank, found: found}
	}()

	select {
	case result := <-results:
		return result.rank, result.found
	case <-time.After(findGuard):
		t.Fatalf("EFSet.Find(%d) did not return within %s: below-minimum search underflow", target, findGuard)
		return 0, false
	}
}

func TestEFSetFindBelowMinimumTerminatesWithoutMatch(t *testing.T) {
	set := container.CompressToEFSet([]uint64{10, 20, 30})

	// The recorded DAWGS-077 trigger: Find(5) previously did not complete.
	rank, found := findWithGuard(t, set, 5)
	assert.False(t, found)
	assert.Zero(t, rank)

	for _, target := range []uint64{0, 1, 9} {
		rank, found := findWithGuard(t, set, target)
		assert.False(t, found, "target %d below the minimum must not match", target)
		assert.Zero(t, rank)
	}
}

func TestEFSetFindOnEmptySetTerminatesWithoutMatch(t *testing.T) {
	set := container.NewEFSet(10, 30, 3)

	for _, target := range []uint64{5, 10, 20, 30, 100} {
		rank, found := findWithGuard(t, set, target)
		assert.False(t, found, "target %d must not match an empty set", target)
		assert.Zero(t, rank)
	}
}

func TestEFSetFindReturnsStoredRanks(t *testing.T) {
	set := container.CompressToEFSet([]uint64{10, 20, 30})

	rank, found := findWithGuard(t, set, 10)
	assert.True(t, found)
	assert.Zero(t, rank)

	rank, found = findWithGuard(t, set, 20)
	assert.True(t, found)
	assert.Equal(t, uint(1), rank)

	rank, found = findWithGuard(t, set, 30)
	assert.True(t, found)
	assert.Equal(t, uint(2), rank)

	// Misses above the maximum and between stored values terminate with no
	// match; only the rank of a match is meaningful.
	for _, target := range []uint64{11, 15, 25, 29, 31, 1000} {
		_, found := findWithGuard(t, set, target)
		assert.False(t, found, "target %d is not stored", target)
	}
}

func TestEFSetFindSingleValue(t *testing.T) {
	set := container.CompressToEFSet([]uint64{42})

	rank, found := findWithGuard(t, set, 5)
	assert.False(t, found)
	assert.Zero(t, rank)

	rank, found = findWithGuard(t, set, 42)
	assert.True(t, found)
	assert.Zero(t, rank)

	_, found = findWithGuard(t, set, 43)
	assert.False(t, found)
}

func TestEFSetFindAcrossLargerSet(t *testing.T) {
	values := make([]uint64, 0, 100)
	for idx := uint64(0); idx < 100; idx++ {
		values = append(values, 5+idx*10)
	}

	set := container.CompressToEFSet(values)

	_, found := findWithGuard(t, set, 4)
	assert.False(t, found)

	for idx, value := range values {
		rank, found := findWithGuard(t, set, value)
		assert.True(t, found, "value %d must be found", value)
		assert.Equal(t, uint(idx), rank)
	}

	for _, target := range []uint64{6, 14, 996, 1004} {
		_, found := findWithGuard(t, set, target)
		assert.False(t, found, "target %d is not stored", target)
	}
}
