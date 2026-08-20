package pg

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngestBucketsRejectInvalidCounts(t *testing.T) {
	for _, count := range []uint64{0, 3, 6, uint64(math.MaxUint32) + 2} {
		t.Run("count", func(t *testing.T) {
			_, err := newIngestBucketSet(count)
			require.Error(t, err)
		})
	}
}

func TestIngestBucketsAcceptPowerOfTwoCounts(t *testing.T) {
	for _, count := range []uint64{1, 2, 4, 256, uint64(math.MaxUint32) + 1} {
		t.Run("count", func(t *testing.T) {
			buckets, err := newIngestBucketSet(count)
			require.NoError(t, err)
			require.Equal(t, count, buckets.count)
		})
	}
}

func TestIngestBucketsUseSignedContiguousRanges(t *testing.T) {
	buckets, err := newIngestBucketSet(4)
	require.NoError(t, err)

	expected := []struct {
		lower int32
		upper *int32
	}{
		{lower: math.MinInt32, upper: int32Pointer(-1073741824)},
		{lower: -1073741824, upper: int32Pointer(0)},
		{lower: 0, upper: int32Pointer(1073741824)},
		{lower: 1073741824, upper: nil},
	}

	for bucket, want := range expected {
		rangeForBucket := buckets.Range(uint64(bucket))
		require.Equal(t, want.lower, rangeForBucket.Lower)
		require.Equal(t, want.upper, rangeForBucket.Upper)
	}
}

func TestIngestBucketsFinalRangeHasNoUpperBound(t *testing.T) {
	buckets, err := newIngestBucketSet(2)
	require.NoError(t, err)

	finalRange := buckets.Range(1)
	require.Equal(t, int32(0), finalRange.Lower)
	require.Nil(t, finalRange.Upper)
	require.True(t, finalRange.Contains(math.MaxInt32))
}

func TestIngestBucketsContainEveryBoundaryExactlyOnce(t *testing.T) {
	buckets, err := newIngestBucketSet(4)
	require.NoError(t, err)

	for _, boundary := range []int32{math.MinInt32, -1073741824, 0, 1073741824, math.MaxInt32} {
		contained := 0
		for bucket := uint64(0); bucket < 4; bucket++ {
			if buckets.Range(bucket).Contains(boundary) {
				contained++
			}
		}
		require.Equal(t, 1, contained, "boundary %d", boundary)
	}
}

func TestIngestBucketsMapHashesThroughSignedOrder(t *testing.T) {
	buckets, err := newIngestBucketSet(4)
	require.NoError(t, err)

	for hash, want := range map[uint32]uint64{
		0x80000000: 0,
		0xc0000000: 1,
		0x00000000: 2,
		0x40000000: 3,
	} {
		require.Equal(t, want, buckets.Bucket(hash))
	}
}

func TestIngestBucketsSpecialCaseSingleAndFullHashSpace(t *testing.T) {
	single, err := newIngestBucketSet(1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), single.Bucket(0))
	require.Equal(t, uint64(0), single.Bucket(math.MaxUint32))
	require.Equal(t, int32(math.MinInt32), single.Range(0).Lower)
	require.Nil(t, single.Range(0).Upper)

	full, err := newIngestBucketSet(uint64(math.MaxUint32) + 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), full.Bucket(0x80000000))
	require.Equal(t, uint64(math.MaxUint32), full.Bucket(0x7fffffff))
	require.Equal(t, int32(math.MinInt32), full.Range(0).Lower)
	require.Equal(t, int32(math.MaxInt32), full.Range(uint64(math.MaxUint32)).Lower)
	require.Nil(t, full.Range(uint64(math.MaxUint32)).Upper)
}

func int32Pointer(value int32) *int32 {
	return &value
}
