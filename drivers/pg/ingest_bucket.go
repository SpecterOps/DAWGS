package pg

import (
	"errors"
	"math/bits"
)

const (
	ingestHashBits       = 32
	ingestHashSignBit    = uint32(1) << (ingestHashBits - 1)
	ingestHashSpaceCount = uint64(1) << ingestHashBits
	ingestHashSignedMin  = -1 << (ingestHashBits - 1)
)

type ingestBucketSet struct {
	count      uint64
	bucketBits uint
}

type ingestBucketRange struct {
	Lower int32
	Upper *int32
}

func newIngestBucketSet(count uint64) (ingestBucketSet, error) {
	if count == 0 || count > ingestHashSpaceCount || count&(count-1) != 0 {
		return ingestBucketSet{}, errors.New("bucket count must be a power of two between 1 and 2^32")
	}

	return ingestBucketSet{
		count:      count,
		bucketBits: uint(bits.TrailingZeros64(count)),
	}, nil
}

func (buckets ingestBucketSet) Bucket(hash uint32) uint64 {
	if buckets.count == 1 {
		return 0
	}

	ordered := hash ^ ingestHashSignBit
	if buckets.count == ingestHashSpaceCount {
		return uint64(ordered)
	}

	return uint64(ordered >> (ingestHashBits - buckets.bucketBits))
}

func (buckets ingestBucketSet) Range(bucket uint64) ingestBucketRange {
	if buckets.count == 1 {
		return ingestBucketRange{Lower: ingestHashSignedMin}
	}

	if buckets.count == ingestHashSpaceCount {
		return buckets.fullRange(bucket)
	}

	shift := ingestHashBits - buckets.bucketBits
	lower := ingestOrderedHashToSigned(uint32(bucket << shift))
	if bucket == buckets.count-1 {
		return ingestBucketRange{Lower: lower}
	}

	upper := ingestOrderedHashToSigned(uint32((bucket + 1) << shift))

	return ingestBucketRange{Lower: lower, Upper: &upper}
}

func (buckets ingestBucketSet) fullRange(bucket uint64) ingestBucketRange {
	lower := ingestOrderedHashToSigned(uint32(bucket))
	if bucket == buckets.count-1 {
		return ingestBucketRange{Lower: lower}
	}

	upper := ingestOrderedHashToSigned(uint32(bucket + 1))

	return ingestBucketRange{Lower: lower, Upper: &upper}
}

func (bucketRange ingestBucketRange) Contains(hash int32) bool {
	return hash >= bucketRange.Lower && (bucketRange.Upper == nil || hash < *bucketRange.Upper)
}

func ingestOrderedHashToSigned(ordered uint32) int32 {
	return int32(ordered ^ ingestHashSignBit)
}
