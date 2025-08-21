package experiment

import (
	"strconv"

	"github.com/OneOfOne/xxhash"
	"github.com/go-core-stack/patricia"
	"github.com/specterops/dawgs/graph"
)

type radixBucket struct {
	store patricia.SimpleTree[[]byte]
}

func newRadixBucket() *radixBucket {
	return &radixBucket{
		store: patricia.NewSimpleTree[[]byte](),
	}
}

func (s *radixBucket) PutEntity(entityIDHash uint64, entityHash []byte) {
	s.store.Insert(strconv.FormatUint(entityIDHash, 10), entityHash)
}

type RadixMap struct {
	buckets          []*radixBucket
	allocatedBuckets []*radixBucket
	digester         *xxhash.XXHash64
}

func NewRadixMap() *RadixMap {
	return &RadixMap{
		buckets:  make([]*radixBucket, NumMapBuckets),
		digester: xxhash.New64(),
	}
}

func (s *RadixMap) getBucket(entityIDHash uint64) *radixBucket {
	var (
		bucketIdx      = entityIDHash / bucketStride
		existingBucket = s.buckets[bucketIdx]
	)

	if existingBucket == nil {
		existingBucket = newRadixBucket()
		s.buckets[bucketIdx] = existingBucket
		s.allocatedBuckets = append(s.allocatedBuckets, existingBucket)
	}

	return existingBucket
}

func (s *RadixMap) PutNode(entityID string, node *graph.Node) ([]byte, error) {
	if entityIDHash, err := digestEntityIDHash(s.digester, entityID); err != nil {
		return nil, err
	} else if entityHash, err := digestNodeHash(s.digester, node); err != nil {
		return nil, err
	} else {
		s.getBucket(entityIDHash).PutEntity(entityIDHash, entityHash)
		return entityHash, nil
	}
}
