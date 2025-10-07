package container

import (
	"math"
	"sort"
	"sync"
)

type Parameters struct {
	BucketKeyStride uint64
	NumMaxEntries   int
	NumBuckets      int
}

func DefaultParameters() Parameters {
	return Parameters{
		BucketKeyStride: DefaultMaxValue / DefaultNumBuckets,
		NumMaxEntries:   DefaultNumMaxEntries,
		NumBuckets:      DefaultNumBuckets,
	}
}

type kvPair struct {
	key   uint64
	value uint64
}

type packedBucket struct {
	keys    *EFSet
	values  []uint64
	pending []kvPair
}

func newPacBucket() *packedBucket {
	return &packedBucket{}
}

func (s *packedBucket) Find(key uint64) (uint64, bool) {
	if s.keys != nil {
		keyRank, found := s.keys.Find(key)

		if found {
			return s.values[keyRank], true
		}
	}

	return 0, false
}

func (s *packedBucket) Append(key uint64, value uint64) {
	s.pending = append(s.pending, kvPair{
		key:   key,
		value: value,
	})
}

func (s *packedBucket) merge() {
	var (
		newUniverseMin uint64 = math.MaxUint64
		newUniverseMax uint64 = 0
	)

	// If there are values stored in this bucket clamp the universe values accordingly
	if s.keys != nil {
		newUniverseMin = s.keys.universeMin
		newUniverseMax = s.keys.universeMax
	}

	// Check the pending values for a new universeMin and universeMax
	if firstPendingKey := s.pending[0].key; firstPendingKey < newUniverseMin {
		newUniverseMin = firstPendingKey
	}

	if lastPendingKey := s.pending[len(s.pending)-1].key; lastPendingKey > newUniverseMax {
		newUniverseMax = lastPendingKey
	}

	// Allocate the new EF set and prepare to iterate through both the existing EF set
	var (
		maxLen     = s.keys.len + uint(len(s.pending))
		mergedSet  = NewEFSet(newUniverseMin, newUniverseMax, maxLen)
		values     = make([]uint64, 0, maxLen)
		pendingIdx = 0
		valueIdx   = 0
	)

	// Iterate the existing EF set in order
	for _, nextKey := range s.keys.Iterator() {
		// While the pending key is less than the next key in the existing EF set, append
		// from the pending key-value pairs
		for pendingIdx < len(s.pending) && s.pending[pendingIdx].key <= nextKey {
			if s.pending[pendingIdx].key != nextKey {
				mergedSet.Append(s.pending[pendingIdx].key)
				values = append(values, s.pending[pendingIdx].value)
			}

			pendingIdx++
		}

		// Append the next key and value from the existing EF set
		mergedSet.Append(nextKey)
		values = append(values, s.values[valueIdx])

		valueIdx++
	}

	// Append remaining pending key value pairs
	for pendingIdx < len(s.pending) {
		mergedSet.Append(s.pending[pendingIdx].key)
		values = append(values, s.pending[pendingIdx].value)

		pendingIdx++
	}

	s.keys = mergedSet
	s.values = s.values[0:len(s.values):len(s.values)]
}

func (s *packedBucket) Compact() {
	if len(s.pending) == 0 {
		return
	}

	// Order pending writes first to append to the merged EF set
	sort.Slice(s.pending, func(i, j int) bool {
		return s.pending[i].key < s.pending[j].key
	})

	if s.keys != nil {
		s.merge()
	} else {
		var (
			newUniverseMin uint64 = s.pending[0].key
			newUniverseMax uint64 = s.pending[len(s.pending)-1].key
		)

		// Allocate the key and value containers
		s.keys = NewEFSet(newUniverseMin, newUniverseMax, uint(len(s.pending)))
		s.values = make([]uint64, len(s.pending))

		for idx, nextKVPair := range s.pending {
			s.keys.Append(nextKVPair.key)
			s.values[idx] = nextKVPair.value
		}
	}

	s.pending = nil
}

type PackedMap struct {
	buckets          []*packedBucket
	allocatedBuckets []*packedBucket
	parameters       Parameters
}

func NewPacMap(parameters Parameters) *PackedMap {
	return &PackedMap{
		buckets:    make([]*packedBucket, parameters.NumBuckets),
		parameters: parameters,
	}
}

func (s *PackedMap) Compact() {
	for _, allocatedBucket := range s.allocatedBuckets {
		allocatedBucket.Compact()
	}
}

func (s *PackedMap) CompactParallel(workers int) {
	var (
		jobC = make(chan *packedBucket)
		wg   = &sync.WaitGroup{}
	)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for nextBucket := range jobC {
				nextBucket.Compact()
			}
		}()
	}

	for _, allocatedBucket := range s.allocatedBuckets {
		jobC <- allocatedBucket
	}

	close(jobC)
	wg.Wait()
}

func (s *PackedMap) getBucket(entityIDHash uint64) *packedBucket {
	bucketIdx := entityIDHash / s.parameters.BucketKeyStride

	if bucketIdx >= uint64(len(s.buckets)) {
		bucketIdx = uint64(len(s.buckets) - 1)
	}

	existingBucket := s.buckets[bucketIdx]

	if existingBucket == nil {
		existingBucket = newPacBucket()

		s.buckets[bucketIdx] = existingBucket
		s.allocatedBuckets = append(s.allocatedBuckets, existingBucket)
	}

	return existingBucket
}

func (s *PackedMap) Get(key uint64) (uint64, bool) {
	return s.getBucket(key).Find(key)
}

func (s *PackedMap) Put(key, value uint64) {
	s.getBucket(key).Append(key, value)
}
