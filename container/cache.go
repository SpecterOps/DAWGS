package experiment

import (
	"sort"
	"sync"

	"github.com/OneOfOne/xxhash"
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

type packedBucket struct {
	keys        *EFSet
	idx         uint64
	pendingKeys []uint64
	values      []uint64
}

func newPacBucket() *packedBucket {
	return &packedBucket{}
}

func (s *packedBucket) Find(key uint64) (uint64, bool) {
	if keyRank, found := s.keys.Find(key); found {
		return s.values[keyRank], true
	}

	return 0, false
}

func (s *packedBucket) Append(key uint64, value uint64) {
	s.pendingKeys = append(s.pendingKeys, key)
	s.values = append(s.values, value)
}

func (s *packedBucket) Compact() {
	if len(s.pendingKeys) == 0 {
		return
	}

	// Sort by key and swap value positions so the two remain associated by their position
	sort.Slice(s.pendingKeys, func(i, j int) bool {
		if s.pendingKeys[i] < s.pendingKeys[j] {
			s.values[i], s.values[j] = s.values[j], s.values[i]
			return true
		}

		return false
	})

	s.keys = CompressToEFSet(s.pendingKeys)
	s.pendingKeys = nil
	s.values = s.values[0:len(s.values):len(s.values)]
}

type PackedMap struct {
	entryBuffer []byte

	buckets          []*packedBucket
	allocatedBuckets []*packedBucket

	digester   *xxhash.XXHash64
	parameters Parameters
}

func NewPacMap(parameters Parameters) *PackedMap {
	return &PackedMap{
		buckets:     make([]*packedBucket, parameters.NumBuckets),
		entryBuffer: make([]byte, EntrySizeBytes),
		digester:    xxhash.New64(),
		parameters:  parameters,
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
	var (
		bucketIdx      = entityIDHash / s.parameters.BucketKeyStride
		existingBucket = s.buckets[bucketIdx]
	)

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
