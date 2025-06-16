package cardinality

import (
	"encoding/binary"
	"sync"

	"github.com/axiomhq/hyperloglog"
)

var (
	size8BufferPool = &sync.Pool{
		New: func() any {
			// Return a new buffer the size of a single uint64 in bytes
			return make([]byte, 8)
		},
	}
)

type hyperLogLog64 struct {
	sketch *hyperloglog.Sketch
}

func NewHyperLogLog64() Simplex[uint64] {
	return &hyperLogLog64{
		sketch: hyperloglog.NewNoSparse(),
	}
}

func NewHyperLogLog64Provider() Provider[uint64] {
	return NewHyperLogLog64()
}

func (s *hyperLogLog64) Clone() Simplex[uint64] {
	return &hyperLogLog64{
		sketch: s.sketch.Clone(),
	}
}

func (s *hyperLogLog64) Clear() {
	s.sketch = hyperloglog.NewNoSparse()
}

func (s *hyperLogLog64) Add(values ...uint64) {
	buffer := size8BufferPool.Get()
	byteBuffer := buffer.([]byte)
	defer size8BufferPool.Put(buffer)

	for idx := 0; idx < len(values); idx++ {
		binary.LittleEndian.PutUint64(byteBuffer, values[idx])
		s.sketch.Insert(byteBuffer)
	}
}

func (s *hyperLogLog64) Or(provider Provider[uint64]) {
	switch typedProvider := provider.(type) {
	case *hyperLogLog64:
		s.sketch.Merge(typedProvider.sketch)

	case Duplex[uint64]:
		typedProvider.Each(func(nextValue uint64) bool {
			s.Add(nextValue)
			return true
		})
	}
}

func (s *hyperLogLog64) Cardinality() uint64 {
	return s.sketch.Estimate()
}
