package cardinality

import (
	"encoding/binary"
	"sync"

	"github.com/axiomhq/hyperloglog"
)

var (
	size4BufferPool = &sync.Pool{
		New: func() any {
			// Return a new buffer the size of a single unit32 in bytes
			return make([]byte, 4)
		},
	}
)

type hyperLogLog32 struct {
	sketch *hyperloglog.Sketch
}

func NewHyperLogLog32() Simplex[uint32] {
	return &hyperLogLog32{
		sketch: hyperloglog.New14(),
	}
}

func NewHyperLogLog32Provider() Provider[uint32] {
	return NewHyperLogLog32()
}

func (s *hyperLogLog32) Clone() Simplex[uint32] {
	return &hyperLogLog32{
		sketch: s.sketch.Clone(),
	}
}

func (s *hyperLogLog32) Clear() {
	s.sketch = hyperloglog.New14()
}

func (s *hyperLogLog32) Add(values ...uint32) {
	buffer := size4BufferPool.Get()
	byteBuffer := buffer.([]byte)
	defer size4BufferPool.Put(buffer)

	for idx := 0; idx < len(values); idx++ {
		binary.LittleEndian.PutUint32(byteBuffer, values[idx])
		s.sketch.Insert(byteBuffer)
	}
}

func (s *hyperLogLog32) Or(provider Provider[uint32]) {
	switch typedProvider := provider.(type) {
	case *hyperLogLog32:
		s.sketch.Merge(typedProvider.sketch)

	case Duplex[uint32]:
		typedProvider.Each(func(nextValue uint32) bool {
			s.Add(nextValue)
			return true
		})
	}
}

func (s *hyperLogLog32) Cardinality() uint64 {
	return s.sketch.Estimate()
}
