package cardinality

import (
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type bitmap64Iterator struct {
	iterator roaring64.IntPeekable64
}

func (s bitmap64Iterator) HasNext() bool {
	return s.iterator.HasNext()
}

func (s bitmap64Iterator) Next() uint64 {
	return s.iterator.Next()
}

type bitmap64 struct {
	bitmap *roaring64.Bitmap
}

func NewBitmap64() Duplex[uint64] {
	return bitmap64{
		bitmap: roaring64.New(),
	}
}

func NewBitmap64Provider() Provider[uint64] {
	return NewBitmap64()
}

func NewBitmap64With(values ...uint64) Duplex[uint64] {
	duplex := NewBitmap64()
	duplex.Add(values...)

	return duplex
}

func (s bitmap64) Clear() {
	s.bitmap.Clear()
}

func (s bitmap64) Each(delegate func(nextValue uint64) bool) {
	for itr := s.bitmap.Iterator(); itr.HasNext(); {
		if ok := delegate(itr.Next()); !ok {
			break
		}
	}
}

func (s bitmap64) Iterator() Iterator[uint64] {
	return bitmap64Iterator{
		iterator: s.bitmap.Iterator(),
	}
}

func (s bitmap64) Slice() []uint64 {
	return s.bitmap.ToArray()
}

func (s bitmap64) Contains(value uint64) bool {
	return s.bitmap.Contains(value)
}

func (s bitmap64) CheckedAdd(value uint64) bool {
	return s.bitmap.CheckedAdd(value)
}

func (s bitmap64) Add(values ...uint64) {
	switch len(values) {
	case 0:
	case 1:
		s.bitmap.Add(values[0])
	default:
		s.bitmap.AddMany(values)
	}
}

func (s bitmap64) Remove(value uint64) {
	s.bitmap.Remove(value)
}

func (s bitmap64) Xor(provider Provider[uint64]) {
	switch typedProvider := provider.(type) {
	case bitmap64:
		s.bitmap.Xor(typedProvider.bitmap)

	case Duplex[uint64]:
		providerCopy := roaring64.New()

		typedProvider.Each(func(value uint64) bool {
			providerCopy.Add(value)
			return true
		})

		s.bitmap.Xor(providerCopy)
	}
}
func (s bitmap64) And(provider Provider[uint64]) {
	switch typedProvider := provider.(type) {
	case bitmap64:
		s.bitmap.And(typedProvider.bitmap)

	case Duplex[uint64]:
		s.Each(func(nextValue uint64) bool {
			if !typedProvider.Contains(nextValue) {
				s.Remove(nextValue)
			}

			return true
		})
	}
}
func (s bitmap64) Or(provider Provider[uint64]) {
	switch typedProvider := provider.(type) {
	case bitmap64:
		s.bitmap.Or(typedProvider.bitmap)

	case Duplex[uint64]:
		typedProvider.Each(func(nextValue uint64) bool {
			s.Add(nextValue)
			return true
		})
	}
}

func (s bitmap64) Cardinality() uint64 {
	return s.bitmap.GetCardinality()
}

func (s bitmap64) Clone() Duplex[uint64] {
	return bitmap64{
		bitmap: s.bitmap.Clone(),
	}
}

func (s bitmap64) AndNot(provider Provider[uint64]) {
	switch typedProvider := provider.(type) {
	case bitmap64:
		s.bitmap.AndNot(typedProvider.bitmap)

	case Duplex[uint64]:
		s.Each(func(nextValue uint64) bool {
			if typedProvider.Contains(nextValue) {
				s.Remove(nextValue)
			}

			return true
		})
	}
}
