package cardinality

import (
	"github.com/RoaringBitmap/roaring/v2"
)

type bitmap32Iterator struct {
	iterator roaring.IntPeekable
}

func (s bitmap32Iterator) HasNext() bool {
	return s.iterator.HasNext()
}

func (s bitmap32Iterator) Next() uint32 {
	return s.iterator.Next()
}

type bitmap32 struct {
	bitmap *roaring.Bitmap
}

func NewBitmap32() Duplex[uint32] {
	return bitmap32{
		bitmap: roaring.New(),
	}
}

func NewBitmap32Provider() Provider[uint32] {
	return NewBitmap32()
}

func NewBitmap32With(values ...uint32) Duplex[uint32] {
	duplex := NewBitmap32()
	duplex.Add(values...)

	return duplex
}

func (s bitmap32) Clear() {
	s.bitmap.Clear()
}

func (s bitmap32) Each(delegate func(nextValue uint32) bool) {
	for itr := s.bitmap.Iterator(); itr.HasNext(); {
		if ok := delegate(itr.Next()); !ok {
			break
		}
	}
}

func (s bitmap32) Iterator() Iterator[uint32] {
	return bitmap32Iterator{
		iterator: s.bitmap.Iterator(),
	}
}

func (s bitmap32) Slice() []uint32 {
	return s.bitmap.ToArray()
}

func (s bitmap32) Contains(value uint32) bool {
	return s.bitmap.Contains(value)
}

func (s bitmap32) CheckedAdd(value uint32) bool {
	return s.bitmap.CheckedAdd(value)
}

func (s bitmap32) Add(values ...uint32) {
	s.bitmap.AddMany(values)
}

func (s bitmap32) Remove(value uint32) {
	s.bitmap.Remove(value)
}

func (s bitmap32) Xor(provider Provider[uint32]) {
	switch typedProvider := provider.(type) {
	case bitmap32:
		s.bitmap.Xor(typedProvider.bitmap)

	case Duplex[uint32]:
		providerCopy := roaring.New()

		typedProvider.Each(func(value uint32) bool {
			providerCopy.Add(value)
			return true
		})

		s.bitmap.Xor(providerCopy)
	}
}

func (s bitmap32) And(provider Provider[uint32]) {
	switch typedProvider := provider.(type) {
	case bitmap32:
		s.bitmap.And(typedProvider.bitmap)

	case Duplex[uint32]:
		s.Each(func(nextValue uint32) bool {
			if !typedProvider.Contains(nextValue) {
				s.Remove(nextValue)
			}

			return true
		})
	}
}

func (s bitmap32) Or(provider Provider[uint32]) {
	switch typedProvider := provider.(type) {
	case bitmap32:
		s.bitmap.Or(typedProvider.bitmap)

	case Duplex[uint32]:
		typedProvider.Each(func(nextValue uint32) bool {
			s.Add(nextValue)
			return true
		})
	}
}

func (s bitmap32) Cardinality() uint64 {
	return s.bitmap.GetCardinality()
}

func (s bitmap32) Clone() Duplex[uint32] {
	return bitmap32{
		bitmap: s.bitmap.Clone(),
	}
}

func (s bitmap32) AndNot(provider Provider[uint32]) {
	switch typedProvider := provider.(type) {
	case bitmap32:
		s.bitmap.AndNot(typedProvider.bitmap)

	case Duplex[uint32]:
		s.Each(func(nextValue uint32) bool {
			if typedProvider.Contains(nextValue) {
				s.Remove(nextValue)
			}

			return true
		})
	}
}
