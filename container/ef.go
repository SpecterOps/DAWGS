package container

import (
	"errors"
	"iter"
	"math"

	"github.com/bits-and-blooms/bitset"
)

var (
	ErrUnorderedAppend      = errors.New("unordered append")
	ErrValueOutsideUniverse = errors.New("value outside EFSet universe")
)

type EFSet struct {
	upperBits       *bitset.BitSet
	lowerBits       *bitset.BitSet
	universeMin     uint64
	universeMax     uint64
	last            uint64
	upperBitsLength uint
	lowerBitsLength uint
	lowerBitsMask   uint64
	len             uint
}

func NewEFSet(universeMin, universeMax uint64, maxValues uint) *EFSet {
	var (
		logN            = math.Ceil(math.Log2(float64(maxValues + 2)))
		entryLength     = math.Ceil(math.Log2(math.Max(float64(universeMax), 2)))
		upperBitsLength = math.Ceil(math.Log2(math.Max(float64(maxValues), 2)))
		lowerBitsLength = uint(math.Max(entryLength, logN) - upperBitsLength)
		lowerBitsMask   = (uint64(1) << lowerBitsLength) - 1
		upperBitset     = bitset.New(0)
		lowerBitset     = bitset.New(0)
	)

	return &EFSet{
		upperBits:       upperBitset,
		lowerBits:       lowerBitset,
		universeMin:     universeMin,
		universeMax:     universeMax,
		last:            0,
		upperBitsLength: uint(upperBitsLength),
		lowerBitsLength: lowerBitsLength,
		lowerBitsMask:   lowerBitsMask,
		len:             0,
	}
}

func AllocateEFSet(universeMin, universeMax uint64, maxValues uint) *EFSet {
	var (
		logN              = math.Ceil(math.Log2(float64(maxValues + 2)))
		entryLength       = math.Ceil(math.Log2(math.Max(float64(universeMax), 2)))
		upperBitsLength   = math.Ceil(math.Log2(math.Max(float64(maxValues), 2)))
		lowerBitsLength   = uint(math.Max(entryLength, logN) - upperBitsLength)
		lowerBitsMask     = (uint64(1) << lowerBitsLength) - 1
		upperBitsetLength = maxValues + uint(universeMax>>lowerBitsLength) + 2
		upperBitset       = bitset.New(upperBitsetLength)
		lowerBitset       = bitset.New(maxValues * lowerBitsLength)
	)

	return &EFSet{
		upperBits:       upperBitset,
		lowerBits:       lowerBitset,
		universeMin:     universeMin,
		universeMax:     universeMax,
		last:            0,
		upperBitsLength: uint(upperBitsLength),
		lowerBitsLength: lowerBitsLength,
		lowerBitsMask:   lowerBitsMask,
		len:             0,
	}
}

func (s *EFSet) Iterator() iter.Seq2[uint, uint64] {
	return iter.Seq2[uint, uint64](func(yield func(uint, uint64) bool) {
		iterInst := NewEFSetIterator(s)

		for idx := uint(0); idx < s.len; idx++ {
			if !yield(idx, iterInst.Next()) {
				break
			}
		}
	})
}

func (s *EFSet) Len() uint {
	return s.len
}

func (s *EFSet) Lookup(index uint) uint64 {
	var (
		upper             = s.upperBits.Select(index) - index - 1
		lower      uint64 = 0
		lowerIndex        = index * s.lowerBitsLength
	)

	for idx := lowerIndex; idx < lowerIndex+s.lowerBitsLength; idx++ {
		lower = lower << 1

		if s.lowerBits.Test(idx + 1) {
			lower += 1
		}
	}

	return (uint64(upper<<s.lowerBitsLength) | lower) + s.universeMin
}

func (s *EFSet) append(value uint64) {
	var (
		universeFloorAdjustedValue = value - s.universeMin
		upper                      = uint(universeFloorAdjustedValue >> s.lowerBitsLength)
		lower                      = uint(universeFloorAdjustedValue & s.lowerBitsMask)
		lowerOffset                = s.len * s.lowerBitsLength
	)

	s.upperBits.Set(upper + s.len + 1)

	for bitIdx := uint(0); bitIdx < s.lowerBitsLength; bitIdx++ {
		lowerBitValue := lower & (1 << (s.lowerBitsLength - bitIdx - 1))
		s.lowerBits.SetTo(lowerOffset+bitIdx+1, lowerBitValue > 0)
	}

	s.len += 1
	s.last = value
}

func (s *EFSet) Append(value uint64) error {
	if value < s.universeMin || value > s.universeMax {
		return ErrValueOutsideUniverse
	}

	if value < s.last {
		return ErrUnorderedAppend
	}

	s.append(value)
	return nil
}

func (s *EFSet) MustAppend(value uint64) {
	if value < s.universeMin || value > s.universeMax {
		panic(ErrValueOutsideUniverse)
	}

	if value < s.last {
		panic(ErrUnorderedAppend)
	}

	s.append(value)
}

func (s *EFSet) Find(target uint64) (uint, bool) {
	var (
		lowEntry  uint = 0
		highEntry      = s.len - 1
	)

	for lowEntry <= highEntry {
		midEntry := lowEntry + (highEntry-lowEntry)/2

		if nextValue := s.Lookup(midEntry); nextValue == target {
			return midEntry, true
		} else if nextValue < target {
			lowEntry = midEntry + 1
		} else {
			highEntry = midEntry - 1
		}
	}

	return 0, false
}

func CompressToEFSet(values []uint64) *EFSet {
	compactedVec := AllocateEFSet(values[0], values[len(values)-1], uint(len(values)))

	for idx := 0; idx < len(values); idx++ {
		compactedVec.MustAppend(values[idx])
	}

	return compactedVec
}

type EFSetIterator struct {
	vec      *EFSet
	upperIdx uint
	lowerIdx uint
}

func NewEFSetIterator(vec *EFSet) *EFSetIterator {
	return &EFSetIterator{
		vec: vec,
	}
}

func (s *EFSetIterator) advanceHigherBitsPosition() {
	if s.upperIdx > 0 {
		s.upperIdx += 1
	}

	s.upperIdx, _ = s.vec.upperBits.NextSet(s.upperIdx)
}

func (s *EFSetIterator) Next() uint64 {
	var (
		nextLower      uint64 = 0
		lowerLookupIdx        = s.lowerIdx * s.vec.lowerBitsLength
	)

	s.advanceHigherBitsPosition()

	for idx := lowerLookupIdx; idx < lowerLookupIdx+s.vec.lowerBitsLength; idx++ {
		nextLower = nextLower << 1

		if s.vec.lowerBits.Test(idx + 1) {
			nextLower += 1
		}
	}

	value := (uint64((s.upperIdx-s.lowerIdx-1)<<s.vec.lowerBitsLength) | nextLower) + s.vec.universeMin
	s.lowerIdx += 1

	return value
}
