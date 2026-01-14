package cardinality

type CommutativeDuplex64 struct {
	duplexes []Duplex[uint64]
}

func NewCommutativeDuplex64() *CommutativeDuplex64 {
	return &CommutativeDuplex64{}
}

func (s *CommutativeDuplex64) orAll() Duplex[uint64] {
	tempBitmap := NewBitmap64()

	for _, nextDuplex := range s.duplexes {
		tempBitmap.Or(nextDuplex)
	}

	return tempBitmap
}

func (s *CommutativeDuplex64) Or(duplex ...Duplex[uint64]) {
	s.duplexes = append(s.duplexes, duplex...)
}

func (s *CommutativeDuplex64) OrInto(duplex Duplex[uint64]) {
	for _, internalDuplex := range s.duplexes {
		duplex.Or(internalDuplex)
	}
}

func (s *CommutativeDuplex64) AndInto(duplex Duplex[uint64]) {
	for _, internalDuplex := range s.duplexes {
		duplex.And(internalDuplex)
	}
}

func (s *CommutativeDuplex64) OrAll(other *CommutativeDuplex64) {
	s.duplexes = append(s.duplexes, other.duplexes...)
}

func (s *CommutativeDuplex64) Cardinality() uint64 {
	return s.orAll().Cardinality()
}

func (s *CommutativeDuplex64) Slice() []uint64 {
	return s.orAll().Slice()
}

func (s *CommutativeDuplex64) Each(delegate func(value uint64) bool) {
	s.orAll().Each(delegate)
}

func (s *CommutativeDuplex64) Contains(value uint64) bool {
	for _, nextDuplex := range s.duplexes {
		if nextDuplex.Contains(value) {
			return true
		}
	}

	return false
}
