package cardinality

type DuplexCommutation[T uint32 | uint64] []Duplex[T]

func CommutativeOr[T uint32 | uint64](duplexes ...Duplex[T]) DuplexCommutation[T] {
	return duplexes
}

func (s DuplexCommutation[T]) Or(duplexes ...Duplex[T]) DuplexCommutation[T] {
	return append(s, duplexes...)
}

func (s DuplexCommutation[T]) Contains(value T) bool {
	for _, duplex := range s {
		if duplex.Cardinality() > 0 && duplex.Contains(value) {
			return true
		}
	}

	return false
}

type CommutativeDuplexes[T uint32 | uint64] struct {
	or  []DuplexCommutation[T]
	and []DuplexCommutation[T]
}

func (s *CommutativeDuplexes[T]) Or(dc DuplexCommutation[T]) {
	s.or = append(s.or, dc)
}

func (s *CommutativeDuplexes[T]) And(dc DuplexCommutation[T]) {
	s.and = append(s.and, dc)
}

func (s *CommutativeDuplexes[T]) valueInOrSets(value T) bool {
	// Search each bitwise or set. Only one set is required to contain the value.
	for _, orSet := range s.or {
		if orSet.Contains(value) {
			return true
		}
	}

	return false
}

func (s *CommutativeDuplexes[T]) valueInAndSets(value T) bool {
	// Search each bitwise and set. Each and set must contain the value.
	for _, andSet := range s.and {
		if !andSet.Contains(value) {
			return false
		}
	}

	return true
}

func (s *CommutativeDuplexes[T]) Contains(value T) bool {
	return s.valueInOrSets(value) && s.valueInAndSets(value)
}
