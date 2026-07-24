package ret

type scrubber struct{}

func (s *scrubber) ScrubNode(node *normalizedNode) actionCounts {
	return actionCounts{}
}

func (s *scrubber) ScrubRelationship(rel *normalizedRelationship) actionCounts {
	return actionCounts{}
}

type PropertyAction string

const (
	actionPreserve       PropertyAction = "preserve"
	actionPseudonymize   PropertyAction = "pseudonymize"
	actionRedact         PropertyAction = "redact"
	actionShiftTimestamp PropertyAction = "shift_timestamp"
)

type actionCounts struct {
	preserve     int
	pseudonymize int
	redact       int
	shift        int
}

func (s *actionCounts) addAction(action PropertyAction) {
	switch action {
	case actionPreserve:
		s.preserve++
	case actionPseudonymize:
		s.pseudonymize++
	case actionRedact:
		s.redact++
	case actionShiftTimestamp:
		s.shift++
	}
}

func (s *actionCounts) addCounts(other actionCounts) {
	s.preserve += other.preserve
	s.pseudonymize += other.pseudonymize
	s.redact += other.redact
	s.shift += other.shift
}
