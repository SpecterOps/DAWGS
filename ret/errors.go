package ret

import "errors"

var (
	ErrInvalidConfig         = errors.New("ret invalid configuration")
	ErrInvalidCollection     = errors.New("ret invalid collection")
	ErrArtifactIntegrity     = errors.New("ret artifact integrity failure")
	ErrDestinationExists     = errors.New("ret destination exists")
	ErrResumeRequired        = errors.New("ret checkpoint requires resume")
	ErrCheckpointMissing     = errors.New("ret resume checkpoint missing")
	ErrCollectionNotLoadable = errors.New("ret collection is not loadable")
	ErrNonEmptyTarget        = errors.New("ret target graph is not empty")
	ErrSourceCountChanged    = errors.New("ret source count changed")
	ErrMetricsMismatch       = errors.New("ret database metrics mismatch")
)
