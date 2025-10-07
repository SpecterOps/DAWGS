package container

import "math"

const (
	EntryFieldSizeBytes  = 8                       // Size of a 64-bit hash
	EntrySizeBytes       = EntryFieldSizeBytes * 2 // Key 64-bit hash + Value 64-bit hash
	DefaultMaxValue      = math.MaxUint64
	DefaultNumMaxEntries = 8_000_000_000
	DefaultNumBuckets    = 1024 * 128
)
