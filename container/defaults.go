package experiment

import "math"

const (
	EntryFieldSizeBytes  = 8  // Expectation is that this is the size of an entry hash, either key or value
	EntrySizeBytes       = 16 // Key + Value - each 64 bits
	DefaultMaxValue      = math.MaxUint64
	DefaultNumMaxEntries = 8_000_000_000
	DefaultNumBuckets    = 1024 * 128
)
