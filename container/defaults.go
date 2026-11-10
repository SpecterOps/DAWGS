package container

import "math"

const (
	// EntryFieldSizeBytes is the byte width of one 64-bit hash field.
	EntryFieldSizeBytes = 8

	// EntrySizeBytes is the combined byte width of a key hash and value hash.
	EntrySizeBytes = EntryFieldSizeBytes * 2

	// DefaultMaxValue is the default upper bound of the unsigned value universe.
	DefaultMaxValue = math.MaxUint64

	// DefaultNumMaxEntries is the default maximum entry count used for container sizing.
	DefaultNumMaxEntries = 8_000_000_000

	// DefaultNumBuckets is the default number of hash buckets.
	DefaultNumBuckets = 1024 * 128
)
