// Package v2 provides an explicit opt-in PostgreSQL driver with translation
// caches owned by individual physical PostgreSQL connections.
package v2

import "fmt"

const defaultTranslationCacheEntries = 64

// Config configures the v2 connection-resident translation cache.
type Config struct {
	// TranslationCacheEntries is the exact SIEVE entry capacity for each live
	// physical PostgreSQL connection. Zero disables retention.
	TranslationCacheEntries int
}

// DefaultConfig returns the conservative v2 defaults. The aggregate upper
// bound is TranslationCacheEntries multiplied by the number of live physical
// PostgreSQL connections.
func DefaultConfig() Config {
	return Config{TranslationCacheEntries: defaultTranslationCacheEntries}
}

func (s Config) validate() error {
	if s.TranslationCacheEntries < 0 {
		return fmt.Errorf("translation cache entries must not be negative: %d", s.TranslationCacheEntries)
	}
	return nil
}
