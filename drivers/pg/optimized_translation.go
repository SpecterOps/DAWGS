package pg

import "sync/atomic"

var optimizedTranslationEnabled atomic.Bool

func init() {
	optimizedTranslationEnabled.Store(true)
}

// SetOptimizedTranslation enables or disables optimized PostgreSQL Cypher
// translation for the current process. It returns the previous setting.
//
// Disabling optimization bypasses the translation cache and uses the baseline
// translation path for calls that begin after the setting changes. Existing
// cached entries remain available if optimization is enabled again.
func SetOptimizedTranslation(enabled bool) bool {
	return optimizedTranslationEnabled.Swap(enabled)
}

// OptimizedTranslationEnabled reports whether PostgreSQL Cypher translation
// uses optimization and the translation cache for newly started compilations.
func OptimizedTranslationEnabled() bool {
	return optimizedTranslationEnabled.Load()
}
