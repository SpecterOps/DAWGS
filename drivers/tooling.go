package drivers

import "sync/atomic"

const (
	// queryAnalysisEnabled is the atomic flag value that permits query analysis.
	queryAnalysisEnabled uint32 = 0

	// queryAnalysisDisabled is the initial atomic flag value that suppresses query analysis.
	queryAnalysisDisabled uint32 = 1
)

// isQueryAnalysisEnabled stores the process-wide analysis flag and must be accessed atomically.
var isQueryAnalysisEnabled = queryAnalysisDisabled

// EnableQueryAnalysis atomically enables process-wide query analysis.
func EnableQueryAnalysis() {
	atomic.StoreUint32(&isQueryAnalysisEnabled, queryAnalysisEnabled)
}

// DisableQueryAnalysis atomically disables process-wide query analysis.
func DisableQueryAnalysis() {
	atomic.StoreUint32(&isQueryAnalysisEnabled, queryAnalysisDisabled)
}

// IsQueryAnalysisEnabled atomically reads whether process-wide query analysis is enabled.
func IsQueryAnalysisEnabled() bool {
	return atomic.LoadUint32(&isQueryAnalysisEnabled) == queryAnalysisEnabled
}
