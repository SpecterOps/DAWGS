package drivers

import "sync/atomic"

const (
	queryAnalysisEnabled  uint32 = 0
	queryAnalysisDisabled uint32 = 1
)

var isQueryAnalysisEnabled = queryAnalysisDisabled

func EnableQueryAnalysis() {
	atomic.StoreUint32(&isQueryAnalysisEnabled, queryAnalysisEnabled)
}

func DisableQueryAnalysis() {
	atomic.StoreUint32(&isQueryAnalysisEnabled, queryAnalysisDisabled)
}

func IsQueryAnalysisEnabled() bool {
	return atomic.LoadUint32(&isQueryAnalysisEnabled) == queryAnalysisEnabled
}
