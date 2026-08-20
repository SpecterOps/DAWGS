package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParsePostgreSQLExplainMetrics(t *testing.T) {
	metrics, err := parsePostgreSQLExplainMetrics(`[{"Plan": {}, "Planning Time": 1.25, "Execution Time": 3.5, "Settings": {"plan_cache_mode": "force_custom_plan"}}]`)
	require.NoError(t, err)
	require.Equal(t, 1250*time.Microsecond, metrics.PlanningTime)
	require.Equal(t, 3500*time.Microsecond, metrics.ExecutionTime)
	require.Equal(t, "force_custom_plan", metrics.Settings["plan_cache_mode"])
	require.Equal(t, "822ae07d4783158bc1912bb623e5107cc9002d519e1143a9c200ed6ee18b6d0f", sqlFingerprint("select 1"))
}

func TestParsePostgreSQLExplainMetricsRejectsNonJSON(t *testing.T) {
	_, err := parsePostgreSQLExplainMetrics("Seq Scan on node")
	require.Error(t, err)
}

func TestExplainValueStringPreservesJSONBytes(t *testing.T) {
	require.Equal(t, `[{"Plan": {}}]`, explainValueString([]byte(`[{"Plan": {}}]`)))
}
