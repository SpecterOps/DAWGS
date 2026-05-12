package pg

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

// Compile-time assertion that *Driver implements graph.Optimizer.
var _ graph.Optimizer = (*Driver)(nil)

// TestNeedsReindex exercises the threshold logic that decides whether a
// measured index is flagged as a rebuild candidate. The function is pure;
// integration coverage of the surrounding pg_extension / pg_inherits /
// pgstatindex queries lands with the Phase 5 REINDEX work.
func TestNeedsReindex(t *testing.T) {
	cases := []struct {
		name          string
		leafDensity   float64
		fragmentation float64
		wantFlagged   bool
		wantReasonHas string
	}{
		{
			name:          "healthy index is not flagged",
			leafDensity:   85.0,
			fragmentation: 5.0,
			wantFlagged:   false,
		},
		{
			name:          "freshly built baseline (73.8%/low frag) is not flagged",
			leafDensity:   73.8,
			fragmentation: 2.5,
			wantFlagged:   false,
		},
		{
			name:          "leaf density exactly at threshold is not flagged",
			leafDensity:   bloatedIndexLeafDensityThreshold,
			fragmentation: 0,
			wantFlagged:   false,
		},
		{
			name:          "leaf density just below threshold is flagged",
			leafDensity:   bloatedIndexLeafDensityThreshold - 0.01,
			fragmentation: 0,
			wantFlagged:   true,
			wantReasonHas: "leaf density",
		},
		{
			name:          "deeply bloated production sample (21%) is flagged on density",
			leafDensity:   21.0,
			fragmentation: 8.0,
			wantFlagged:   true,
			wantReasonHas: "leaf density",
		},
		{
			name:          "fragmentation just below threshold is not flagged when density is healthy",
			leafDensity:   85.0,
			fragmentation: highIndexFragmentationThreshold - 0.01,
			wantFlagged:   false,
		},
		{
			name:          "fragmentation exactly at threshold is flagged",
			leafDensity:   85.0,
			fragmentation: highIndexFragmentationThreshold,
			wantFlagged:   true,
			wantReasonHas: "fragmentation",
		},
		{
			name:          "fragmentation flag triggers when density alone would not",
			leafDensity:   65.0,
			fragmentation: 50.0,
			wantFlagged:   true,
			wantReasonHas: "fragmentation",
		},
		{
			name:          "density takes precedence in the reason when both cross",
			leafDensity:   30.0,
			fragmentation: 60.0,
			wantFlagged:   true,
			wantReasonHas: "leaf density",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := indexCandidate{
				leafDensity:   tc.leafDensity,
				fragmentation: tc.fragmentation,
			}
			reason, flagged := needsReindex(candidate)
			assert.Equal(t, tc.wantFlagged, flagged, "unexpected flagged result for %s", tc.name)
			if tc.wantFlagged {
				assert.True(t, strings.Contains(reason, tc.wantReasonHas),
					"reason %q does not mention expected substring %q", reason, tc.wantReasonHas)
			} else {
				assert.Empty(t, reason, "expected empty reason when not flagged")
			}
		})
	}
}

// TestThresholdsAreOrdered guards against accidental reordering of the
// calibrated thresholds. If these inequalities ever fail the calibration
// rationale documented on the constants must be revisited.
func TestThresholdsAreOrdered(t *testing.T) {
	assert.Greater(t, bloatedIndexLeafDensityThreshold, 0.0, "leaf density threshold must be positive")
	assert.Less(t, bloatedIndexLeafDensityThreshold, 100.0, "leaf density threshold must be a percentage")
	assert.Greater(t, highIndexFragmentationThreshold, 0.0, "fragmentation threshold must be positive")
	assert.Less(t, highIndexFragmentationThreshold, 100.0, "fragmentation threshold must be a percentage")
}
