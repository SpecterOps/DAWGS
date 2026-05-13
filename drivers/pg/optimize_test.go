package pg

import (
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

// Compile-time assertion that *Driver implements graph.Optimizer.
var _ graph.Optimizer = (*Driver)(nil)

// TestNeedsReindex exercises the threshold logic that decides whether a
// measured index is flagged as a rebuild candidate. The function is pure;
// integration coverage of the surrounding pg_extension / pg_inherits /
// pgstatindex queries and the REINDEX execution itself is exercised under
// make test_integration against a real Postgres backend.
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
	assert.Greater(t, ginPendingPagesFlushThreshold, int(0), "GIN pending-pages threshold must be positive")
	assert.Greater(t, vacuumDeadTupleRatioThreshold, 0.0, "vacuum dead-tuple ratio threshold must be positive")
	assert.Less(t, vacuumDeadTupleRatioThreshold, 1.0, "vacuum dead-tuple ratio threshold must be a fraction")
	assert.Greater(t, vacuumDeadTupleAbsoluteFloor, int(0), "vacuum dead-tuple floor must be positive")
	assert.Greater(t, analyzeModificationThreshold, int(0), "analyze modification threshold must be positive")
	assert.Greater(t, analyzeStalenessWindow, time.Duration(0), "analyze staleness window must be positive")
}

// TestBuildReindexSQL verifies the REINDEX statement is produced with both
// schema and index name quoted via pgx.Identifier.Sanitize, including the
// edge case where an identifier itself contains a double quote.
func TestBuildReindexSQL(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		index  string
		want   string
	}{
		{
			name:   "ordinary identifiers are double-quoted",
			schema: "graph",
			index:  "edge_1_pkey",
			want:   `reindex index concurrently "graph"."edge_1_pkey"`,
		},
		{
			name:   "mixed-case identifiers preserve case under quoting",
			schema: "Graph",
			index:  "Edge_1_PKey",
			want:   `reindex index concurrently "Graph"."Edge_1_PKey"`,
		},
		{
			name:   "embedded double quote is escaped by doubling",
			schema: `evil"schema`,
			index:  "edge_1_pkey",
			want:   `reindex index concurrently "evil""schema"."edge_1_pkey"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, buildReindexSQL(tc.schema, tc.index))
		})
	}
}

// TestBuildDropInvalidIndexSQL verifies the orphan cleanup statement is
// produced with IF EXISTS and CONCURRENTLY guarding identifier handling.
func TestBuildDropInvalidIndexSQL(t *testing.T) {
	got := buildDropInvalidIndexSQL("graph", "edge_1_pkey_ccnew")
	assert.Equal(t, `drop index concurrently if exists "graph"."edge_1_pkey_ccnew"`, got)

	// Identifier sanitization must apply to the orphan name as well, since
	// _ccnew suffixes are read directly from pg_class.relname.
	got = buildDropInvalidIndexSQL("graph", `weird"_ccnew`)
	assert.Equal(t, `drop index concurrently if exists "graph"."weird""_ccnew"`, got)
}

// TestOrphanedReindexArtifactQuery_FiltersByValidityAndNameAndAm guards the
// SQL string that scans for cleanup candidates against accidental loosening
// of its filters, since this query controls the blast radius of DROP INDEX.
func TestOrphanedReindexArtifactQuery_FiltersByValidityAndNameAndAm(t *testing.T) {
	for _, fragment := range []string{
		"x.indisvalid = false",
		"a.amname = 'btree'",
		"i.relname ~ '_ccnew[0-9]*$'",
		"p.relname in ('node', 'edge')",
	} {
		assert.Contains(t, sqlSelectOrphanedReindexArtifacts, fragment,
			"orphan-cleanup SQL is missing required filter %q; relaxing this filter risks dropping unrelated indexes", fragment)
	}
}

// TestNeedsGinFlush exercises the threshold logic that decides whether a
// measured GIN index is flagged for an explicit gin_clean_pending_list call.
// The function is pure; integration coverage of the surrounding pgstatginindex
// query and the gin_clean_pending_list execution itself is exercised under
// make test_integration against a real Postgres backend.
func TestNeedsGinFlush(t *testing.T) {
	cases := []struct {
		name          string
		pendingPages  int64
		wantFlagged   bool
		wantReasonHas string
	}{
		{
			name:         "empty pending list is not flagged",
			pendingPages: 0,
			wantFlagged:  false,
		},
		{
			name:         "pending list one page below threshold is not flagged",
			pendingPages: ginPendingPagesFlushThreshold - 1,
			wantFlagged:  false,
		},
		{
			name:          "pending list exactly at threshold is flagged",
			pendingPages:  ginPendingPagesFlushThreshold,
			wantFlagged:   true,
			wantReasonHas: "16.0 MiB",
		},
		{
			name:          "pending list well above threshold is flagged",
			pendingPages:  22076,
			wantFlagged:   true,
			wantReasonHas: "pending pages 22076",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := ginFlushCandidate{pendingPages: tc.pendingPages}
			reason, flagged := needsGinFlush(c)
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

// TestVacuumAssessment exercises the threshold logic that decides whether a
// partition is flagged for VACUUM (ANALYZE). Both triggers are independent
// and may co-fire; the test covers each in isolation plus the boundary cases.
func TestVacuumAssessment(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	freshAnalyze := now.Add(-time.Hour)
	staleAnalyze := now.Add(-(analyzeStalenessWindow + time.Minute))

	cases := []struct {
		name             string
		row              vacuumStatsRow
		wantFlagged      bool
		wantNeedsVacuum  bool
		wantNeedsAnalyze bool
		wantReasonHas    string
	}{
		{
			name:        "healthy partition with low dead tuples and recent analyze is not flagged",
			row:         vacuumStatsRow{liveTuples: 1_000_000, deadTuples: 5_000, modSinceAnalyze: 1_000, lastAnalyzedAt: &freshAnalyze},
			wantFlagged: false,
		},
		{
			name:        "dead-tuple ratio above threshold but below absolute floor is not flagged",
			row:         vacuumStatsRow{liveTuples: 100, deadTuples: 100, modSinceAnalyze: 0, lastAnalyzedAt: &freshAnalyze},
			wantFlagged: false,
		},
		{
			name:             "dead-tuple ratio at threshold and above floor flags vacuum",
			row:              vacuumStatsRow{liveTuples: 40_000, deadTuples: 10_000, modSinceAnalyze: 0, lastAnalyzedAt: &freshAnalyze},
			wantFlagged:      true,
			wantNeedsVacuum:  true,
			wantNeedsAnalyze: false,
			wantReasonHas:    "dead tuples 10000",
		},
		{
			name:             "modifications above threshold with stale analyze flags analyze",
			row:              vacuumStatsRow{liveTuples: 1_000_000, deadTuples: 0, modSinceAnalyze: analyzeModificationThreshold, lastAnalyzedAt: &staleAnalyze},
			wantFlagged:      true,
			wantNeedsVacuum:  false,
			wantNeedsAnalyze: true,
			wantReasonHas:    "modifications since analyze",
		},
		{
			name:             "modifications above threshold with fresh analyze is not flagged",
			row:              vacuumStatsRow{liveTuples: 1_000_000, deadTuples: 0, modSinceAnalyze: analyzeModificationThreshold, lastAnalyzedAt: &freshAnalyze},
			wantFlagged:      false,
			wantNeedsAnalyze: false,
		},
		{
			name:             "never-analyzed partition above modification threshold flags analyze",
			row:              vacuumStatsRow{liveTuples: 1_000_000, deadTuples: 0, modSinceAnalyze: analyzeModificationThreshold, lastAnalyzedAt: nil},
			wantFlagged:      true,
			wantNeedsAnalyze: true,
			wantReasonHas:    "never analyzed",
		},
		{
			name:             "both triggers fire concurrently",
			row:              vacuumStatsRow{liveTuples: 100_000, deadTuples: 50_000, modSinceAnalyze: analyzeModificationThreshold, lastAnalyzedAt: &staleAnalyze},
			wantFlagged:      true,
			wantNeedsVacuum:  true,
			wantNeedsAnalyze: true,
			wantReasonHas:    "dead tuples",
		},
		{
			name:        "empty partition is not flagged",
			row:         vacuumStatsRow{liveTuples: 0, deadTuples: 0, modSinceAnalyze: 0, lastAnalyzedAt: nil},
			wantFlagged: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate, flagged := vacuumAssessment(tc.row, now)
			assert.Equal(t, tc.wantFlagged, flagged, "unexpected flagged result")
			assert.Equal(t, tc.wantNeedsVacuum, candidate.needsVacuum, "unexpected needsVacuum")
			assert.Equal(t, tc.wantNeedsAnalyze, candidate.needsAnalyze, "unexpected needsAnalyze")
			if tc.wantFlagged {
				assert.True(t, strings.Contains(candidate.reason, tc.wantReasonHas),
					"reason %q does not mention expected substring %q", candidate.reason, tc.wantReasonHas)
			} else {
				assert.Empty(t, candidate.reason, "expected empty reason when not flagged")
			}
		})
	}
}

// TestBuildVacuumSQL verifies the VACUUM (ANALYZE) statement is produced with
// both schema and table name quoted via pgx.Identifier.Sanitize. VACUUM FULL
// must never appear in the generated SQL.
func TestBuildVacuumSQL(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		table  string
		want   string
	}{
		{
			name:   "ordinary identifiers are double-quoted",
			schema: "graph",
			table:  "edge_1",
			want:   `vacuum (analyze) "graph"."edge_1"`,
		},
		{
			name:   "embedded double quote is escaped by doubling",
			schema: `evil"schema`,
			table:  "edge_1",
			want:   `vacuum (analyze) "evil""schema"."edge_1"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildVacuumSQL(tc.schema, tc.table)
			assert.Equal(t, tc.want, got)
			assert.False(t, strings.Contains(strings.ToLower(got), "full"),
				"generated VACUUM statement must not contain FULL: %q", got)
		})
	}
}

// TestGinIndexQuery_FiltersByAmAndParents guards the SQL string that scans
// for GIN flush candidates against accidental loosening of its filters,
// since this query controls the blast radius of gin_clean_pending_list.
func TestGinIndexQuery_FiltersByAmAndParents(t *testing.T) {
	for _, fragment := range []string{
		"a.amname = 'gin'",
		"p.relname in ('node', 'edge')",
	} {
		assert.Contains(t, sqlSelectGraphPartitionGinIndexes, fragment,
			"GIN listing SQL is missing required filter %q; relaxing this filter risks flushing unrelated indexes", fragment)
	}
}

// TestVacuumStatsQuery_FiltersByParents guards the SQL string that scans for
// vacuum candidates against accidental loosening of its filters, since this
// query controls the blast radius of VACUUM (ANALYZE).
func TestVacuumStatsQuery_FiltersByParents(t *testing.T) {
	assert.Contains(t, sqlSelectGraphPartitionVacuumStats, "p.relname in ('node', 'edge')",
		"vacuum stats SQL is missing required parent-table filter; relaxing this filter risks vacuuming unrelated tables")
}
