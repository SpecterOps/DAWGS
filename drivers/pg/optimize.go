package pg

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// bloatedIndexLeafDensityThreshold is the average leaf-page fill percentage
	// below which a btree index is considered dense enough that REINDEX would
	// reclaim significant space. Calibrated against seven production tenant
	// samples whose live indexes regularly fell to 21-55%, against a freshly
	// rebuilt baseline (edge_1_pkey on a recently restored tenant) of 73.8%.
	bloatedIndexLeafDensityThreshold = 60.0

	// highIndexFragmentationThreshold is the leaf-page fragmentation percentage
	// at or above which a btree index has accumulated enough out-of-order page
	// splits to warrant a rebuild even when leaf density alone has not crossed
	// its threshold.
	highIndexFragmentationThreshold = 40.0

	// ginPendingPagesFlushThreshold is the pending-list page count at or above
	// which a GIN index is flagged for an explicit gin_clean_pending_list call.
	// At Postgres's 8 KiB page size this is 16 MiB, four times the default
	// gin_pending_list_limit (4 MiB), giving autovacuum room to handle the
	// common case while ensuring optimization addresses outliers that have
	// outpaced background cleanup. Starting default; not yet calibrated
	// against fleet samples.
	ginPendingPagesFlushThreshold = 2048

	// vacuumDeadTupleRatioThreshold mirrors Postgres's default
	// autovacuum_vacuum_scale_factor (0.20). Tables whose dead-tuple ratio
	// reaches this value are flagged for an explicit VACUUM (ANALYZE).
	vacuumDeadTupleRatioThreshold = 0.20

	// vacuumDeadTupleAbsoluteFloor prevents thrashing tiny partitions where
	// a 20% dead-tuple ratio represents only a handful of rows.
	vacuumDeadTupleAbsoluteFloor = 10000

	// analyzeModificationThreshold is the n_mod_since_analyze count at or
	// above which an ANALYZE is warranted, provided no analyze has run
	// within analyzeStalenessWindow.
	analyzeModificationThreshold = 50000

	// analyzeStalenessWindow is the maximum age of the most recent
	// (auto)analyze that suppresses an ANALYZE rerun. Aligned with the
	// pipeline-level optimization cooldown.
	analyzeStalenessWindow = 24 * time.Hour
)

const (
	sqlPgstattupleInstalled = `select exists(select 1 from pg_extension where extname = 'pgstattuple')`

	sqlSelectGraphPartitionBtreeIndexes = `
		select i.oid                  as index_oid,
		       n.nspname              as schema_name,
		       c.relname              as table_name,
		       i.relname              as index_name,
		       pg_relation_size(i.oid) as index_size_bytes
		from pg_inherits inh
		         join pg_class p on p.oid = inh.inhparent
		         join pg_class c on c.oid = inh.inhrelid
		         join pg_namespace n on n.oid = c.relnamespace
		         join pg_index x on x.indrelid = c.oid
		         join pg_class i on i.oid = x.indexrelid
		         join pg_am a on a.oid = i.relam
		where p.relname in ('node', 'edge')
		  and p.relnamespace = n.oid
		  and a.amname = 'btree'
		order by c.relname, i.relname`

	sqlSelectIndexBloatMetrics = `select avg_leaf_density, leaf_fragmentation from pgstatindex($1::regclass)`

	// sqlSelectOrphanedReindexArtifacts identifies INVALID btree indexes left
	// behind by aborted REINDEX CONCURRENTLY runs on partitions of the node
	// and edge tables. Postgres names the in-progress copy <original>_ccnew or
	// <original>_ccnew<N>; on failure or cancellation those copies remain in
	// pg_index with indisvalid = false until explicitly dropped.
	sqlSelectOrphanedReindexArtifacts = `
		select n.nspname as schema_name,
		       i.relname as index_name
		from pg_inherits inh
		         join pg_class p on p.oid = inh.inhparent
		         join pg_class c on c.oid = inh.inhrelid
		         join pg_namespace n on n.oid = c.relnamespace
		         join pg_index x on x.indrelid = c.oid
		         join pg_class i on i.oid = x.indexrelid
		         join pg_am a on a.oid = i.relam
		where p.relname in ('node', 'edge')
		  and p.relnamespace = n.oid
		  and a.amname = 'btree'
		  and x.indisvalid = false
		  and i.relname ~ '_ccnew[0-9]*$'
		order by n.nspname, i.relname`

	sqlSelectGraphPartitionGinIndexes = `
		select i.oid                  as index_oid,
		       n.nspname              as schema_name,
		       c.relname              as table_name,
		       i.relname              as index_name,
		       pg_relation_size(i.oid) as index_size_bytes
		from pg_inherits inh
		         join pg_class p on p.oid = inh.inhparent
		         join pg_class c on c.oid = inh.inhrelid
		         join pg_namespace n on n.oid = c.relnamespace
		         join pg_index x on x.indrelid = c.oid
		         join pg_class i on i.oid = x.indexrelid
		         join pg_am a on a.oid = i.relam
		where p.relname in ('node', 'edge')
		  and p.relnamespace = n.oid
		  and a.amname = 'gin'
		order by c.relname, i.relname`

	sqlSelectGinPendingMetrics = `select pending_pages, pending_tuples from pgstatginindex($1::regclass)`

	// sqlCleanGinPendingList flushes the pending list of a single GIN index.
	// gin_clean_pending_list returns the number of pages it processed; the
	// optimizer ignores the return value and relies on per-call error handling.
	sqlCleanGinPendingList = `select gin_clean_pending_list($1::regclass)`

	// sqlSelectGraphPartitionVacuumStats reads pg_stat_user_tables for every
	// partition of the node and edge tables. The left join keeps partitions
	// that have never been touched by autovacuum visible (the stats collector
	// drops rows for relations that have not yet accumulated activity), so
	// callers must treat zero/NULL as "no data" rather than "definitely zero".
	sqlSelectGraphPartitionVacuumStats = `
		select n.nspname                                 as schema_name,
		       c.relname                                 as table_name,
		       coalesce(s.n_live_tup, 0)                 as n_live_tup,
		       coalesce(s.n_dead_tup, 0)                 as n_dead_tup,
		       coalesce(s.n_mod_since_analyze, 0)        as n_mod_since_analyze,
		       greatest(s.last_analyze, s.last_autoanalyze) as last_analyzed_at
		from pg_inherits inh
		         join pg_class p on p.oid = inh.inhparent
		         join pg_class c on c.oid = inh.inhrelid
		         join pg_namespace n on n.oid = c.relnamespace
		         left join pg_stat_user_tables s
		                on s.schemaname = n.nspname
		               and s.relname    = c.relname
		where p.relname in ('node', 'edge')
		  and p.relnamespace = n.oid
		order by n.nspname, c.relname`
)

// indexRow is a candidate index discovered by the listing query, prior to
// per-index pgstatindex assessment.
type indexRow struct {
	oid       uint32
	schema    string
	table     string
	index     string
	sizeBytes int64
}

// indexCandidate is an index whose pgstatindex measurement caused it to be
// flagged for rebuild by needsReindex.
type indexCandidate struct {
	indexRow
	leafDensity   float64
	fragmentation float64
	reason        string
}

// Optimize satisfies the graph.Optimizer interface. It runs the maintenance
// phases registered below in order against partitions of the node and edge
// tables. Phases are independent: per-candidate failures within a phase are
// logged at WARN and do not abort subsequent phases, and loops honor ctx
// cancellation between candidates. Optimize returns an error only when the
// pre-flight pgstattuple check itself fails; a missing extension is logged
// and treated as a no-op so that callers can run the daemon unconditionally
// against environments where pgstattuple cannot be installed.
//
// Phase ordering rationale: orphan cleanup is cheap disk reclamation that
// reduces noise for the rest of the pass; GIN pending-list flushes precede
// vacuum so autovacuum's own opportunistic flush is not duplicated; vacuum
// precedes btree reindex so reclaimed space is reflected in the bloat
// measurement that drives the rebuild decision.
func (s *Driver) Optimize(ctx context.Context) error {
	return optimize(ctx, s.pool)
}

// optimize is the package-level implementation that the four maintenance
// phases dispatch through. Splitting Optimize from its receiver lets unit
// tests drive the phase wiring against a fake driver without standing up a
// real *pgxpool.Pool; the *Driver method is a forwarder.
func optimize(ctx context.Context, db driver) error {
	if installed, err := pgstattupleInstalled(ctx, db); err != nil {
		return fmt.Errorf("checking pgstattuple extension: %w", err)
	} else if !installed {
		slog.WarnContext(ctx, "Index optimization skipped: pgstattuple extension is not installed; verify the DAWGS schema bootstrap completed successfully")
		return nil
	}

	cleanupOrphanedReindexArtifacts(ctx, db)
	flushGinPendingLists(ctx, db)
	vacuumGraphPartitions(ctx, db)
	reindexBloatedBtreeIndexes(ctx, db)
	return nil
}

// needsReindex applies the rebuild thresholds to a measured index and returns
// a short human-readable reason when the index is flagged. Pure function;
// unit-tested in optimize_test.go.
func needsReindex(c indexCandidate) (string, bool) {
	if c.leafDensity < bloatedIndexLeafDensityThreshold {
		return fmt.Sprintf("leaf density %.1f%% below %.1f%% threshold", c.leafDensity, bloatedIndexLeafDensityThreshold), true
	}
	if c.fragmentation >= highIndexFragmentationThreshold {
		return fmt.Sprintf("fragmentation %.1f%% at or above %.1f%% threshold", c.fragmentation, highIndexFragmentationThreshold), true
	}
	return "", false
}

// reindexBloatedBtreeIndexes discovers btree indexes on partitions of the
// node and edge tables, measures each index's leaf density and fragmentation
// with pgstatindex, and rebuilds those flagged by needsReindex with REINDEX
// INDEX CONCURRENTLY. Per-candidate failures are logged at WARN and never
// fatal; the loop honors ctx cancellation between candidates. Candidates are
// processed smallest first so that an early cancellation still produces the
// maximum number of completed rebuilds.
func reindexBloatedBtreeIndexes(ctx context.Context, db driver) {
	indexes, err := listGraphPartitionBtreeIndexes(ctx, db)
	if err != nil {
		slog.WarnContext(ctx, fmt.Sprintf("Btree reindex scan failed; continuing: %v", err))
		return
	}

	slog.InfoContext(ctx, fmt.Sprintf("Btree reindex assessment starting: %d index(es) under consideration", len(indexes)))

	var (
		candidates []indexCandidate
		totalBytes int64
	)
	for _, idx := range indexes {
		density, fragmentation, err := measureIndexBloat(ctx, db, idx.oid)
		if err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("Skipping bloat assessment for index %s.%s: %v", idx.schema, idx.index, err))
			continue
		}

		candidate := indexCandidate{indexRow: idx, leafDensity: density, fragmentation: fragmentation}
		if reason, flagged := needsReindex(candidate); flagged {
			candidate.reason = reason
			candidates = append(candidates, candidate)
			totalBytes += candidate.sizeBytes
			slog.InfoContext(ctx, fmt.Sprintf(
				"Btree reindex candidate: %s.%s on %s (size=%d bytes, leaf_density=%.1f%%, fragmentation=%.1f%%, reason=%s)",
				candidate.schema, candidate.index, candidate.table,
				candidate.sizeBytes, candidate.leafDensity, candidate.fragmentation, candidate.reason,
			))
		}
	}

	slog.InfoContext(ctx, fmt.Sprintf(
		"Btree reindex assessment complete: %d candidate(s) totaling %d bytes",
		len(candidates), totalBytes,
	))

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].sizeBytes < candidates[j].sizeBytes
	})

	reindexCandidates(ctx, db, candidates)
}

func pgstattupleInstalled(ctx context.Context, db driver) (bool, error) {
	var installed bool
	if err := db.QueryRow(ctx, sqlPgstattupleInstalled).Scan(&installed); err != nil {
		return false, err
	}
	return installed, nil
}

func listGraphPartitionBtreeIndexes(ctx context.Context, db driver) ([]indexRow, error) {
	rows, err := db.Query(ctx, sqlSelectGraphPartitionBtreeIndexes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []indexRow
	for rows.Next() {
		var r indexRow
		if err := rows.Scan(&r.oid, &r.schema, &r.table, &r.index, &r.sizeBytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func measureIndexBloat(ctx context.Context, db driver, indexOID uint32) (float64, float64, error) {
	var density, fragmentation float64
	if err := db.QueryRow(ctx, sqlSelectIndexBloatMetrics, indexOID).Scan(&density, &fragmentation); err != nil {
		return 0, 0, err
	}
	return density, fragmentation, nil
}

// orphanedReindexArtifact is an INVALID btree index left behind by an aborted
// REINDEX CONCURRENTLY run on a node or edge partition.
type orphanedReindexArtifact struct {
	schema string
	name   string
}

// cleanupOrphanedReindexArtifacts scans for and drops INVALID _ccnew indexes
// left behind by previously aborted REINDEX CONCURRENTLY runs. Failures are
// logged at WARN and never fatal: an orphan wastes disk but does not block
// productive rebuilds, so a stuck cleanup must not gate the rest of the pass.
func cleanupOrphanedReindexArtifacts(ctx context.Context, db driver) {
	orphans, err := listOrphanedReindexArtifacts(ctx, db)
	if err != nil {
		slog.WarnContext(ctx, fmt.Sprintf("Index optimization cleanup: failed to scan for orphaned reindex artifacts; continuing: %v", err))
		return
	}

	slog.InfoContext(ctx, fmt.Sprintf("Orphan reindex cleanup assessment starting: %d artifact(s) under consideration", len(orphans)))
	if len(orphans) == 0 {
		slog.InfoContext(ctx, "Orphan reindex cleanup assessment complete: 0 artifact(s) to drop")
		return
	}

	slog.InfoContext(ctx, fmt.Sprintf("Orphan reindex cleanup assessment complete: %d artifact(s) to drop", len(orphans)))
	for _, o := range orphans {
		if _, err := db.Exec(ctx, buildDropInvalidIndexSQL(o.schema, o.name)); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("Index optimization cleanup: failed to drop orphaned reindex artifact %s.%s; continuing: %v", o.schema, o.name, err))
			continue
		}
		slog.InfoContext(ctx, fmt.Sprintf("Index optimization cleanup: dropped orphaned reindex artifact %s.%s", o.schema, o.name))
	}
}

func listOrphanedReindexArtifacts(ctx context.Context, db driver) ([]orphanedReindexArtifact, error) {
	rows, err := db.Query(ctx, sqlSelectOrphanedReindexArtifacts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []orphanedReindexArtifact
	for rows.Next() {
		var o orphanedReindexArtifact
		if err := rows.Scan(&o.schema, &o.name); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// reindexCandidates rebuilds each flagged index with REINDEX INDEX
// CONCURRENTLY, in the order supplied (Optimize sorts ascending by size).
// Per-candidate failures are logged at WARN and the loop continues; ctx
// cancellation aborts further candidates but in-flight REINDEX statements
// must run to completion in Postgres to avoid leaving _ccnew artifacts.
func reindexCandidates(ctx context.Context, db driver, candidates []indexCandidate) {
	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("Index optimization rebuild cancelled before processing %s.%s: %v", c.schema, c.index, err))
			return
		}

		slog.InfoContext(ctx, fmt.Sprintf(
			"Index optimization rebuild starting: %s.%s on %s (size=%d bytes, leaf_density=%.1f%%, fragmentation=%.1f%%)",
			c.schema, c.index, c.table, c.sizeBytes, c.leafDensity, c.fragmentation,
		))

		started := time.Now()
		if _, err := db.Exec(ctx, buildReindexSQL(c.schema, c.index)); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf(
				"Index optimization rebuild failed for %s.%s after %s; continuing with next candidate: %v",
				c.schema, c.index, time.Since(started), err,
			))
			continue
		}

		slog.InfoContext(ctx, fmt.Sprintf(
			"Index optimization rebuild complete: %s.%s in %s",
			c.schema, c.index, time.Since(started),
		))
	}
}

// buildReindexSQL composes a REINDEX INDEX CONCURRENTLY statement with a
// safely quoted schema-qualified identifier. REINDEX does not accept query
// parameters; quoting is the only defense against malformed identifiers.
func buildReindexSQL(schema, name string) string {
	return "reindex index concurrently " + pgx.Identifier{schema, name}.Sanitize()
}

// buildDropInvalidIndexSQL composes a DROP INDEX CONCURRENTLY IF EXISTS
// statement for cleaning up an orphaned _ccnew artifact. IF EXISTS prevents
// a race where another session has already dropped the same orphan.
func buildDropInvalidIndexSQL(schema, name string) string {
	return "drop index concurrently if exists " + pgx.Identifier{schema, name}.Sanitize()
}

// ginIndexRow is a candidate GIN index discovered by the listing query, prior
// to per-index pgstatginindex assessment.
type ginIndexRow struct {
	oid       uint32
	schema    string
	table     string
	index     string
	sizeBytes int64
}

// ginFlushCandidate is a GIN index whose pending-list measurement caused it
// to be flagged for an explicit gin_clean_pending_list call by needsGinFlush.
type ginFlushCandidate struct {
	ginIndexRow
	pendingPages  int64
	pendingTuples int64
	reason        string
}

// needsGinFlush applies the pending-list threshold to a measured GIN index.
// Pure function; unit-tested in optimize_test.go.
func needsGinFlush(c ginFlushCandidate) (string, bool) {
	if c.pendingPages >= ginPendingPagesFlushThreshold {
		return fmt.Sprintf(
			"pending pages %d at or above %d page threshold (%.1f MiB)",
			c.pendingPages, ginPendingPagesFlushThreshold,
			float64(c.pendingPages*8192)/(1024*1024),
		), true
	}
	return "", false
}

// flushGinPendingLists discovers GIN indexes on partitions of the node and
// edge tables, measures each index's pending-list size, and calls
// gin_clean_pending_list on those whose pending pages have crossed the
// threshold. Per-candidate failures are logged at WARN and never fatal: a
// stuck flush wastes time on the next pass but does not block the rest of
// the optimization phases.
func flushGinPendingLists(ctx context.Context, db driver) {
	indexes, err := listGraphPartitionGinIndexes(ctx, db)
	if err != nil {
		slog.WarnContext(ctx, fmt.Sprintf("Index optimization GIN scan failed; continuing: %v", err))
		return
	}

	slog.InfoContext(ctx, fmt.Sprintf("GIN flush assessment starting: %d index(es) under consideration", len(indexes)))

	var (
		candidates        []ginFlushCandidate
		totalPendingPages int64
	)
	for _, idx := range indexes {
		pages, tuples, err := measureGinPending(ctx, db, idx.oid)
		if err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("Skipping GIN pending-list assessment for index %s.%s: %v", idx.schema, idx.index, err))
			continue
		}

		c := ginFlushCandidate{ginIndexRow: idx, pendingPages: pages, pendingTuples: tuples}
		if reason, flagged := needsGinFlush(c); flagged {
			c.reason = reason
			candidates = append(candidates, c)
			totalPendingPages += c.pendingPages
			slog.InfoContext(ctx, fmt.Sprintf(
				"GIN flush candidate: %s.%s on %s (size=%d bytes, pending_pages=%d, pending_tuples=%d, reason=%s)",
				c.schema, c.index, c.table, c.sizeBytes, c.pendingPages, c.pendingTuples, c.reason,
			))
		}
	}

	slog.InfoContext(ctx, fmt.Sprintf(
		"GIN flush assessment complete: %d candidate(s) totaling %d pending page(s)",
		len(candidates), totalPendingPages,
	))

	if len(candidates) == 0 {
		return
	}

	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("GIN flush cancelled before processing %s.%s: %v", c.schema, c.index, err))
			return
		}

		started := time.Now()
		if _, err := db.Exec(ctx, sqlCleanGinPendingList, c.oid); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf(
				"GIN flush failed for %s.%s after %s; continuing with next candidate: %v",
				c.schema, c.index, time.Since(started), err,
			))
			continue
		}

		slog.InfoContext(ctx, fmt.Sprintf(
			"GIN flush complete: %s.%s in %s (was %d pending pages, %d pending tuples)",
			c.schema, c.index, time.Since(started), c.pendingPages, c.pendingTuples,
		))
	}
}

func listGraphPartitionGinIndexes(ctx context.Context, db driver) ([]ginIndexRow, error) {
	rows, err := db.Query(ctx, sqlSelectGraphPartitionGinIndexes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ginIndexRow
	for rows.Next() {
		var r ginIndexRow
		if err := rows.Scan(&r.oid, &r.schema, &r.table, &r.index, &r.sizeBytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func measureGinPending(ctx context.Context, db driver, indexOID uint32) (int64, int64, error) {
	var pendingPages, pendingTuples int64
	if err := db.QueryRow(ctx, sqlSelectGinPendingMetrics, indexOID).Scan(&pendingPages, &pendingTuples); err != nil {
		return 0, 0, err
	}
	return pendingPages, pendingTuples, nil
}

// vacuumStatsRow is a snapshot of pg_stat_user_tables for one partition of
// the node or edge tables. lastAnalyzedAt is nil when the table has never
// been analyzed (manually or by autovacuum).
type vacuumStatsRow struct {
	schema          string
	table           string
	liveTuples      int64
	deadTuples      int64
	modSinceAnalyze int64
	lastAnalyzedAt  *time.Time
}

// vacuumCandidate is a partition flagged by vacuumAssessment. needsVacuum
// and needsAnalyze are independent and may both be true; the executor issues
// a single VACUUM (ANALYZE) when either fires for the analyze trigger.
type vacuumCandidate struct {
	vacuumStatsRow
	needsVacuum  bool
	needsAnalyze bool
	reason       string
}

// vacuumAssessment applies the vacuum and analyze thresholds to a measured
// partition and returns a candidate plus a flagged bool. Pure function;
// unit-tested in optimize_test.go. now is injected to keep the staleness
// test deterministic.
func vacuumAssessment(r vacuumStatsRow, now time.Time) (vacuumCandidate, bool) {
	candidate := vacuumCandidate{vacuumStatsRow: r}
	var reasons []string

	if r.deadTuples >= vacuumDeadTupleAbsoluteFloor {
		total := r.liveTuples + r.deadTuples
		if total > 0 {
			ratio := float64(r.deadTuples) / float64(total)
			if ratio >= vacuumDeadTupleRatioThreshold {
				candidate.needsVacuum = true
				reasons = append(reasons, fmt.Sprintf(
					"dead tuples %d (%.1f%% of %d) at or above %.0f%% threshold",
					r.deadTuples, ratio*100, total, vacuumDeadTupleRatioThreshold*100,
				))
			}
		}
	}

	if r.modSinceAnalyze >= analyzeModificationThreshold {
		stale := r.lastAnalyzedAt == nil || now.Sub(*r.lastAnalyzedAt) >= analyzeStalenessWindow
		if stale {
			candidate.needsAnalyze = true
			ageMsg := "never analyzed"
			if r.lastAnalyzedAt != nil {
				ageMsg = fmt.Sprintf("last analyzed %s ago", now.Sub(*r.lastAnalyzedAt).Round(time.Minute))
			}
			reasons = append(reasons, fmt.Sprintf(
				"modifications since analyze %d at or above %d threshold and %s",
				r.modSinceAnalyze, analyzeModificationThreshold, ageMsg,
			))
		}
	}

	if !candidate.needsVacuum && !candidate.needsAnalyze {
		return candidate, false
	}
	candidate.reason = strings.Join(reasons, "; ")
	return candidate, true
}

// vacuumGraphPartitions discovers partitions of the node and edge tables,
// reads pg_stat_user_tables for each, and runs VACUUM (ANALYZE) on those
// flagged by vacuumAssessment. Per-candidate failures are logged at WARN and
// never fatal. The loop honors ctx cancellation between partitions; an
// in-flight VACUUM aborts at the next safe point when ctx is cancelled.
func vacuumGraphPartitions(ctx context.Context, db driver) {
	stats, err := listGraphPartitionVacuumStats(ctx, db)
	if err != nil {
		slog.WarnContext(ctx, fmt.Sprintf("Vacuum assessment scan failed; continuing: %v", err))
		return
	}

	slog.InfoContext(ctx, fmt.Sprintf("Vacuum assessment starting: %d partition(s) under consideration", len(stats)))

	now := time.Now()
	var (
		candidates     []vacuumCandidate
		vacuumFlagged  int
		analyzeFlagged int
	)
	for _, r := range stats {
		if c, flagged := vacuumAssessment(r, now); flagged {
			candidates = append(candidates, c)
			if c.needsVacuum {
				vacuumFlagged++
			}
			if c.needsAnalyze {
				analyzeFlagged++
			}
			slog.InfoContext(ctx, fmt.Sprintf(
				"Vacuum candidate: %s.%s (live=%d, dead=%d, mod_since_analyze=%d, reason=%s)",
				c.schema, c.table, c.liveTuples, c.deadTuples, c.modSinceAnalyze, c.reason,
			))
		}
	}

	slog.InfoContext(ctx, fmt.Sprintf(
		"Vacuum assessment complete: %d candidate(s) (%d flagged for VACUUM, %d for ANALYZE)",
		len(candidates), vacuumFlagged, analyzeFlagged,
	))

	if len(candidates) == 0 {
		return
	}

	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("Vacuum cancelled before processing %s.%s: %v", c.schema, c.table, err))
			return
		}

		started := time.Now()
		if _, err := db.Exec(ctx, buildVacuumSQL(c.schema, c.table)); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf(
				"Vacuum failed for %s.%s after %s; continuing with next candidate: %v",
				c.schema, c.table, time.Since(started), err,
			))
			continue
		}

		slog.InfoContext(ctx, fmt.Sprintf(
			"Vacuum complete: %s.%s in %s",
			c.schema, c.table, time.Since(started),
		))
	}
}

func listGraphPartitionVacuumStats(ctx context.Context, db driver) ([]vacuumStatsRow, error) {
	rows, err := db.Query(ctx, sqlSelectGraphPartitionVacuumStats)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []vacuumStatsRow
	for rows.Next() {
		var r vacuumStatsRow
		if err := rows.Scan(&r.schema, &r.table, &r.liveTuples, &r.deadTuples, &r.modSinceAnalyze, &r.lastAnalyzedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// buildVacuumSQL composes a VACUUM (ANALYZE) statement with a safely quoted
// schema-qualified identifier. VACUUM does not accept query parameters, and
// VACUUM (ANALYZE) is preferred over a separate ANALYZE because the planner
// statistics ride along at no additional cost when a vacuum is already
// scheduled. VACUUM FULL is intentionally never emitted: it takes
// AccessExclusiveLock and rewrites the table.
func buildVacuumSQL(schema, name string) string {
	return "vacuum (analyze) " + pgx.Identifier{schema, name}.Sanitize()
}
