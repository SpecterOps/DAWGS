package pg

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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

// Optimize satisfies the graph.Optimizer interface. It performs a pre-flight
// sweep of orphaned REINDEX CONCURRENTLY artifacts, identifies btree indexes
// on partitions of the node and edge tables whose leaf density or
// fragmentation cross the rebuild thresholds, and rebuilds each flagged index
// with REINDEX INDEX CONCURRENTLY. Per-candidate failures are logged and do
// not abort the pass; the loop honors ctx cancellation between candidates.
func (s *Driver) Optimize(ctx context.Context) error {
	if installed, err := s.pgstattupleInstalled(ctx); err != nil {
		return fmt.Errorf("checking pgstattuple extension: %w", err)
	} else if !installed {
		slog.WarnContext(ctx, "Index optimization skipped: pgstattuple extension is not installed; verify the DAWGS schema bootstrap completed successfully")
		return nil
	}

	s.cleanupOrphanedReindexArtifacts(ctx)

	indexes, err := s.listGraphPartitionBtreeIndexes(ctx)
	if err != nil {
		return fmt.Errorf("listing graph partition btree indexes: %w", err)
	}

	slog.InfoContext(ctx, fmt.Sprintf("Index optimization assessment starting: %d btree index(es) under consideration", len(indexes)))

	var (
		candidates []indexCandidate
		totalBytes int64
	)
	for _, idx := range indexes {
		density, fragmentation, err := s.measureIndexBloat(ctx, idx.oid)
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
				"Index optimization candidate: %s.%s on %s (size=%d bytes, leaf_density=%.1f%%, fragmentation=%.1f%%, reason=%s)",
				candidate.schema, candidate.index, candidate.table,
				candidate.sizeBytes, candidate.leafDensity, candidate.fragmentation, candidate.reason,
			))
		}
	}

	slog.InfoContext(ctx, fmt.Sprintf(
		"Index optimization assessment complete: %d candidate(s) totaling %d bytes",
		len(candidates), totalBytes,
	))

	// Process smallest candidates first so that a mid-pass ctx cancellation
	// still results in the maximum number of completed rebuilds.
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].sizeBytes < candidates[j].sizeBytes
	})

	s.reindexCandidates(ctx, candidates)
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

func (s *Driver) pgstattupleInstalled(ctx context.Context) (bool, error) {
	var installed bool
	if err := s.pool.QueryRow(ctx, sqlPgstattupleInstalled).Scan(&installed); err != nil {
		return false, err
	}
	return installed, nil
}

func (s *Driver) listGraphPartitionBtreeIndexes(ctx context.Context) ([]indexRow, error) {
	rows, err := s.pool.Query(ctx, sqlSelectGraphPartitionBtreeIndexes)
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

func (s *Driver) measureIndexBloat(ctx context.Context, indexOID uint32) (float64, float64, error) {
	var density, fragmentation float64
	if err := s.pool.QueryRow(ctx, sqlSelectIndexBloatMetrics, indexOID).Scan(&density, &fragmentation); err != nil {
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
func (s *Driver) cleanupOrphanedReindexArtifacts(ctx context.Context) {
	orphans, err := s.listOrphanedReindexArtifacts(ctx)
	if err != nil {
		slog.WarnContext(ctx, fmt.Sprintf("Index optimization cleanup: failed to scan for orphaned reindex artifacts; continuing: %v", err))
		return
	}
	if len(orphans) == 0 {
		return
	}

	slog.InfoContext(ctx, fmt.Sprintf("Index optimization cleanup: dropping %d orphaned reindex artifact(s)", len(orphans)))
	for _, o := range orphans {
		if _, err := s.pool.Exec(ctx, buildDropInvalidIndexSQL(o.schema, o.name)); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("Index optimization cleanup: failed to drop orphaned reindex artifact %s.%s; continuing: %v", o.schema, o.name, err))
			continue
		}
		slog.InfoContext(ctx, fmt.Sprintf("Index optimization cleanup: dropped orphaned reindex artifact %s.%s", o.schema, o.name))
	}
}

func (s *Driver) listOrphanedReindexArtifacts(ctx context.Context) ([]orphanedReindexArtifact, error) {
	rows, err := s.pool.Query(ctx, sqlSelectOrphanedReindexArtifacts)
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
func (s *Driver) reindexCandidates(ctx context.Context, candidates []indexCandidate) {
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
		if _, err := s.pool.Exec(ctx, buildReindexSQL(c.schema, c.index)); err != nil {
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
