package pg

import (
	"context"
	"fmt"
	"log/slog"
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

// Optimize satisfies the graph.Optimizer interface. The current phase performs
// a read-only assessment: it identifies btree indexes on partitions of the
// node and edge tables whose leaf density or fragmentation cross the rebuild
// thresholds and logs the candidates. No DDL is executed in this phase.
func (s *Driver) Optimize(ctx context.Context) error {
	if installed, err := s.pgstattupleInstalled(ctx); err != nil {
		return fmt.Errorf("checking pgstattuple extension: %w", err)
	} else if !installed {
		slog.WarnContext(ctx, "Index optimization skipped: pgstattuple extension is not installed; verify the DAWGS schema bootstrap completed successfully")
		return nil
	}

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
