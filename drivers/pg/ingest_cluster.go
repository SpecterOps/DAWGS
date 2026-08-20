package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pgquery "github.com/specterops/dawgs/drivers/pg/query"
)

type ingestClusterDB interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (s *ingestEngine) clusterTargetPartitions(ctx context.Context) error {
	if s.clusterDB == nil {
		return fmt.Errorf("PostgreSQL ingest clustering database is not configured")
	}

	for _, partition := range []string{
		s.graphTarget.Partitions.Node.Name,
		s.graphTarget.Partitions.Edge.Name,
	} {
		indexName, err := findIngestHashIndex(ctx, s.clusterDB, partition)
		if err != nil {
			return err
		}

		statement := fmt.Sprintf(
			"cluster %s using %s;",
			pgx.Identifier{partition}.Sanitize(),
			pgx.Identifier{indexName}.Sanitize(),
		)
		if _, err := s.clusterDB.Exec(ctx, statement); err != nil {
			return fmt.Errorf("cluster PostgreSQL ingest target partition %q: %w", partition, err)
		}
	}

	return nil
}

func findIngestHashIndex(
	ctx context.Context,
	db ingestClusterDB,
	partition string,
) (string, error) {
	regclass := pgx.Identifier{partition}.Sanitize()
	rows, err := db.Query(ctx, pgquery.FormatFindIngestHashIndex(), regclass)
	if err != nil {
		return "", fmt.Errorf("find id_hash index for PostgreSQL ingest target partition %q: %w", partition, err)
	}
	defer rows.Close()

	var indexNames []string
	for rows.Next() {
		var indexName string
		if err := rows.Scan(&indexName); err != nil {
			return "", fmt.Errorf("scan id_hash index for PostgreSQL ingest target partition %q: %w", partition, err)
		}
		indexNames = append(indexNames, indexName)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("read id_hash indexes for PostgreSQL ingest target partition %q: %w", partition, err)
	}
	if len(indexNames) != 1 || indexNames[0] == "" {
		return "", fmt.Errorf(
			"PostgreSQL ingest target partition %q requires exactly one valid single-column B-tree id_hash index; found %d",
			partition,
			len(indexNames),
		)
	}

	return indexNames[0], nil
}
