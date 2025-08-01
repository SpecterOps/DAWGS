package changestream

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	LAST_NODE_CHANGE_SQL = `select cs.properties_hash != $2 as has_changed, cs.change_type from node_change_stream cs where cs.node_id = $1 order by created_at desc limit 1;`
	ASSERT_TABLE_SQL     = `create table if not exists node_change_stream (
							id bigint generated always as identity not null,
							node_id text not null,
							kind_ids smallint[] not null,
							properties_hash bytea not null,
							property_fields text[] not null,
							change_type integer not null,
							created_at timestamp with time zone not null,

							primary key (id, created_at)
						) partition by range (created_at);

						create index if not exists node_change_stream_node_id_index on node_change_stream using hash (node_id);
						create index if not exists node_change_stream_created_at_index on node_change_stream using btree (created_at);`

	INSERT_NODE_CHANGE_SQL = `insert into node_change_stream (node_id, kind_ids, property_fields, properties_hash, change_type, created_at) select unnest($1::text[]), unnest($2::text[])::int2[], unnest($3::text[])::text[], unnest($4::bytea[]), 0, now();`

	CREATE_PARTITIONS_SQL_FMT = `
create table if not exists node_change_stream_%s partition of node_change_stream for values from ('%s') to ('%s');
`
)

const (
	tablePartitionRangeDuration   = time.Hour
	tablePartitionRangeFormatStr  = "2006-01-02 15:00:00"
	tablePartitionSuffixFormatStr = "2006_01_02_15"
)

func AssertChangelogPartition(ctx context.Context, pgxPool *pgxpool.Pool) error {
	var (
		now                  = time.Now()
		partitionTableSuffix = now.Format(tablePartitionSuffixFormatStr)
		partitionRangeStart  = now.Format(tablePartitionRangeFormatStr)
		partitionRangeEnd    = now.Add(tablePartitionRangeDuration).Format(tablePartitionRangeFormatStr)
		// todo: can clean this up a bit by expressing the string in smaller pieces (node stream piece + edge edge piece perhaps)
		assertSQL = fmt.Sprintf(CREATE_PARTITIONS_SQL_FMT, partitionTableSuffix, partitionRangeStart, partitionRangeEnd)
		_, err    = pgxPool.Exec(ctx, assertSQL)
	)

	return err
}

func shouldAssertNextPartition(lastPartitionAssert time.Time) bool {
	var (
		now                   = time.Now()
		lastPartitionRangeStr = lastPartitionAssert.Format(tablePartitionRangeFormatStr)
		nowPartitionRangeStr  = now.Format(tablePartitionRangeFormatStr)
	)

	return lastPartitionRangeStr != nowPartitionRangeStr
}

func NodeChangePartitionName(now time.Time) string {
	return "node_change_stream_" + now.Format(tablePartitionSuffixFormatStr)
}
