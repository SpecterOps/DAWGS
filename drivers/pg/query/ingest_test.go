package query_test

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg/model"
	query "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/stretchr/testify/require"
)

func ingestTestGraph() model.Graph {
	return model.Graph{
		ID: 42,
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition("node_42"),
			Edge: model.NewGraphPartition("edge_42"),
		},
	}
}

func TestIngestStagingColumns(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		[]string{"object_id", "id_hash", "kind_ids", "properties"},
		query.NodeIngestStagingColumns,
	)
	require.Equal(t,
		[]string{"start_id", "end_id", "start_object_id", "end_object_id", "kind_id", "id_hash", "properties"},
		query.EdgeIngestStagingColumns,
	)
}

func TestFormatSelectIngestNodeHashes(t *testing.T) {
	t.Parallel()

	graphTarget := ingestTestGraph()
	require.Equal(t,
		"select properties->>'objectid', content_hash from \"node_42\" where id_hash >= $1 and id_hash < $2;",
		query.FormatSelectIngestNodeHashes(graphTarget, false),
	)
	require.Equal(t,
		"select properties->>'objectid', content_hash from \"node_42\" where id_hash >= $1;",
		query.FormatSelectIngestNodeHashes(graphTarget, true),
	)
}

func TestFormatSelectIngestEdgeHashes(t *testing.T) {
	t.Parallel()

	graphTarget := ingestTestGraph()
	require.Equal(t,
		"select start_object_id, kind_id, end_object_id, content_hash from \"edge_42\" where id_hash >= $1 and id_hash < $2;",
		query.FormatSelectIngestEdgeHashes(graphTarget, false),
	)
	require.Equal(t,
		"select start_object_id, kind_id, end_object_id, content_hash from \"edge_42\" where id_hash >= $1;",
		query.FormatSelectIngestEdgeHashes(graphTarget, true),
	)
}

func TestFormatCreateNodeIngestStagingTable(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		"create temp table node_ingest_staging (object_id text not null, id_hash integer not null, kind_ids smallint[] not null, properties jsonb not null) on commit drop;",
		query.FormatCreateNodeIngestStagingTable(),
	)
}

func TestFormatCreateEdgeIngestStagingTable(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		"create temp table edge_ingest_staging (start_id bigint not null, end_id bigint not null, start_object_id text not null, end_object_id text not null, kind_id smallint not null, id_hash integer not null, properties jsonb not null) on commit drop;",
		query.FormatCreateEdgeIngestStagingTable(),
	)
}

func TestIngestStagingTableDDLRejectsPreexistingSessionTables(t *testing.T) {
	t.Parallel()

	for _, statement := range []string{
		query.FormatCreateNodeIngestStagingTable(),
		query.FormatCreateEdgeIngestStagingTable(),
	} {
		require.NotContains(t, strings.ToLower(statement), "if not exists")
		require.Contains(t, strings.ToLower(statement), "on commit drop")
	}
}

func TestFormatResolveIngestEndpoints(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		"select requested.object_id, n.id from unnest($1::integer[], $2::text[]) as requested(id_hash, object_id) join \"node_42\" as n on n.id_hash = requested.id_hash and n.properties->>'objectid' = requested.object_id;",
		query.FormatResolveIngestEndpoints(ingestTestGraph()),
	)
}

func TestFormatValidateIngestEdgeSources(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		"select count(*) from edge_ingest_staging as s join \"edge_42\" as e on e.start_id = s.start_id and e.end_id = s.end_id and e.kind_id = s.kind_id and e.graph_id = $1 where e.start_object_id is null or e.start_object_id <> s.start_object_id or e.end_object_id is null or e.end_object_id <> s.end_object_id;",
		query.FormatValidateIngestEdgeSources(ingestTestGraph()),
	)
}

func TestFormatUpsertIngestNodes(t *testing.T) {
	t.Parallel()

	expected := strings.Join([]string{
		"insert into \"node_42\" as n ",
		"(graph_id, kind_ids, properties, id_hash, content_hash) ",
		"select $1, s.kind_ids, s.properties, s.id_hash, public.dawgs_ingest_node_content_hash(s.kind_ids, s.properties) ",
		"from node_ingest_staging as s ",
		"order by s.id_hash, s.object_id ",
		"on conflict ((properties->>'objectid')) do update set ",
		"kind_ids = uniq(sort(n.kind_ids || excluded.kind_ids))::smallint[], ",
		"properties = n.properties || excluded.properties, ",
		"id_hash = excluded.id_hash, ",
		"content_hash = public.dawgs_ingest_node_content_hash(uniq(sort(n.kind_ids || excluded.kind_ids))::smallint[], n.properties || excluded.properties);",
	}, "")

	require.Equal(t, expected, query.FormatUpsertIngestNodes(ingestTestGraph()))
}

func TestFormatUpsertIngestEdges(t *testing.T) {
	t.Parallel()

	expected := strings.Join([]string{
		"insert into \"edge_42\" as e ",
		"(graph_id, start_id, end_id, kind_id, properties, id_hash, content_hash, start_object_id, end_object_id) ",
		"select $1, s.start_id, s.end_id, s.kind_id, s.properties, s.id_hash, public.dawgs_ingest_edge_content_hash(s.properties), s.start_object_id, s.end_object_id ",
		"from edge_ingest_staging as s ",
		"order by s.id_hash, s.start_object_id, s.kind_id, s.end_object_id ",
		"on conflict (start_id, end_id, kind_id, graph_id) do update set ",
		"properties = e.properties || excluded.properties, ",
		"id_hash = excluded.id_hash, ",
		"content_hash = public.dawgs_ingest_edge_content_hash(e.properties || excluded.properties), ",
		"start_object_id = excluded.start_object_id, ",
		"end_object_id = excluded.end_object_id;",
	}, "")

	require.Equal(t, expected, query.FormatUpsertIngestEdges(ingestTestGraph()))
}

func TestFormatFindIngestHashIndex(t *testing.T) {
	t.Parallel()

	expected := strings.Join([]string{
		"select index_class.relname ",
		"from pg_index as index_definition ",
		"join pg_class as index_class on index_class.oid = index_definition.indexrelid ",
		"join pg_class as table_class on table_class.oid = index_definition.indrelid ",
		"join pg_am as access_method on access_method.oid = index_class.relam ",
		"join pg_attribute as attribute on attribute.attrelid = index_definition.indrelid and attribute.attnum = index_definition.indkey[0] ",
		"where table_class.oid = $1::regclass ",
		"and table_class.relispartition ",
		"and access_method.amname = 'btree' ",
		"and index_definition.indisvalid ",
		"and index_definition.indisready ",
		"and index_definition.indislive ",
		"and index_definition.indpred is null ",
		"and index_definition.indexprs is null ",
		"and index_definition.indnkeyatts = 1 ",
		"and attribute.attname = 'id_hash';",
	}, "")

	require.Equal(t, expected, query.FormatFindIngestHashIndex())
}

func TestIngestFormattersQuoteAdversarialPartitionIdentifiers(t *testing.T) {
	t.Parallel()

	graphTarget := model.Graph{
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition(`node"; punctuation!?`),
			Edge: model.NewGraphPartition(`edge"; punctuation!?`),
		},
	}

	for _, statement := range []string{
		query.FormatSelectIngestNodeHashes(graphTarget, true),
		query.FormatSelectIngestEdgeHashes(graphTarget, true),
		query.FormatResolveIngestEndpoints(graphTarget),
		query.FormatValidateIngestEdgeSources(graphTarget),
		query.FormatUpsertIngestNodes(graphTarget),
		query.FormatUpsertIngestEdges(graphTarget),
	} {
		require.NotContains(t, statement, `node"; punctuation!?`)
		require.NotContains(t, statement, `edge"; punctuation!?`)
	}

	require.Contains(t, query.FormatSelectIngestNodeHashes(graphTarget, true), `"node""; punctuation!?"`)
	require.Contains(t, query.FormatSelectIngestEdgeHashes(graphTarget, true), `"edge""; punctuation!?"`)
}
