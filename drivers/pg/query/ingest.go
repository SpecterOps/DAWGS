package query

import (
	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
)

const (
	// NodeIngestStagingTable is the transaction-local COPY target for node ingest mutations.
	NodeIngestStagingTable = "node_ingest_staging"

	// EdgeIngestStagingTable is the transaction-local COPY target for edge ingest mutations.
	EdgeIngestStagingTable = "edge_ingest_staging"
)

var (
	// NodeIngestStagingColumns is the required COPY column order for node ingest mutations.
	NodeIngestStagingColumns = []string{"object_id", "id_hash", "kind_ids", "properties"}

	// EdgeIngestStagingColumns is the required COPY column order for edge ingest mutations.
	EdgeIngestStagingColumns = []string{"start_id", "end_id", "start_object_id", "end_object_id", "kind_id", "id_hash", "properties"}
)

func FormatSelectIngestNodeHashes(graphTarget model.Graph, finalRange bool) string {
	statement := join(
		"select properties->>'objectid', content_hash from ",
		formatIngestIdentifier(graphTarget.Partitions.Node.Name),
		" where id_hash >= $1",
	)
	if !finalRange {
		statement += " and id_hash < $2"
	}

	return statement + ";"
}

func FormatSelectIngestEdgeHashes(graphTarget model.Graph, finalRange bool) string {
	statement := join(
		"select start_object_id, kind_id, end_object_id, content_hash from ",
		formatIngestIdentifier(graphTarget.Partitions.Edge.Name),
		" where id_hash >= $1",
	)
	if !finalRange {
		statement += " and id_hash < $2"
	}

	return statement + ";"
}

func FormatCreateNodeIngestStagingTable() string {
	return "create temp table node_ingest_staging (object_id text not null, id_hash integer not null, kind_ids smallint[] not null, properties jsonb not null) on commit drop;"
}

func FormatCreateEdgeIngestStagingTable() string {
	return "create temp table edge_ingest_staging (start_id bigint not null, end_id bigint not null, start_object_id text not null, end_object_id text not null, kind_id smallint not null, id_hash integer not null, properties jsonb not null) on commit drop;"
}

// FormatResolveIngestEndpoints resolves exact node identities within their signed hash buckets.
// The caller supplies parallel integer-hash and object-ID arrays as $1 and $2 respectively.
func FormatResolveIngestEndpoints(graphTarget model.Graph) string {
	return join(
		"select requested.object_id, n.id from unnest($1::integer[], $2::text[]) as requested(id_hash, object_id) ",
		"join ", formatIngestIdentifier(graphTarget.Partitions.Node.Name), " as n ",
		"on n.id_hash = requested.id_hash and n.properties->>'objectid' = requested.object_id;",
	)
}

// FormatValidateIngestEdgeSources detects legacy or inconsistent persisted endpoint source strings.
// The caller supplies the graph ID as $1.
func FormatValidateIngestEdgeSources(graphTarget model.Graph) string {
	return join(
		"select count(*) from ", EdgeIngestStagingTable, " as s ",
		"join ", formatIngestIdentifier(graphTarget.Partitions.Edge.Name), " as e ",
		"on e.start_id = s.start_id and e.end_id = s.end_id and e.kind_id = s.kind_id and e.graph_id = $1 ",
		"where e.start_object_id is null or e.start_object_id <> s.start_object_id ",
		"or e.end_object_id is null or e.end_object_id <> s.end_object_id;",
	)
}

// FormatUpsertIngestNodes writes every staged node mismatch. It deliberately has no no-op predicate:
// client-side canonical hashes are the only no-op filter for this ingest path.
// The caller supplies the graph ID as $1.
func FormatUpsertIngestNodes(graphTarget model.Graph) string {
	const finalKindIDs = "uniq(sort(n.kind_ids || excluded.kind_ids))::smallint[]"

	return join(
		"insert into ", formatIngestIdentifier(graphTarget.Partitions.Node.Name), " as n ",
		"(graph_id, kind_ids, properties, id_hash, content_hash) ",
		"select $1, s.kind_ids, s.properties, s.id_hash, public.dawgs_ingest_node_content_hash(s.kind_ids, s.properties) ",
		"from ", NodeIngestStagingTable, " as s ",
		"order by s.id_hash, s.object_id ",
		"on conflict ((properties->>'objectid')) do update set ",
		"kind_ids = ", finalKindIDs, ", ",
		"properties = n.properties || excluded.properties, ",
		"id_hash = excluded.id_hash, ",
		"content_hash = public.dawgs_ingest_node_content_hash(", finalKindIDs, ", n.properties || excluded.properties);",
	)
}

// FormatUpsertIngestEdges writes every staged edge mismatch. Endpoint source strings are preflighted
// before this statement so an update cannot silently replace an inconsistent persisted edge identity.
// The caller supplies the graph ID as $1.
func FormatUpsertIngestEdges(graphTarget model.Graph) string {
	return join(
		"insert into ", formatIngestIdentifier(graphTarget.Partitions.Edge.Name), " as e ",
		"(graph_id, start_id, end_id, kind_id, properties, id_hash, content_hash, start_object_id, end_object_id) ",
		"select $1, s.start_id, s.end_id, s.kind_id, s.properties, s.id_hash, public.dawgs_ingest_edge_content_hash(s.properties), s.start_object_id, s.end_object_id ",
		"from ", EdgeIngestStagingTable, " as s ",
		"order by s.id_hash, s.start_object_id, s.kind_id, s.end_object_id ",
		"on conflict (start_id, end_id, kind_id, graph_id) do update set ",
		"properties = e.properties || excluded.properties, ",
		"id_hash = excluded.id_hash, ",
		"content_hash = public.dawgs_ingest_edge_content_hash(e.properties || excluded.properties), ",
		"start_object_id = excluded.start_object_id, ",
		"end_object_id = excluded.end_object_id;",
	)
}

// FormatFindIngestHashIndex returns every valid, ready, live, non-partial, non-expression,
// single-key B-tree id_hash index for the target child relation supplied as $1::regclass.
// The caller must reject zero or multiple rows.
func FormatFindIngestHashIndex() string {
	return join(
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
	)
}

func formatIngestIdentifier(identifier string) string {
	return pgx.Identifier{identifier}.Sanitize()
}
