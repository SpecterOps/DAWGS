package query

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngestSchemaContractsAreEmbedded(t *testing.T) {
	t.Parallel()

	schemaUp := strings.Join(strings.Fields(sqlSchemaUp), " ")
	schemaDown := strings.Join(strings.Fields(sqlSchemaDown), " ")

	require.Contains(t, schemaUp, strings.Join(strings.Fields(`
		create table if not exists node
		(
		  id           bigserial  not null,
		  graph_id     integer    not null,
		  kind_ids     smallint[] not null,
		  properties   jsonb      not null,
		  id_hash      integer,
		  content_hash bytea check (content_hash is null or octet_length(content_hash) = 16),

		  primary key (id, graph_id),
		  foreign key (graph_id) references graph (id) on delete cascade
		) partition by list (graph_id);
	`), " "))
	require.Contains(t, schemaUp, strings.Join(strings.Fields(`
		create table if not exists edge
		(
		  id              bigserial not null,
		  graph_id        integer   not null,
		  start_id        bigint    not null,
		  end_id          bigint    not null,
		  kind_id         smallint  not null,
		  properties      jsonb     not null,
		  id_hash         integer,
		  content_hash    bytea check (content_hash is null or octet_length(content_hash) = 16),
		  start_object_id text,
		  end_object_id   text,

		  primary key (id, graph_id),
		  foreign key (graph_id) references graph (id) on delete cascade,

		  unique (start_id, end_id, kind_id, graph_id)
		) partition by list (graph_id);
	`), " "))

	require.Contains(t, schemaUp, "create index if not exists node_id_hash_index on node using btree (id_hash);")
	require.Contains(t, schemaUp, "create index if not exists edge_id_hash_index on edge using btree (id_hash);")
	require.Contains(t, schemaUp, "create type nodeComposite as ( id bigint, kind_ids smallint[], properties jsonb );")
	require.Contains(t, schemaUp, "create type edgeComposite as ( id bigint, start_id bigint, end_id bigint, kind_id smallint, properties jsonb );")

	immutableFunctions := map[string]string{
		"dawgs_ingest_u64be":             "create or replace function public.dawgs_ingest_u64be(_value bigint) returns bytea",
		"dawgs_ingest_zigzag_varint":     "create or replace function public.dawgs_ingest_zigzag_varint(_value bigint) returns bytea",
		"dawgs_ingest_canonical_number":  "create or replace function public.dawgs_ingest_canonical_number(_number text) returns bytea",
		"dawgs_ingest_canonical_jsonb":   "create or replace function public.dawgs_ingest_canonical_jsonb(_value jsonb) returns bytea",
		"dawgs_ingest_edge_content_hash": "create or replace function public.dawgs_ingest_edge_content_hash(_properties jsonb) returns bytea",
	}
	for name, signature := range immutableFunctions {
		definition := ingestSchemaFunctionDefinition(t, schemaUp, name)
		require.Contains(t, definition, signature)
		require.Contains(t, definition, "immutable")
		require.Contains(t, definition, "parallel safe")
		require.Contains(t, definition, "strict")
	}

	nodeHashDefinition := ingestSchemaFunctionDefinition(t, schemaUp, "dawgs_ingest_node_content_hash")
	require.Contains(t, nodeHashDefinition,
		"create or replace function public.dawgs_ingest_node_content_hash(_kind_ids smallint[], _properties jsonb) returns bytea")
	require.Contains(t, nodeHashDefinition, "stable")
	require.Contains(t, nodeHashDefinition, "parallel safe")
	require.Contains(t, nodeHashDefinition, "strict")
	require.NotContains(t, nodeHashDefinition, "immutable")

	canonicalJSONDefinition := ingestSchemaFunctionDefinition(t, schemaUp, "dawgs_ingest_canonical_jsonb")
	require.Contains(t, canonicalJSONDefinition, "jsonb_typeof(_value)")
	require.Contains(t, canonicalJSONDefinition, "with ordinality")
	require.Contains(t, canonicalJSONDefinition, "order by convert_to(key, 'UTF8')")
	require.Contains(t, canonicalJSONDefinition, "octet_length")

	canonicalNumberDefinition := ingestSchemaFunctionDefinition(t, schemaUp, "dawgs_ingest_canonical_number")
	require.Contains(t, canonicalNumberDefinition, "1073741823")
	require.Contains(t, canonicalNumberDefinition, "131072")
	require.Contains(t, canonicalNumberDefinition, "16383")

	require.Contains(t, nodeHashDefinition, "dawgs:pg-ingest:node-content:v1")
	require.Contains(t, nodeHashDefinition, "_properties - 'objectid'")
	require.Contains(t, nodeHashDefinition, "convert_to(name, 'UTF8')")
	require.Contains(t, nodeHashDefinition, "substring(sha256(content) from 1 for 16)")
	require.Contains(t, nodeHashDefinition, "if jsonb_typeof(_properties) <> 'object' then")

	edgeHashDefinition := ingestSchemaFunctionDefinition(t, schemaUp, "dawgs_ingest_edge_content_hash")
	require.Contains(t, edgeHashDefinition, "dawgs:pg-ingest:edge-content:v1")
	require.Contains(t, edgeHashDefinition, "substring(sha256(content) from 1 for 16)")
	require.Contains(t, edgeHashDefinition, "if jsonb_typeof(_properties) <> 'object' then")

	drops := []string{
		"drop function if exists dawgs_ingest_edge_content_hash(jsonb);",
		"drop function if exists dawgs_ingest_node_content_hash(smallint[], jsonb);",
		"drop function if exists dawgs_ingest_canonical_jsonb(jsonb);",
		"drop function if exists dawgs_ingest_canonical_number(text);",
		"drop function if exists dawgs_ingest_zigzag_varint(bigint);",
		"drop function if exists dawgs_ingest_u64be(bigint);",
	}
	previous := -1
	for _, drop := range drops {
		index := strings.Index(schemaDown, drop)
		require.Greater(t, index, previous, drop)
		previous = index
	}
	require.Less(t, previous, strings.Index(schemaDown, "drop table if exists node;"))
}

func ingestSchemaFunctionDefinition(t *testing.T, schema, name string) string {
	t.Helper()

	start := strings.Index(schema, "create or replace function public."+name+"(")
	require.NotEqual(t, -1, start, name)

	remainder := schema[start+1:]
	if length := strings.Index(remainder, "create or replace function "); length >= 0 {
		return schema[start : start+1+length]
	}

	return schema[start:]
}
