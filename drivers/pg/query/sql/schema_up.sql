-- DAWGS Property Graph Partitioned Layout for PostgreSQL

-- Notes on TOAST:
--
-- Graph entity properties are stored in a JSONB column at the end of the row. There is a soft-limit of 2KiB for rows in
-- a PostgreSQL database page. The database will compress this value in an attempt not to exceed this limit. Once a
-- compressed value reaches the absolute limit of what the database can do to either compact it or give it more of the
-- 8 KiB page size limit, the database evicts the value to an associated TOAST (The Oversized-Attribute Storage Technique)
-- table and creates a reference to the entry to be joined upon fetch of the row.
--
-- TOAST comes with certain performance caveats that can affect access time anywhere from a factor 3 to 6 times. It is
-- in the best interest of the database user that the properties of a graph entity never exceed this limit in large
-- graphs.

-- We need the trigram extension to create a GIN text-search index. The goal here isn't full-text search, in which
-- case ts_vector and its ilk would be more suited. This particular selection was made to support faster lookups
-- for "contains", "starts with" and, "ends with" comparison operations.
create extension if not exists pg_trgm;

-- We need the intarray extension for extended integer array operations like unions. This is useful for managing kind
-- arrays for nodes.
create extension if not exists intarray;

-- This is an optional but useful extension for validating performance of queries
-- create extension if not exists pg_stat_statements;
--
-- create or replace function public.query_perf()
--   returns table
--           (
--             query              text,
--             calls              int,
--             total_time         numeric,
--             mean_time          numeric,
--             percent_total_time numeric
--           )
-- as
-- $$
-- select query                                                                      as query,
--        calls                                                                      as calls,
--        round(total_exec_time::numeric, 2)                                         as total_time,
--        round(mean_exec_time::numeric, 2)                                          as mean_time,
--        round((100 * total_exec_time / sum(total_exec_time) over ()):: numeric, 2) as percent_total_time
-- from pg_stat_statements
-- order by total_exec_time desc
-- limit 25
-- $$
--   language sql
--   immutable
--   parallel safe
--   strict;

-- Table definitions

-- The graph table contains name to ID mappings for graphs contained within the database. Each graph ID should have
-- corresponding table partitions for the node and edge tables.
create table if not exists graph
(
  id   bigserial,
  name varchar(256) not null,
  primary key (id),
  unique (name)
);

-- graph_traversal_epoch is a graph-scoped, transactionally visible generation
-- for topology-aware SQL selection. It is deliberately independent of driver
-- cache generations: a stale or missing epoch is an incumbent-only condition.
create table if not exists graph_traversal_epoch
(
  graph_id bigint primary key references graph (id) on delete cascade,
  epoch    bigint not null default 1,
  check (epoch > 0)
);

-- The latest atomically published topology synopsis for each graph. The
-- initial selector uses only its generation validity; estimator payloads are
-- added in versioned relations as candidate families require them.
create table if not exists graph_traversal_synopsis_generation
(
  graph_id              bigint primary key references graph (id) on delete cascade,
  epoch                 bigint not null,
  source_mutation_epoch bigint not null,
  estimator_version     text not null,
  status                text not null,
  node_count            bigint not null default 0,
  edge_count            bigint not null default 0,
  built_at              timestamptz not null default clock_timestamp(),
  check (epoch > 0),
  check (source_mutation_epoch > 0),
  check (status in ('ready', 'building', 'failed'))
);

insert into graph_traversal_epoch (graph_id)
select id
from graph
on conflict (graph_id) do nothing;

create or replace function public.create_graph_traversal_epoch() returns trigger as
$$
begin
  insert into graph_traversal_epoch (graph_id)
  values (new.id)
  on conflict (graph_id) do nothing;
  return new;
end
$$
  language plpgsql
  volatile;

drop trigger if exists create_graph_traversal_epoch on graph;
create trigger create_graph_traversal_epoch
  after insert on graph
  for each row
execute procedure public.create_graph_traversal_epoch();

-- The kind table contains name to ID mappings for graph kinds. Storage of these types is necessary to maintain search
-- capability of a database without the origin application that generated it.
-- To support FK in asset_group_tags table, the kind table is now maintained by the stepwise migration files.
-- Any schema updates here should be reflected in a stepwise migration file as well.
create table if not exists kind
(
  id   smallserial,
  name varchar(256) not null,
  primary key (id),
  unique (name)
);

-- Node composite type
do
$$
  begin
    create type nodeComposite as
    (
      id         bigint,
      kind_ids   smallint[],
      properties jsonb
    );
  exception
    when duplicate_object then null;
  end
$$;

-- The node table is a partitioned table view that partitions over the graph ID that each node belongs to. Nodes may
-- contain a disjunction of kinds for creating node subsets without requiring edges.
create table if not exists node
(
  id         bigserial  not null,
  graph_id   integer    not null,
  kind_ids   smallint[] not null,
  properties jsonb      not null,

  primary key (id, graph_id),
  foreign key (graph_id) references graph (id) on delete cascade
) partition by list (graph_id);

-- The storage strategy chosen for the properties JSONB column informs the database of the user's preference to resort
-- to creating a TOAST table entry only after there is no other possible way to inline the row attribute in the current
-- page.
alter table node
  alter column properties set storage main;

-- Remove the old graph ID index.
drop index if exists node_graph_id_index;

-- Index node kind IDs so that lookups by kind is accelerated.
create index if not exists node_kind_ids_index on node using gin (kind_ids);

-- Edge composite type
do
$$
  begin
    create type edgeComposite as
    (
      id         bigint,
      start_id   bigint,
      end_id     bigint,
      kind_id    smallint,
      properties jsonb
    );
  exception
    when duplicate_object then null;
  end
$$;

-- The edge table is a partitioned table view that partitions over the graph ID that each edge belongs to.
create table if not exists edge
(
  id         bigserial not null,
  graph_id   integer   not null,
  start_id   bigint    not null,
  end_id     bigint    not null,
  kind_id    smallint  not null,
  properties jsonb     not null,

  primary key (id, graph_id),
  foreign key (graph_id) references graph (id) on delete cascade,

  unique (start_id, end_id, kind_id, graph_id)
) partition by list (graph_id);

-- delete_node_edges is a trigger and associated plpgsql function to cascade delete edges when attached nodes are
-- deleted. While this could be done with a foreign key relationship, it would scope the cascade delete to individual
-- node partitions and therefore require the graph_id value of each node as part of the delete statement. The trigger is
-- statement-level and reads the deleted rows from a transition table so that deleting many nodes in a single statement
-- fires the cascading edge delete once rather than once per row.
create or replace function delete_node_edges() returns trigger as
$$
begin
  delete from edge where start_id in (select id from deleted_nodes)
                      or end_id in (select id from deleted_nodes);
  return null;
end
$$
  language plpgsql
  volatile;

-- Drop and create the delete_node_edges trigger for the delete_node_edges() plpgsql function. See the function comment
-- for more information. The referencing clause exposes the rows removed by the delete statement as the deleted_nodes
-- transition table.
drop trigger if exists delete_node_edges on node;
create trigger delete_node_edges
  after delete
  on node
  referencing old table as deleted_nodes
  for each statement
execute procedure delete_node_edges();

-- Each mutating statement advances the affected graph's topology epoch in the
-- same transaction. Multiple statements may advance it more than once; that
-- is conservative and makes every previously read synopsis stale.
create or replace function public.bump_graph_traversal_epoch_new() returns trigger as
$$
begin
  update graph_traversal_epoch
  set epoch = epoch + 1
  where graph_id in (select distinct graph_id from new_rows);
  return null;
end
$$
  language plpgsql
  volatile;

create or replace function public.bump_graph_traversal_epoch_old() returns trigger as
$$
begin
  update graph_traversal_epoch
  set epoch = epoch + 1
  where graph_id in (select distinct graph_id from old_rows);
  return null;
end
$$
  language plpgsql
  volatile;

create or replace function public.bump_all_graph_traversal_epochs() returns trigger as
$$
begin
  update graph_traversal_epoch
  set epoch = epoch + 1;
  return null;
end
$$
  language plpgsql
  volatile;

drop trigger if exists bump_node_traversal_epoch_insert on node;
create trigger bump_node_traversal_epoch_insert after insert on node
  referencing new table as new_rows for each statement
execute procedure public.bump_graph_traversal_epoch_new();
drop trigger if exists bump_node_traversal_epoch_update on node;
create trigger bump_node_traversal_epoch_update after update on node
  referencing new table as new_rows for each statement
execute procedure public.bump_graph_traversal_epoch_new();
drop trigger if exists bump_node_traversal_epoch_delete on node;
create trigger bump_node_traversal_epoch_delete after delete on node
  referencing old table as old_rows for each statement
execute procedure public.bump_graph_traversal_epoch_old();
drop trigger if exists bump_edge_traversal_epoch_insert on edge;
create trigger bump_edge_traversal_epoch_insert after insert on edge
  referencing new table as new_rows for each statement
execute procedure public.bump_graph_traversal_epoch_new();
drop trigger if exists bump_edge_traversal_epoch_update on edge;
create trigger bump_edge_traversal_epoch_update after update on edge
  referencing new table as new_rows for each statement
execute procedure public.bump_graph_traversal_epoch_new();
drop trigger if exists bump_edge_traversal_epoch_delete on edge;
create trigger bump_edge_traversal_epoch_delete after delete on edge
  referencing old table as old_rows for each statement
execute procedure public.bump_graph_traversal_epoch_old();
drop trigger if exists bump_node_traversal_epoch_truncate on node;
create trigger bump_node_traversal_epoch_truncate after truncate on node
  for each statement execute procedure public.bump_all_graph_traversal_epochs();
drop trigger if exists bump_edge_traversal_epoch_truncate on edge;
create trigger bump_edge_traversal_epoch_truncate after truncate on edge
  for each statement execute procedure public.bump_all_graph_traversal_epochs();


-- The storage strategy chosen for the properties JSONB column informs the database of the user's preference to resort
-- to creating a TOAST table entry only after there is no other possible way to inline the row attribute in the current
-- page.
alter table edge
  alter column properties set storage main;

-- Remove old indexes that are now redundant or superseded.
drop index if exists edge_graph_id_index;
drop index if exists edge_start_id_index;
drop index if exists edge_end_id_index;
drop index if exists edge_kind_index;
drop index if exists edge_start_kind_index;
drop index if exists edge_end_kind_index;

-- Covering indexes for traversal joins and relationship counts. The INCLUDE columns allow index-only scans for
-- the common case where the join needs (id, start_id, end_id, kind_id) without fetching from the heap. The standalone
-- start_id and end_id indexes are intentionally omitted: the composite indexes satisfy left-prefix lookups on start_id
-- or end_id alone. Relationship count fast paths query kind_id without an endpoint anchor, so keep a kind_id-first
-- covering index for those shapes.
create index if not exists edge_start_id_kind_id_id_end_id_index on edge using btree (start_id, kind_id) include (id, end_id);
create index if not exists edge_end_id_kind_id_id_start_id_index on edge using btree (end_id, kind_id) include (id, start_id);
create index if not exists edge_kind_id_id_start_id_end_id_index on edge using btree (kind_id) include (id, start_id, end_id);

-- Path composite type
do
$$
  begin
    create type pathComposite as
    (
      nodes nodeComposite[],
      edges edgeComposite[]
    );
  exception
    when duplicate_object then null;
  end
$$;

-- Database helper functions
create or replace function public.kind_name(_kind_id smallint) returns text as
$$
select k.name::text
from kind k
where k.id = _kind_id
limit 1;
$$
  language sql
  stable
  parallel safe
  strict;

create or replace function public.start_node(rel edgeComposite) returns nodeComposite as
$$
select (n.id, n.kind_ids, n.properties)::nodeComposite
from node n
where n.id = (rel).start_id
limit 1;
$$
  language sql
  stable
  parallel safe
  strict;

create or replace function public.end_node(rel edgeComposite) returns nodeComposite as
$$
select (n.id, n.kind_ids, n.properties)::nodeComposite
from node n
where n.id = (rel).end_id
limit 1;
$$
  language sql
  stable
  parallel safe
  strict;

create or replace function public.lock_details()
  returns table
          (
            datname      text,
            locktype     text,
            relation     text,
            lock_mode    text,
            txid         xid,
            virtual_txid text,
            pid          integer,
            tx_granted   bool,
            client_addr  text,
            client_port  integer,
            elapsed_time interval
          )
as
$$
select db.datname              as datname,
       locktype                as locktype,
       relation::regclass      as relation,
       mode                    as lock_mode,
       transactionid           as txid,
       virtualtransaction      as virtual_txid,
       l.pid                   as pid,
       granted                 as tx_granted,
       psa.client_addr         as client_addr,
       psa.client_port         as client_port,
       now() - psa.query_start as elapsed_time
from pg_catalog.pg_locks l
       left join pg_catalog.pg_database db on db.oid = l.database
       left join pg_catalog.pg_stat_activity psa on l.pid = psa.pid
where not l.pid = pg_backend_pid();
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.table_sizes()
  returns table
          (
            oid          int,
            table_schema text,
            table_name   text,
            total_bytes  numeric,
            total_size   text,
            index_size   text,
            toast_size   text,
            table_size   text
          )
as
$$
select oid                         as oid,
       table_schema                as table_schema,
       table_name                  as table_name,
       total_bytes                 as total_bytes,
       pg_size_pretty(total_bytes) as total_size,
       pg_size_pretty(index_bytes) as index_size,
       pg_size_pretty(toast_bytes) as toast_size,
       pg_size_pretty(table_bytes) as table_size
from (select *, total_bytes - index_bytes - coalesce(toast_bytes, 0) as table_bytes
      from (select c.oid                                 as oid,
                   nspname                               as table_schema,
                   relname                               as table_name,
                   c.reltuples                           as row_estimate,
                   pg_total_relation_size(c.oid)         as total_bytes,
                   pg_indexes_size(c.oid)                as index_bytes,
                   pg_total_relation_size(reltoastrelid) as toast_bytes
            from pg_class c
                   left join pg_namespace n on n.oid = c.relnamespace
            where relkind = 'r') a) a
order by total_bytes desc;
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.index_utilization()
  returns table
          (
            table_name    text,
            idx_scans     int,
            seq_scans     int,
            index_usage   int,
            rows_in_table int
          )
as
$$
select relname                                table_name,
       idx_scan                               index_scan,
       seq_scan                               table_scan,
       100 * idx_scan / (seq_scan + idx_scan) index_usage,
       n_live_tup                             rows_in_table
from pg_stat_user_tables
where seq_scan + idx_scan > 0
order by index_usage desc
limit 25;
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.jsonb_to_text_array(target jsonb)
  returns text[]
as
$$
begin
  if target != 'null'::jsonb then
    return array(select jsonb_array_elements_text(target));
  end if;

  return null;
end
$$
  language plpgsql
  immutable
  parallel safe
  strict;

drop aggregate if exists public.cypher_min(jsonb);
drop aggregate if exists public.cypher_max(jsonb);

create or replace function public.cypher_jsonb_type_rank(value jsonb)
  returns int
as
$$
select case jsonb_typeof(value)
         when 'object' then 1
         when 'array' then 2
         when 'string' then 3
         when 'boolean' then 4
         when 'number' then 5
         when 'null' then 6
         else 7
       end;
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.cypher_value_compare(left_value jsonb, right_value jsonb)
  returns int
as
$$
declare
  left_type text;
  right_type text;
  left_rank int;
  right_rank int;
  left_number numeric;
  right_number numeric;
  left_text text;
  right_text text;
  left_bool bool;
  right_bool bool;
  left_len int;
  right_len int;
  left_keys text[];
  right_keys text[];
  idx int;
  comparison int;
begin
  if left_value = right_value then
    return 0;
  end if;

  if left_value = 'null'::jsonb then
    return 1;
  end if;

  if right_value = 'null'::jsonb then
    return -1;
  end if;

  left_type := jsonb_typeof(left_value);
  right_type := jsonb_typeof(right_value);

  if left_type != right_type then
    left_rank := public.cypher_jsonb_type_rank(left_value);
    right_rank := public.cypher_jsonb_type_rank(right_value);

    if left_rank < right_rank then
      return -1;
    end if;

    return 1;
  end if;

  case left_type
    when 'number' then
      left_number := (left_value #>> '{}')::numeric;
      right_number := (right_value #>> '{}')::numeric;

      if left_number < right_number then
        return -1;
      elsif left_number > right_number then
        return 1;
      end if;

      return 0;

    when 'string' then
      left_text := left_value #>> '{}';
      right_text := right_value #>> '{}';

      if left_text < right_text then
        return -1;
      elsif left_text > right_text then
        return 1;
      end if;

      return 0;

    when 'boolean' then
      left_bool := (left_value #>> '{}')::bool;
      right_bool := (right_value #>> '{}')::bool;

      if left_bool = right_bool then
        return 0;
      elsif not left_bool and right_bool then
        return -1;
      end if;

      return 1;

    when 'array' then
      left_len := jsonb_array_length(left_value);
      right_len := jsonb_array_length(right_value);
      idx := 0;

      while idx < least(left_len, right_len) loop
        comparison := public.cypher_value_compare(left_value -> idx, right_value -> idx);

        if comparison != 0 then
          return comparison;
        end if;

        idx := idx + 1;
      end loop;

      if left_len = right_len then
        return 0;
      elsif left_len < right_len then
        return -1;
      end if;

      return 1;

    when 'object' then
      select count(*)::int into left_len from jsonb_object_keys(left_value);
      select count(*)::int into right_len from jsonb_object_keys(right_value);

      if left_len < right_len then
        return -1;
      elsif left_len > right_len then
        return 1;
      end if;

      select array_agg(key order by key) into left_keys from jsonb_object_keys(left_value) as left_object_keys(key);
      select array_agg(key order by key) into right_keys from jsonb_object_keys(right_value) as right_object_keys(key);

      for idx in 1..left_len loop
        if left_keys[idx] < right_keys[idx] then
          return -1;
        elsif left_keys[idx] > right_keys[idx] then
          return 1;
        end if;
      end loop;

      for idx in 1..left_len loop
        comparison := public.cypher_value_compare(left_value -> left_keys[idx], right_value -> right_keys[idx]);

        if comparison != 0 then
          return comparison;
        end if;
      end loop;

      return 0;

    else
      if left_value::text < right_value::text then
        return -1;
      elsif left_value::text > right_value::text then
        return 1;
      end if;

      return 0;
  end case;
end
$$
  language plpgsql
  immutable
  parallel safe
  strict;

create or replace function public.cypher_min_transition(state jsonb, value jsonb)
  returns jsonb
as
$$
begin
  if value is null or value = 'null'::jsonb then
    return state;
  end if;

  if state is null then
    return value;
  end if;

  if public.cypher_value_compare(value, state) < 0 then
    return value;
  end if;

  return state;
end
$$
  language plpgsql
  immutable
  parallel safe;

create or replace function public.cypher_max_transition(state jsonb, value jsonb)
  returns jsonb
as
$$
begin
  if value is null or value = 'null'::jsonb then
    return state;
  end if;

  if state is null then
    return value;
  end if;

  if public.cypher_value_compare(value, state) > 0 then
    return value;
  end if;

  return state;
end
$$
  language plpgsql
  immutable
  parallel safe;

create aggregate public.cypher_min(jsonb)
(
  sfunc = public.cypher_min_transition,
  stype = jsonb,
  parallel = safe
);

create aggregate public.cypher_max(jsonb)
(
  sfunc = public.cypher_max_transition,
  stype = jsonb,
  parallel = safe
);

create or replace function public.cypher_contains(haystack text, needle text)
  returns bool as
$$
select strpos(haystack, needle) > 0;
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.cypher_starts_with(haystack text, prefix text)
  returns bool as
$$
select left(haystack, char_length(prefix)) = prefix;
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.cypher_ends_with(haystack text, suffix text)
  returns bool as
$$
select right(haystack, char_length(suffix)) = suffix;
$$
  language sql
  immutable
  parallel safe
  strict;

-- CREATE OR REPLACE does not replace a function when its argument signature
-- changes. Remove the pre-graph-scope overloads explicitly so upgrades cannot
-- retain helpers that hydrate entities from a different graph partition.
drop function if exists public.nodes_to_path(int8[]);
drop function if exists public.edges_to_path(int8[]);
drop function if exists public.ordered_edges_to_path(nodeComposite, edgeComposite[], nodeComposite[]);

create or replace function public.nodes_to_path(target_graph_id int4, nodes variadic int8[]) returns pathComposite as
$$
select row (array_agg(distinct (n.id, n.kind_ids, n.properties)::nodeComposite)::nodeComposite[],
         array []::edgeComposite[])::pathComposite
from node n
where n.graph_id = target_graph_id
  and n.id = any (nodes);
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.edges_to_path(target_graph_id int4, path variadic int8[]) returns pathComposite as
$$
select row (
  (select array_agg(distinct (n.id, n.kind_ids, n.properties)::nodeComposite)
   from node n
   where n.graph_id = target_graph_id
     and n.id in (
     select start_id from edge where graph_id = target_graph_id and id = any(path)
     union
     select end_id from edge where graph_id = target_graph_id and id = any(path)
   )),
  (select array_agg(distinct (r.id, r.start_id, r.end_id, r.kind_id, r.properties)::edgeComposite)
   from edge r
   where r.graph_id = target_graph_id
     and r.id = any(path))
)::pathComposite;
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.ordered_edges_to_path(target_graph_id int4, root nodeComposite, edges edgeComposite[], known_nodes nodeComposite[]) returns pathComposite as
$$
with recursive edge_bounds(edge_count) as
(
  select coalesce(array_length(edges, 1), 0)
),
path_walk(idx, current_node_id, node_ids, edge_ordinals, last_ordinal, direction) as
(
  select 1::int4,
         (root).id,
         array [(root).id]::int8[],
         array []::int8[],
         case
           when edge_bounds.edge_count > 0 and ((root).id = (edges[1]).start_id or (root).id = (edges[1]).end_id) then 0::int8
           when edge_bounds.edge_count > 0 and ((root).id = (edges[edge_bounds.edge_count]).start_id or (root).id = (edges[edge_bounds.edge_count]).end_id) then edge_bounds.edge_count::int8 + 1
           else 0::int8
         end as last_ordinal,
         case
           when edge_bounds.edge_count > 0 and
                not ((root).id = (edges[1]).start_id or (root).id = (edges[1]).end_id) and
                ((root).id = (edges[edge_bounds.edge_count]).start_id or (root).id = (edges[edge_bounds.edge_count]).end_id) then -1::int8
           else 1::int8
         end as direction
  from edge_bounds
  union all
  select path_walk.idx + 1,
         next_step.next_node_id,
         path_walk.node_ids || next_step.next_node_id,
         path_walk.edge_ordinals || next_step.ordinality,
         next_step.ordinality,
         path_walk.direction
  from path_walk
  cross join edge_bounds
  cross join lateral
  (
    select edge_item.input_ordinality as ordinality,
           case
             when path_walk.current_node_id = edge_item.start_id then edge_item.end_id
             when path_walk.current_node_id = edge_item.end_id then edge_item.start_id
           end as next_node_id
    from unnest(edges) with ordinality as edge_item(id, start_id, end_id, kind_id, properties, input_ordinality)
    where edge_item.input_ordinality != all (path_walk.edge_ordinals)
      and (
        path_walk.current_node_id = edge_item.start_id or
        path_walk.current_node_id = edge_item.end_id
      )
    order by
      case when edge_item.input_ordinality = path_walk.last_ordinal + path_walk.direction then 0 else 1 end,
      case when path_walk.direction < 0 then -edge_item.input_ordinality else edge_item.input_ordinality end
    limit 1
  ) next_step
  where path_walk.idx <= edge_bounds.edge_count
),
final_walk as
(
  select path_walk.node_ids, path_walk.edge_ordinals
  from path_walk
  order by path_walk.idx desc
  limit 1
)
select row (
  (
    select coalesce(
      array_agg(coalesce(known_node.node, (n.id, n.kind_ids, n.properties)::nodeComposite) order by ordered_node.ordinality)::nodeComposite[],
      array []::nodeComposite[]
    )
    from final_walk
    cross join lateral unnest(final_walk.node_ids) with ordinality as ordered_node(id, ordinality)
    left join lateral
    (
      select (candidate.id, candidate.kind_ids, candidate.properties)::nodeComposite as node
      from unnest(known_nodes) as candidate(id, kind_ids, properties)
      where candidate.id = ordered_node.id
      limit 1
    ) known_node on true
    left join node n on n.id = ordered_node.id and n.graph_id = target_graph_id and known_node.node is null
  ),
  (
    select coalesce(
      array_agg((ordered_edge.id, ordered_edge.start_id, ordered_edge.end_id, ordered_edge.kind_id, ordered_edge.properties)::edgeComposite order by selected_edge.path_ordinality)::edgeComposite[],
      array []::edgeComposite[]
    )
    from final_walk
    cross join lateral unnest(final_walk.edge_ordinals) with ordinality as selected_edge(edge_ordinality, path_ordinality)
    join lateral unnest(edges) with ordinality as ordered_edge(id, start_id, end_id, kind_id, properties, input_ordinality)
      on ordered_edge.input_ordinality = selected_edge.edge_ordinality
  )
)::pathComposite;
$$
  language sql
  stable
  parallel safe
  strict;

-- ordered_edge_ids_to_path is the read-expansion materializer. Expansion
-- lowering already knows the edge order, so this helper walks that order once
-- instead of repeatedly searching the remaining edge array. Every persistent
-- lookup is constrained by target_graph_id because entity IDs are only unique
-- within a graph partition.
create or replace function public.ordered_edge_ids_to_path(target_graph_id int4, root nodeComposite, edge_ids int8[], known_nodes nodeComposite[]) returns pathComposite as
$$
with recursive
edge_count(value) as
(
  select coalesce(cardinality(edge_ids), 0)
),
hydrated_edges as materialized
(
  select path_edge.ordinality::int4 as ordinality,
         (e.id, e.start_id, e.end_id, e.kind_id, e.properties)::edgeComposite as edge
  from unnest(edge_ids) with ordinality as path_edge(id, ordinality)
  join edge e
    on e.id = path_edge.id
   and e.graph_id = target_graph_id
),
path_walk(idx, current_node_id, node_ids) as
(
  select 0::int4, (root).id, array [(root).id]::int8[]
  union all
  select path_walk.idx + 1,
         case
           when path_walk.current_node_id = (next_edge.edge).start_id then (next_edge.edge).end_id
           else (next_edge.edge).start_id
         end,
         path_walk.node_ids || case
           when path_walk.current_node_id = (next_edge.edge).start_id then (next_edge.edge).end_id
           else (next_edge.edge).start_id
         end
  from path_walk
  join hydrated_edges next_edge
    on next_edge.ordinality = path_walk.idx + 1
   and path_walk.current_node_id in ((next_edge.edge).start_id, (next_edge.edge).end_id)
),
final_walk as
(
  select path_walk.node_ids
  from path_walk
  cross join edge_count
  where path_walk.idx = edge_count.value
)
select row (
  (
    select coalesce(
      array_agg(coalesce(known_node.node, (n.id, n.kind_ids, n.properties)::nodeComposite) order by ordered_node.ordinality)::nodeComposite[],
      array []::nodeComposite[]
    )
    from final_walk
    cross join lateral unnest(final_walk.node_ids) with ordinality as ordered_node(id, ordinality)
    left join lateral
    (
      select (candidate.id, candidate.kind_ids, candidate.properties)::nodeComposite as node
      from unnest(known_nodes) as candidate(id, kind_ids, properties)
      where candidate.id = ordered_node.id
      limit 1
    ) known_node on true
    left join node n
      on n.id = ordered_node.id
     and n.graph_id = target_graph_id
     and known_node.node is null
  ),
  (
    select coalesce(
      array_agg(hydrated_edges.edge order by hydrated_edges.ordinality)::edgeComposite[],
      array []::edgeComposite[]
    )
    from hydrated_edges
  )
)::pathComposite
from final_walk;
$$
  language sql
  stable
  parallel safe
  strict;

create or replace function public.create_unidirectional_pathspace_tables()
  returns void as
$$
begin
  -- The path column is not used as a primary key. Deduplication is handled by DISTINCT ON clauses in the
  -- harness functions. Removing the PK on the variable-length int8[] array eliminates O(n)-key B-tree
  -- maintenance that grows with traversal depth.
  create temporary table if not exists forward_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit preserve rows;

  create temporary table if not exists next_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit preserve rows;

  create index if not exists forward_front_next_id_index on forward_front using btree (next_id);
  create index if not exists forward_front_satisfied_index on forward_front using btree (root_id, next_id, depth) where satisfied;
  create index if not exists forward_front_is_cycle_index on forward_front using btree (root_id, next_id) where is_cycle;

  create index if not exists next_front_next_id_index on next_front using btree (next_id);
  create index if not exists next_front_satisfied_index on next_front using btree (root_id, next_id, depth) where satisfied;
  create index if not exists next_front_is_cycle_index on next_front using btree (root_id, next_id) where is_cycle;

  truncate table forward_front, next_front;
end;
$$
  language plpgsql
  volatile
  strict;


create or replace function public.create_unidirectional_shortest_path_tables()
  returns void as
$$
begin
  create temporary table if not exists visited
  (
    root_id int8 not null,
    id      int8 not null,
    primary key (root_id, id)
  ) on commit preserve rows;

  create temporary table if not exists paths
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit preserve rows;

  create temporary table if not exists resolved_roots
  (
    root_id int8 not null,
    primary key (root_id)
  ) on commit preserve rows;

  truncate table visited, paths, resolved_roots;

  perform create_unidirectional_pathspace_tables();

  create index if not exists forward_front_root_id_next_id_index on forward_front using btree (root_id, next_id);
  create index if not exists next_front_root_id_next_id_index on next_front using btree (root_id, next_id);
  create index if not exists paths_root_id_next_id_index on paths using btree (root_id, next_id);
end;
$$
  language plpgsql
  volatile
  strict;

-- create_traversal_filter_tables materializes the root, terminal and pair filter sets into temporary tables that the
-- harness functions join against. Definitions persist for the physical
-- session; each invocation truncates its row state before loading a new filter.
create or replace function public.create_traversal_filter_tables()
  returns void as
$$
begin
  create temporary table if not exists traversal_root_filter
  (
    id int8 not null,
    primary key (id)
  ) on commit preserve rows;

  create temporary table if not exists traversal_terminal_filter
  (
    id int8 not null,
    primary key (id)
  ) on commit preserve rows;

  create temporary table if not exists traversal_pair_filter
  (
    root_id     int8 not null,
    terminal_id int8 not null,
    primary key (root_id, terminal_id)
  ) on commit preserve rows;

  create index if not exists traversal_pair_filter_terminal_id_root_id_index on traversal_pair_filter using btree (terminal_id, root_id);

  truncate table traversal_root_filter;
  truncate table traversal_terminal_filter;
  truncate table traversal_pair_filter;

  return;
end;
$$
  language plpgsql
  volatile;

create or replace function public.create_traversal_filter_tables(root_ids int8[], terminal_ids int8[])
  returns void as
$$
begin
  perform create_traversal_filter_tables();

  insert into traversal_root_filter
  select distinct root_id
  from unnest(root_ids) as root_ids(root_id)
  where root_id is not null
  on conflict (id) do nothing;

  insert into traversal_terminal_filter
  select distinct terminal_id
  from unnest(terminal_ids) as terminal_ids(terminal_id)
  where terminal_id is not null
  on conflict (id) do nothing;

  analyze traversal_root_filter;
  analyze traversal_terminal_filter;

  return;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.create_traversal_filter_tables(root_filter text, terminal_filter text, pair_filter text)
  returns void as
$$
begin
  perform create_traversal_filter_tables();

  if length(pair_filter) > 0 then
    execute pair_filter;
  end if;

  if length(root_filter) > 0 then
    execute root_filter;
  elsif length(pair_filter) > 0 then
    insert into traversal_root_filter
    select distinct root_id
    from traversal_pair_filter
    on conflict (id) do nothing;
  end if;

  if length(terminal_filter) > 0 then
    execute terminal_filter;
  elsif length(pair_filter) > 0 then
    insert into traversal_terminal_filter
    select distinct terminal_id
    from traversal_pair_filter
    on conflict (id) do nothing;
  end if;

  analyze traversal_root_filter;
  analyze traversal_terminal_filter;
  analyze traversal_pair_filter;

  return;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.create_traversal_filter_tables(root_filter text, terminal_filter text)
  returns void as
$$
select public.create_traversal_filter_tables(root_filter, terminal_filter, ''::text);
$$
  language sql
  volatile
  strict;

create or replace function public.shortest_path_self_endpoint_error(root_id int8, terminal_id int8)
  returns bool as
$$
begin
  raise exception using
    errcode = '22023',
    message = format('shortest path endpoints must not resolve to the same node: root_id=%s terminal_id=%s',
                     root_id,
                     terminal_id);

  return false;
end;
$$
  language plpgsql
  volatile
  strict;

-- Compact bound-pair shortest-path searches share a session-local workspace.
-- The tables survive transaction boundaries so their catalog objects and
-- indexes are paid for once per physical connection. Every public executor
-- resets row state before use; an aborted call is therefore harmless to the
-- next invocation on the same pooled connection.
create or replace function public.ensure_shortest_dag_workspace()
  returns void as
$$
declare
  expected_version constant int4 := 2;
  present_version int4;
begin
  if to_regclass('pg_temp.spd_workspace_version') is not null then
    select version into present_version from pg_temp.spd_workspace_version limit 1;
  end if;

  if to_regclass('pg_temp.spd_workspace_version') is not null
     and present_version is distinct from expected_version then
    drop table if exists pg_temp.spd_predecessor;
    drop table if exists pg_temp.spd_candidate;
    drop table if exists pg_temp.spd_seen;
    drop table if exists pg_temp.spd_next;
    drop table if exists pg_temp.spd_front;
    drop table if exists pg_temp.spd_workspace_version;
  end if;

  if to_regclass('pg_temp.spd_workspace_version') is null then
    create temporary table spd_workspace_version
    (
      version int4 not null primary key
    ) on commit preserve rows;

    create temporary table spd_seen
    (
      node_id int8 not null primary key,
      depth int4 not null
    ) on commit preserve rows;

    create temporary table spd_candidate
    (
      node_id int8 not null,
      depth int4 not null,
      predecessor_id int8 not null,
      edge_id int8 not null,
      primary key (depth, node_id, predecessor_id, edge_id)
    ) on commit preserve rows;
    create index spd_candidate_node_id_depth_index
      on spd_candidate using btree (node_id, depth);

    create temporary table spd_predecessor
    (
      node_id int8 not null,
      depth int4 not null,
      predecessor_id int8 not null,
      edge_id int8 not null,
      primary key (node_id, depth, predecessor_id, edge_id)
    ) on commit preserve rows;
    create index spd_predecessor_predecessor_id_depth_index
      on spd_predecessor using btree (predecessor_id, depth);

    insert into spd_workspace_version(version) values (expected_version);
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.reset_shortest_dag_workspace()
  returns void as
$$
begin
  perform public.ensure_shortest_dag_workspace();
  truncate table pg_temp.spd_seen, pg_temp.spd_candidate, pg_temp.spd_predecessor;
end;
$$
  language plpgsql
  volatile;

-- The A1 diagnostic workspace is armed only by GraphBench's untimed replay.
-- It stays separate from the A1 search workspace so ordinary calls neither
-- allocate telemetry state nor retain a previous invocation's counters.
create or replace function public.ensure_all_shortest_paths_a1_diagnostic_workspace_v1()
  returns void as
$$
begin
  if to_regclass('pg_temp.asd_telemetry_invocation') is null then
    create temporary table asd_telemetry_invocation
    (
      invocation_id text not null primary key,
      schema_version int4 not null,
      search_calls int8 not null default 0,
      source_id int8,
      target_id int8,
      runtime_branch text,
      target_depth int4,
      output_paths int8,
      fallback_executed bool,
      check (btrim(invocation_id) <> ''),
      check (search_calls >= 0),
      check (output_paths is null or output_paths >= 0)
    ) on commit preserve rows;

    create temporary table asd_telemetry_level
    (
      invocation_id text not null,
      action_index int8 not null,
      depth int4 not null,
      candidate_edges int8 not null,
      distinct_new_nodes int8 not null,
      seen_rows int8 not null,
      predecessor_rows int8 not null,
      primary key (invocation_id, action_index),
      check (action_index >= 1),
      check (depth >= 0),
      check (candidate_edges >= 0),
      check (distinct_new_nodes >= 0),
      check (seen_rows >= 0),
      check (predecessor_rows >= 0)
    ) on commit preserve rows;
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.begin_all_shortest_paths_a1_diagnostic_v1(target_invocation_id text)
  returns void as
$$
begin
  if target_invocation_id is null or btrim(target_invocation_id) = '' or length(target_invocation_id) > 256 then
    raise exception using errcode = '22023', message = 'A1 all-shortest diagnostic invocation ID must contain 1 to 256 characters';
  end if;
  perform public.ensure_all_shortest_paths_a1_diagnostic_workspace_v1();
  delete from pg_temp.asd_telemetry_level where invocation_id = target_invocation_id;
  delete from pg_temp.asd_telemetry_invocation where invocation_id = target_invocation_id;
  insert into pg_temp.asd_telemetry_invocation(invocation_id, schema_version)
  values (target_invocation_id, 1);
  -- A shallow A1 call does not otherwise touch spd_*, so clear it before every
  -- replay and make stale recursive state impossible to report as shallow work.
  perform public.reset_shortest_dag_workspace();
  perform set_config('dawgs.asd_diagnostic_invocation_id', target_invocation_id, true);
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public._start_all_shortest_paths_a1_diagnostic_v1(target_source_id int8, target_target_id int8)
  returns void as
$$
declare
  target_invocation_id text := nullif(current_setting('dawgs.asd_diagnostic_invocation_id', true), '');
begin
  if target_invocation_id is null then
    return;
  end if;
  update pg_temp.asd_telemetry_invocation
  set search_calls = search_calls + 1,
      source_id = target_source_id,
      target_id = target_target_id
  where invocation_id = target_invocation_id;
  if not found then
    raise exception using errcode = '55000', message = 'A1 all-shortest diagnostic invocation is missing';
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public._record_all_shortest_paths_a1_diagnostic_level_v1(
                                                          target_depth int4,
                                                          target_candidate_edges int8,
                                                          target_distinct_new_nodes int8,
                                                          target_seen_rows int8,
                                                          target_predecessor_rows int8)
  returns void as
$$
declare
  target_invocation_id text := nullif(current_setting('dawgs.asd_diagnostic_invocation_id', true), '');
  next_action_index int8;
begin
  if target_invocation_id is null then
    return;
  end if;
  if target_depth < 0 or target_candidate_edges < 0 or target_distinct_new_nodes < 0 or
     target_seen_rows < 0 or target_predecessor_rows < 0 then
    raise exception using errcode = '22023', message = 'A1 all-shortest diagnostic counters must be non-negative';
  end if;
  if not exists (select 1 from pg_temp.asd_telemetry_invocation where invocation_id = target_invocation_id) then
    raise exception using errcode = '55000', message = 'A1 all-shortest diagnostic invocation is missing';
  end if;
  select coalesce(max(action_index), 0) + 1 into next_action_index
  from pg_temp.asd_telemetry_level
  where invocation_id = target_invocation_id;
  insert into pg_temp.asd_telemetry_level(invocation_id, action_index, depth, candidate_edges,
                                          distinct_new_nodes, seen_rows, predecessor_rows)
  values (target_invocation_id, next_action_index, target_depth, target_candidate_edges,
          target_distinct_new_nodes, target_seen_rows, target_predecessor_rows);
end;
$$
  language plpgsql
  volatile;

create or replace function public._finish_all_shortest_paths_a1_diagnostic_v1(
                                                          target_runtime_branch text,
                                                          completed_depth int4,
                                                          completed_output_paths int8)
  returns void as
$$
declare
  target_invocation_id text := nullif(current_setting('dawgs.asd_diagnostic_invocation_id', true), '');
begin
  if target_invocation_id is null then
    return;
  end if;
  if target_runtime_branch is null or btrim(target_runtime_branch) = '' or
     completed_depth < -1 or completed_output_paths < 0 then
    raise exception using errcode = '22023', message = 'A1 all-shortest diagnostic completion is invalid';
  end if;
  update pg_temp.asd_telemetry_invocation
  set runtime_branch = target_runtime_branch,
      target_depth = completed_depth,
      output_paths = completed_output_paths,
      fallback_executed = false
  where invocation_id = target_invocation_id;
  if not found then
    raise exception using errcode = '55000', message = 'A1 all-shortest diagnostic invocation is missing';
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.read_all_shortest_paths_a1_diagnostic_v1(target_invocation_id text)
  returns jsonb as
$$
declare
  result jsonb;
begin
  if target_invocation_id is null or btrim(target_invocation_id) = '' then
    return null;
  end if;
  select jsonb_build_object(
    'schema_version', invocation.schema_version,
    'invocation_id', invocation.invocation_id,
    'scheduler', 'single_ended_level',
    'search_calls', invocation.search_calls,
    'source_id', invocation.source_id,
    'target_id', invocation.target_id,
    'runtime_branch', invocation.runtime_branch,
    'target_depth', invocation.target_depth,
    'output_paths', invocation.output_paths,
    'fallback_executed', invocation.fallback_executed,
    'levels', coalesce(levels.value, '[]'::jsonb)
  ) into result
  from pg_temp.asd_telemetry_invocation invocation
  left join lateral (
    select jsonb_agg(jsonb_build_object(
      'action_index', level.action_index,
      'depth', level.depth,
      'candidate_edges', level.candidate_edges,
      'distinct_new_nodes', level.distinct_new_nodes,
      'seen_rows', level.seen_rows,
      'predecessor_rows', level.predecessor_rows
    ) order by level.action_index) as value
    from pg_temp.asd_telemetry_level level
    where level.invocation_id = invocation.invocation_id
  ) levels on true
  where invocation.invocation_id = target_invocation_id;
  return result;
end;
$$
  language plpgsql
  stable
  strict;

create or replace function public.clear_all_shortest_paths_a1_diagnostic_v1(target_invocation_id text)
  returns void as
$$
begin
  if to_regclass('pg_temp.asd_telemetry_invocation') is not null then
    delete from pg_temp.asd_telemetry_level where invocation_id = target_invocation_id;
    delete from pg_temp.asd_telemetry_invocation where invocation_id = target_invocation_id;
  end if;
  if current_setting('dawgs.asd_diagnostic_invocation_id', true) = target_invocation_id then
    perform set_config('dawgs.asd_diagnostic_invocation_id', '', true);
  end if;
end;
$$
  language plpgsql
  volatile
  strict;

-- all_shortest_paths_dag separates minimum-depth discovery from path
-- enumeration. It retains every relationship-distinct predecessor edge at a
-- node's minimum depth, then enumerates only the resulting predecessor DAG.
-- The min_depth=1/distinct-endpoint contract is enforced by the production
-- selector; the guards below keep direct SQL callers honest as well.
create or replace function public.all_shortest_paths_dag(target_graph_id int4, source_id int8, target_id int8,
                                                          min_depth int4, max_depth int4,
                                                          edge_kind_ids int2[], inbound bool)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
#variable_conflict use_column
declare
  search_depth int4;
  target_depth int4;
  emitted_count int8;
  candidate_count int8;
  distinct_node_count int8;
  seen_count int8;
  predecessor_count int8;
  diagnostic_enabled bool := nullif(current_setting('dawgs.asd_diagnostic_invocation_id', true), '') is not null;
begin
  if source_id is null or target_id is null or max_depth < 1 then
    return;
  end if;
  if min_depth <> 1 then
    raise exception using errcode = '22023', message = 'all_shortest_paths_dag requires min_depth = 1';
  end if;
  if source_id = target_id then
    perform public.shortest_path_self_endpoint_error(source_id, target_id);
  end if;
  if diagnostic_enabled then
    perform public._start_all_shortest_paths_a1_diagnostic_v1(source_id, target_id);
  end if;

  -- Exact depth-one fast arm. Every qualifying parallel edge is observable.
  if not inbound then
    return query
      select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
      from edge e
      where e.graph_id = target_graph_id
        and e.start_id = source_id and e.end_id = target_id
        and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
      order by e.id;
  else
    return query
      select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
      from edge e
      where e.graph_id = target_graph_id
        and e.end_id = source_id and e.start_id = target_id
        and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
      order by e.id;
  end if;
  get diagnostics emitted_count = row_count;
  if emitted_count > 0 then
    if diagnostic_enabled then
      perform public._record_all_shortest_paths_a1_diagnostic_level_v1(1, emitted_count, 1, 2, emitted_count);
      perform public._finish_all_shortest_paths_a1_diagnostic_v1('one_hop_preflight', 1, emitted_count);
    end if;
    return;
  end if;

  -- Exact depth-two fast arm. Relationship uniqueness is explicit so self
  -- loops and reciprocal patterns cannot reuse one physical relationship.
  if max_depth >= 2 then
    if not inbound then
      return query
        select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.start_id = e1.end_id
        where e1.graph_id = target_graph_id
          and e1.start_id = source_id and e2.end_id = target_id
          and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        order by e1.id, e2.id;
    else
      return query
        select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.end_id = e1.start_id
        where e1.graph_id = target_graph_id
          and e1.end_id = source_id and e2.start_id = target_id
          and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        order by e1.id, e2.id;
    end if;
    get diagnostics emitted_count = row_count;
    if emitted_count > 0 then
      if diagnostic_enabled then
        if not inbound then
          select count(distinct e1.end_id) into distinct_node_count
          from edge e1
          join edge e2 on e2.graph_id = target_graph_id and e2.start_id = e1.end_id
          where e1.graph_id = target_graph_id and e1.start_id = source_id and e2.end_id = target_id and e1.id <> e2.id
            and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
            and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids));
        else
          select count(distinct e1.start_id) into distinct_node_count
          from edge e1
          join edge e2 on e2.graph_id = target_graph_id and e2.end_id = e1.start_id
          where e1.graph_id = target_graph_id and e1.end_id = source_id and e2.start_id = target_id and e1.id <> e2.id
            and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
            and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids));
        end if;
        perform public._record_all_shortest_paths_a1_diagnostic_level_v1(2, emitted_count * 2, coalesce(distinct_node_count, 0) + 1, coalesce(distinct_node_count, 0) + 2, emitted_count * 2);
        perform public._finish_all_shortest_paths_a1_diagnostic_v1('two_hop_preflight', 2, emitted_count);
      end if;
      return;
    end if;
  end if;

  if max_depth <= 2 then
    if diagnostic_enabled then
      perform public._record_all_shortest_paths_a1_diagnostic_level_v1(max_depth, 0, 0, 1, 0);
      perform public._finish_all_shortest_paths_a1_diagnostic_v1('preflight_no_path', -1, 0);
    end if;
    return;
  end if;

  perform public.reset_shortest_dag_workspace();
  insert into pg_temp.spd_seen(node_id, depth) values (source_id, 0);

  for search_depth in 1..max_depth loop
    if not inbound then
      insert into pg_temp.spd_candidate(node_id, depth, predecessor_id, edge_id)
      select e.end_id, search_depth, f.node_id, e.id
      from pg_temp.spd_seen f
      join edge e on e.graph_id = target_graph_id and e.start_id = f.node_id
      where f.depth = search_depth - 1
        and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        and not exists (select 1 from pg_temp.spd_seen s where s.node_id = e.end_id)
      on conflict do nothing;
    else
      insert into pg_temp.spd_candidate(node_id, depth, predecessor_id, edge_id)
      select e.start_id, search_depth, f.node_id, e.id
      from pg_temp.spd_seen f
      join edge e on e.graph_id = target_graph_id and e.end_id = f.node_id
      where f.depth = search_depth - 1
        and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        and not exists (select 1 from pg_temp.spd_seen s where s.node_id = e.start_id)
      on conflict do nothing;
    end if;
    get diagnostics candidate_count = row_count;

    if diagnostic_enabled then
      select count(*) into seen_count from pg_temp.spd_seen;
      select count(*) into predecessor_count from pg_temp.spd_predecessor;
    end if;

    if not exists (select 1 from pg_temp.spd_candidate where depth = search_depth) then
      if diagnostic_enabled then
        perform public._record_all_shortest_paths_a1_diagnostic_level_v1(search_depth, candidate_count, 0, seen_count, predecessor_count);
      end if;
      exit;
    end if;

    insert into pg_temp.spd_predecessor(node_id, depth, predecessor_id, edge_id)
    select node_id, search_depth, predecessor_id, edge_id
    from pg_temp.spd_candidate
    where depth = search_depth
    on conflict do nothing;

    insert into pg_temp.spd_seen(node_id, depth)
    select distinct node_id, search_depth from pg_temp.spd_candidate
    where depth = search_depth
    on conflict do nothing;
    get diagnostics distinct_node_count = row_count;
    if diagnostic_enabled then
      select count(*) into seen_count from pg_temp.spd_seen;
      select count(*) into predecessor_count from pg_temp.spd_predecessor;
    end if;
    if diagnostic_enabled then
      perform public._record_all_shortest_paths_a1_diagnostic_level_v1(search_depth, candidate_count, distinct_node_count, seen_count, predecessor_count);
    end if;

    if exists (select 1 from pg_temp.spd_candidate where depth = search_depth and node_id = target_id) then
      target_depth = search_depth;
      exit;
    end if;
  end loop;

  if target_depth is null then
    if diagnostic_enabled then
      perform public._finish_all_shortest_paths_a1_diagnostic_v1('search_no_path', -1, 0);
    end if;
    return;
  end if;

  return query
    with recursive shortest_paths(node_id, path_depth, edge_ids) as (
      select target_id, target_depth, array []::int8[]
      union all
      select predecessor.predecessor_id,
             shortest_paths.path_depth - 1,
             array[predecessor.edge_id]::int8[] || shortest_paths.edge_ids
      from shortest_paths
      join pg_temp.spd_predecessor predecessor
        on predecessor.node_id = shortest_paths.node_id
       and predecessor.depth = shortest_paths.path_depth
    )
    select source_id, target_id, target_depth, true, false, shortest_paths.edge_ids
    from shortest_paths
    where shortest_paths.node_id = source_id and shortest_paths.path_depth = 0
    order by shortest_paths.edge_ids;
  get diagnostics emitted_count = row_count;
  if diagnostic_enabled then
    perform public._finish_all_shortest_paths_a1_diagnostic_v1('single_ended_search', target_depth, emitted_count);
  end if;
end;
$$
  language plpgsql
  volatile
  strict
  cost 100
  set recursive_worktable_factor = 1
  rows 100;

-- shortest_path_compact keeps one deterministic predecessor per minimum-depth
-- node. If its bounded state budget is exceeded it restarts an exact
-- relationship-trail recursive search before returning any row, preserving the
-- transaction snapshot and the incumbent relationship-simple semantics.
create or replace function public.shortest_path_compact(target_graph_id int4, source_id int8, target_id int8,
                                                         min_depth int4, max_depth int4,
                                                         edge_kind_ids int2[], inbound bool,
                                                         state_limit int8)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
#variable_conflict use_column
declare
  search_depth int4;
  target_depth int4;
  emitted_count int8;
  retained_state int8;
  overflowed bool := false;
begin
  if source_id is null or target_id is null or max_depth < min_depth then
    return;
  end if;
  if min_depth <> 0 and min_depth <> 1 then
    raise exception using errcode = '22023', message = 'shortest_path_compact requires min_depth = 0 or 1';
  end if;
  if source_id = target_id then
    if min_depth = 0 then
      return query select source_id, target_id, 0::int4, true, false, array []::int8[];
      return;
    end if;
    perform public.shortest_path_self_endpoint_error(source_id, target_id);
  end if;

  if min_depth <= 1 and max_depth >= 1 then
    if not inbound then
      return query
        select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
        from edge e
        where e.graph_id = target_graph_id
          and e.start_id = source_id and e.end_id = target_id
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        order by e.id limit 1;
    else
      return query
        select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
        from edge e
        where e.graph_id = target_graph_id
          and e.end_id = source_id and e.start_id = target_id
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        order by e.id limit 1;
    end if;
    get diagnostics emitted_count = row_count;
    if emitted_count > 0 then
      perform public.record_requested_traversal_runtime_attestation_v1('one_hop_preflight', false, 'SP-S4-C-WE+MAT-M0');
      return;
    end if;
  end if;

  if min_depth <= 2 and max_depth >= 2 then
    if not inbound then
      return query
        select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.start_id = e1.end_id
        where e1.graph_id = target_graph_id
          and e1.start_id = source_id and e2.end_id = target_id and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        order by e1.id, e2.id limit 1;
    else
      return query
        select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.end_id = e1.start_id
        where e1.graph_id = target_graph_id
          and e1.end_id = source_id and e2.start_id = target_id and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        order by e1.id, e2.id limit 1;
    end if;
    get diagnostics emitted_count = row_count;
    if emitted_count > 0 then
      perform public.record_requested_traversal_runtime_attestation_v1('two_hop_preflight', false, 'SP-S4-C-WE+MAT-M0');
      return;
    end if;
  end if;

  if max_depth <= 2 then
    perform public.record_requested_traversal_runtime_attestation_v1('preflight_no_path', false, 'SP-S4-C-WE+MAT-M0');
    return;
  end if;

  perform public.reset_shortest_dag_workspace();
  insert into pg_temp.spd_seen(node_id, depth) values (source_id, 0);

  for search_depth in 1..max_depth loop
    if not inbound then
      insert into pg_temp.spd_candidate(node_id, depth, predecessor_id, edge_id)
      select distinct on (e.end_id) e.end_id, search_depth, f.node_id, e.id
      from pg_temp.spd_seen f
      join edge e on e.graph_id = target_graph_id and e.start_id = f.node_id
      where f.depth = search_depth - 1
        and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        and not exists (select 1 from pg_temp.spd_seen s where s.node_id = e.end_id)
      order by e.end_id, e.id, f.node_id
      limit case when state_limit > 0 then greatest(state_limit - (select count(*) from pg_temp.spd_seen) + 1, 0) else 9223372036854775807 end
      on conflict do nothing;
    else
      insert into pg_temp.spd_candidate(node_id, depth, predecessor_id, edge_id)
      select distinct on (e.start_id) e.start_id, search_depth, f.node_id, e.id
      from pg_temp.spd_seen f
      join edge e on e.graph_id = target_graph_id and e.end_id = f.node_id
      where f.depth = search_depth - 1
        and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        and not exists (select 1 from pg_temp.spd_seen s where s.node_id = e.start_id)
      order by e.start_id, e.id, f.node_id
      limit case when state_limit > 0 then greatest(state_limit - (select count(*) from pg_temp.spd_seen) + 1, 0) else 9223372036854775807 end
      on conflict do nothing;
    end if;

    if not exists (select 1 from pg_temp.spd_candidate where depth = search_depth) then
      exit;
    end if;

    if state_limit > 0 then
      select (select count(*) from pg_temp.spd_seen) +
             (select count(distinct node_id) from pg_temp.spd_candidate where depth = search_depth)
      into retained_state;
      if retained_state > state_limit then
        overflowed = true;
        exit;
      end if;
    end if;

    insert into pg_temp.spd_predecessor(node_id, depth, predecessor_id, edge_id)
    select distinct on (node_id) node_id, search_depth, predecessor_id, edge_id
    from pg_temp.spd_candidate
    where depth = search_depth
    order by node_id, edge_id, predecessor_id
    on conflict do nothing;

    insert into pg_temp.spd_seen(node_id, depth)
    select distinct node_id, search_depth from pg_temp.spd_candidate
    where depth = search_depth
    on conflict do nothing;

    if search_depth >= min_depth and exists (
      select 1 from pg_temp.spd_candidate where depth = search_depth and node_id = target_id
    ) then
      target_depth = search_depth;
      exit;
    end if;
  end loop;

  if overflowed then
    perform public.record_requested_traversal_runtime_attestation_v1('exact_relationship_trail_fallback', true, 'SP-S3-U-E+MAT-M0');
    if not inbound then
      return query
        with recursive trails(node_id, trail_depth, edge_ids) as (
          select source_id, 0::int4, array []::int8[]
          union all
          select e.end_id, trails.trail_depth + 1, trails.edge_ids || e.id
          from trails
          join edge e on e.graph_id = target_graph_id and e.start_id = trails.node_id
          where trails.trail_depth < max_depth
            and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
            and not e.id = any(trails.edge_ids)
        )
        select source_id, target_id, trails.trail_depth, true, false, trails.edge_ids
        from trails
        where trails.node_id = target_id and trails.trail_depth >= min_depth
        order by trails.trail_depth, trails.edge_ids
        limit 1;
    else
      return query
        with recursive trails(node_id, trail_depth, edge_ids) as (
          select source_id, 0::int4, array []::int8[]
          union all
          select e.start_id, trails.trail_depth + 1, trails.edge_ids || e.id
          from trails
          join edge e on e.graph_id = target_graph_id and e.end_id = trails.node_id
          where trails.trail_depth < max_depth
            and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
            and not e.id = any(trails.edge_ids)
        )
        select source_id, target_id, trails.trail_depth, true, false, trails.edge_ids
        from trails
        where trails.node_id = target_id and trails.trail_depth >= min_depth
        order by trails.trail_depth, trails.edge_ids
        limit 1;
    end if;
    return;
  end if;

  if target_depth is null then
    perform public.record_requested_traversal_runtime_attestation_v1('compact_no_path', false, 'SP-S4-C-WE+MAT-M0');
    return;
  end if;

  perform public.record_requested_traversal_runtime_attestation_v1('compact_workspace_witness', false, 'SP-S4-C-WE+MAT-M0');

  return query
    with recursive witness(node_id, path_depth, edge_ids) as (
      select target_id, target_depth, array []::int8[]
      union all
      select predecessor.predecessor_id,
             witness.path_depth - 1,
             array[predecessor.edge_id]::int8[] || witness.edge_ids
      from witness
      join pg_temp.spd_predecessor predecessor
        on predecessor.node_id = witness.node_id
       and predecessor.depth = witness.path_depth
    )
    select source_id, target_id, target_depth, true, false, witness.edge_ids
    from witness
    where witness.node_id = source_id and witness.path_depth = 0
    order by witness.edge_ids
    limit 1;
end;
$$
  language plpgsql
  volatile
  strict
  cost 100
  set recursive_worktable_factor = 1
  rows 1;

-- Compact bidirectional shortest-path candidates use a workspace that is
-- deliberately disjoint from spd_*. An overflow can therefore invoke the
-- production S4 executor in the same top-level statement without corrupting
-- either search. The version row makes pooled-session reuse fail closed when
-- the typed workspace shape changes.
create or replace function public.ensure_bidirectional_shortest_path_workspace()
  returns void as
$$
declare
  expected_version constant int4 := 1;
  present_version int4;
begin
  if to_regclass('pg_temp.spb_workspace_version') is not null then
    select version into present_version from pg_temp.spb_workspace_version limit 1;
  end if;

  if to_regclass('pg_temp.spb_workspace_version') is not null
     and present_version is distinct from expected_version then
    drop table if exists pg_temp.spb_predecessor;
    drop table if exists pg_temp.spb_candidate;
    drop table if exists pg_temp.spb_active;
    drop table if exists pg_temp.spb_seen;
    drop table if exists pg_temp.spb_front;
    drop table if exists pg_temp.spb_workspace_version;
  end if;

  if to_regclass('pg_temp.spb_workspace_version') is null then
    create temporary table spb_workspace_version
    (
      version int4 not null primary key
    ) on commit preserve rows;

    -- side is f for logical source search and b for reverse search from the
    -- logical target. queue_order is a stable FIFO order for B1; B2 groups the
    -- same ID-only rows by depth into complete levels.
    create temporary table spb_front
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      queue_order int8 not null,
      primary key (side, node_id),
      unique (side, queue_order),
      check (side in ('f', 'b'))
    ) on commit preserve rows;
    create index spb_front_side_depth_index on spb_front using btree (side, depth, queue_order);

    create temporary table spb_seen
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      primary key (side, node_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;
    create index spb_seen_node_side_index on spb_seen using btree (node_id, side, depth);

    create temporary table spb_active
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      primary key (side, node_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;

    create temporary table spb_candidate
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      adjacent_id int8 not null,
      edge_id int8 not null,
      primary key (side, node_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;

    -- For f rows adjacent_id is the predecessor toward source. For b rows it
    -- is the successor toward target. One stable edge is retained per node.
    create temporary table spb_predecessor
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      adjacent_id int8 not null,
      edge_id int8 not null,
      primary key (side, node_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;
    create index spb_predecessor_adjacent_side_index on spb_predecessor using btree (adjacent_id, side, depth);

    insert into spb_workspace_version(version) values (expected_version);
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.reset_bidirectional_shortest_path_workspace()
  returns void as
$$
begin
  perform public.ensure_bidirectional_shortest_path_workspace();
  truncate table pg_temp.spb_front, pg_temp.spb_seen, pg_temp.spb_active,
                 pg_temp.spb_candidate, pg_temp.spb_predecessor;
end;
$$
  language plpgsql
  volatile;

-- Runtime receipts bind a GraphBench latency sample to the branch executed by
-- that exact statement. The receipt is armed and read outside the timed block
-- on the same session. Instrumentation is inert unless an invocation is armed.
create or replace function public.ensure_traversal_runtime_attestation_workspace_v1()
  returns void as
$$
begin
  if to_regclass('pg_temp.traversal_runtime_attestation_v1') is null then
    create temporary table traversal_runtime_attestation_v1
    (
      invocation_id text not null primary key,
      requested_identity text not null,
      runtime_identity text,
      runtime_branch text,
      fallback_executed bool,
      record_count int4 not null default 0,
      events jsonb not null default '[]'::jsonb,
      check (btrim(invocation_id) <> ''),
      check (btrim(requested_identity) <> '')
    ) on commit preserve rows;
  end if;
  -- Avoid issuing even a no-op ALTER in ordinary read-only transactions.
  -- The conditional branch is retained for pooled sessions whose temporary
  -- v1 receipt table predates the event-chain column.
  if not exists (
    select 1
    from pg_attribute
    where attrelid = 'pg_temp.traversal_runtime_attestation_v1'::regclass
      and attname = 'events'
      and not attisdropped
  ) then
    alter table pg_temp.traversal_runtime_attestation_v1
      add column events jsonb not null default '[]'::jsonb;
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.begin_traversal_runtime_attestation_v1(
                                                         target_invocation_id text,
                                                         target_requested_identity text)
  returns void as
$$
begin
  if target_invocation_id is null or btrim(target_invocation_id) = '' or length(target_invocation_id) > 256 then
    raise exception using errcode = '22023', message = 'traversal runtime invocation ID must contain 1 to 256 characters';
  end if;
  if target_requested_identity is null or btrim(target_requested_identity) = '' or length(target_requested_identity) > 256 then
    raise exception using errcode = '22023', message = 'traversal runtime requested identity must contain 1 to 256 characters';
  end if;
  perform public.ensure_traversal_runtime_attestation_workspace_v1();
  delete from pg_temp.traversal_runtime_attestation_v1 where invocation_id = target_invocation_id;
  insert into pg_temp.traversal_runtime_attestation_v1(invocation_id, requested_identity)
  values (target_invocation_id, target_requested_identity);
  -- Session scope deliberately survives the arming autocommit. The matching
  -- clear call executes immediately after the timed statement.
  perform set_config('dawgs.traversal_runtime_invocation_id', target_invocation_id, false);
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.record_traversal_runtime_attestation_v1(
                                                         target_runtime_identity text,
                                                         target_runtime_branch text,
                                                         target_fallback_executed bool)
  returns bool as
$$
declare
  target_invocation_id text := nullif(current_setting('dawgs.traversal_runtime_invocation_id', true), '');
begin
  if target_invocation_id is null then
    return true;
  end if;
  update pg_temp.traversal_runtime_attestation_v1 receipt
  set runtime_identity = target_runtime_identity,
      runtime_branch = target_runtime_branch,
      fallback_executed = coalesce(receipt.fallback_executed, false) or target_fallback_executed,
      record_count = receipt.record_count + 1,
      events = receipt.events || jsonb_build_array(jsonb_build_object(
        'ordinal', receipt.record_count + 1,
        'runtime_identity', target_runtime_identity,
        'runtime_branch', target_runtime_branch,
        'fallback_executed', target_fallback_executed
      ))
  where receipt.invocation_id = target_invocation_id;
  if not found then
    raise exception using errcode = '55000', message = 'traversal runtime receipt is missing';
  end if;
  return true;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.record_requested_traversal_runtime_attestation_v1(
                                                         target_runtime_branch text,
                                                         target_fallback_executed bool,
                                                         target_fallback_identity text)
  returns bool as
$$
declare
  target_invocation_id text := nullif(current_setting('dawgs.traversal_runtime_invocation_id', true), '');
  target_requested_identity text;
begin
  if target_invocation_id is null then
    return true;
  end if;
  select requested_identity into target_requested_identity
  from pg_temp.traversal_runtime_attestation_v1
  where invocation_id = target_invocation_id;
  if target_requested_identity is null then
    raise exception using errcode = '55000', message = 'armed traversal runtime receipt is missing';
  end if;
  if target_fallback_executed and target_fallback_identity = 'SP-S4' then
    target_fallback_identity = case when target_requested_identity like '%-D'
      then 'SP-S4-C-D' else 'SP-S4-C-WE+MAT-M0' end;
  end if;
  return public.record_traversal_runtime_attestation_v1(
    case when target_fallback_executed then target_fallback_identity else target_requested_identity end,
    target_runtime_branch,
    target_fallback_executed
  );
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.read_traversal_runtime_attestation_v1(target_invocation_id text)
  returns jsonb as
$$
begin
  return (
    select jsonb_build_object(
      'schema_version', 2,
      'invocation_id', invocation_id,
      'requested_identity', requested_identity,
      'runtime_identity', runtime_identity,
      'runtime_branch', runtime_branch,
      'fallback_executed', fallback_executed,
      'record_count', record_count,
      'events', events
    )
    from pg_temp.traversal_runtime_attestation_v1
    where invocation_id = target_invocation_id
  );
end;
$$
  language plpgsql
  stable
  strict;

create or replace function public.clear_traversal_runtime_attestation_v1(target_invocation_id text)
  returns void as
$$
begin
  if to_regclass('pg_temp.traversal_runtime_attestation_v1') is not null then
    delete from pg_temp.traversal_runtime_attestation_v1 where invocation_id = target_invocation_id;
  end if;
  if nullif(current_setting('dawgs.traversal_runtime_invocation_id', true), '') = target_invocation_id then
    perform set_config('dawgs.traversal_runtime_invocation_id', '', false);
  end if;
end;
$$
  language plpgsql
  volatile
  strict;

-- Detailed bidirectional SP counters live in a second, independently
-- versioned temporary workspace. GraphBench enables this workspace only for
-- an untimed replay. The transaction-local invocation setting means pooled
-- sessions cannot accidentally attribute a later statement to an earlier
-- replay, while the explicit invocation key keeps every row attributable.
create or replace function public.ensure_bidirectional_shortest_path_telemetry_workspace()
  returns void as
$$
declare
  expected_version constant int4 := 1;
  present_version int4;
begin
  if to_regclass('pg_temp.spb_telemetry_workspace_version') is not null then
    select version into present_version
    from pg_temp.spb_telemetry_workspace_version
    limit 1;
  end if;

  if to_regclass('pg_temp.spb_telemetry_workspace_version') is not null
     and present_version is distinct from expected_version then
    drop table if exists pg_temp.spb_telemetry_level;
    drop table if exists pg_temp.spb_telemetry_call;
    drop table if exists pg_temp.spb_telemetry_invocation;
    drop table if exists pg_temp.spb_telemetry_workspace_version;
  end if;

  if to_regclass('pg_temp.spb_telemetry_workspace_version') is null then
    create temporary table spb_telemetry_workspace_version
    (
      version int4 not null primary key
    ) on commit preserve rows;

    create temporary table spb_telemetry_invocation
    (
      invocation_id text not null primary key,
      schema_version int4 not null,
      scheduler text,
      state_limit int8,
      frontier_limit int8,
      predecessor_limit int8,
      next_search_id int8 not null default 0,
      check (btrim(invocation_id) <> '')
    ) on commit preserve rows;

    create temporary table spb_telemetry_call
    (
      invocation_id text not null,
      search_id int8 not null,
      source_id int8 not null,
      target_id int8 not null,
      runtime_branch text not null default 'started',
      scheduler_actions int8 not null default 0,
      candidate_edges int8 not null default 0,
      distinct_new_nodes int8 not null default 0,
      seen_peak int8 not null default 0,
      frontier_peak int8 not null default 0,
      queue_peak int8 not null default 0,
      predecessor_peak int8 not null default 0,
      meeting_candidates int8 not null default 0,
      frozen_distance int4,
      witness_rows int8 not null default 0,
      overflowed bool not null default false,
      fallback_executed bool not null default false,
      primary key (invocation_id, search_id)
    ) on commit preserve rows;

    create temporary table spb_telemetry_level
    (
      invocation_id text not null,
      search_id int8 not null,
      action_index int8 not null,
      side text not null,
      action text not null,
      depth int4 not null,
      frontier_rows int8 not null,
      candidate_edges int8 not null,
      distinct_new_nodes int8 not null,
      seen_rows int8 not null,
      queue_rows int8 not null,
      predecessor_rows int8 not null,
      meeting_candidates int8 not null,
      primary key (invocation_id, search_id, action_index)
    ) on commit preserve rows;

    insert into spb_telemetry_workspace_version(version) values (expected_version);
  end if;
end;
$$
  language plpgsql
  volatile;

-- begin_bidirectional_shortest_path_diagnostic_v1 must be called inside the
-- same explicit transaction and on the same PostgreSQL connection as the
-- diagnostic replay. It clears only its own invocation key and enables
-- instrumentation through a transaction-local setting.
create or replace function public.begin_bidirectional_shortest_path_diagnostic_v1(invocation_id text)
  returns void as
$$
begin
  if invocation_id is null or btrim(invocation_id) = '' or length(invocation_id) > 256 then
    raise exception using errcode = '22023', message = 'bidirectional shortest-path diagnostic invocation ID must contain 1 to 256 characters';
  end if;

  perform public.ensure_bidirectional_shortest_path_telemetry_workspace();
  delete from pg_temp.spb_telemetry_level where spb_telemetry_level.invocation_id = begin_bidirectional_shortest_path_diagnostic_v1.invocation_id;
  delete from pg_temp.spb_telemetry_call where spb_telemetry_call.invocation_id = begin_bidirectional_shortest_path_diagnostic_v1.invocation_id;
  delete from pg_temp.spb_telemetry_invocation where spb_telemetry_invocation.invocation_id = begin_bidirectional_shortest_path_diagnostic_v1.invocation_id;
  insert into pg_temp.spb_telemetry_invocation(invocation_id, schema_version)
  values (invocation_id, 1);
  perform set_config('dawgs.spb_diagnostic_invocation_id', invocation_id, true);
end;
$$
  language plpgsql
  volatile;

-- The reader returns one self-describing document. Aggregate counters support
-- the common single-bound-pair replay, while calls preserve exact per-pair
-- attribution if a translated statement invokes the kernel more than once.
create or replace function public.read_bidirectional_shortest_path_diagnostic_v1(target_invocation_id text)
  returns jsonb as
$$
declare
  result jsonb;
begin
  select jsonb_build_object(
         'schema_version', invocation.schema_version,
         'invocation_id', invocation.invocation_id,
         'scheduler', invocation.scheduler,
         'state_limit', invocation.state_limit,
         'frontier_limit', invocation.frontier_limit,
         'predecessor_limit', invocation.predecessor_limit,
         'search_calls', coalesce(call_totals.search_calls, 0),
         'runtime_branch', coalesce(call_totals.runtime_branch, 'missing'),
         'overflowed', coalesce(call_totals.overflowed, false),
         'fallback_executed', coalesce(call_totals.fallback_executed, false),
         'counters', jsonb_build_object(
           'scheduler_actions', coalesce(call_totals.scheduler_actions, 0),
           'candidate_edges', coalesce(call_totals.candidate_edges, 0),
           'distinct_new_nodes', coalesce(call_totals.distinct_new_nodes, 0),
           'seen_peak', coalesce(call_totals.seen_peak, 0),
           'frontier_peak', coalesce(call_totals.frontier_peak, 0),
           'queue_peak', coalesce(call_totals.queue_peak, 0),
           'predecessor_peak', coalesce(call_totals.predecessor_peak, 0),
           'meeting_candidates', coalesce(call_totals.meeting_candidates, 0),
           -- -1 is the explicit no-frozen-meeting sentinel. Exact values are
           -- retained per call below when a statement evaluates many pairs.
           'frozen_distance', coalesce(call_totals.frozen_distance, -1),
           'witness_rows', coalesce(call_totals.witness_rows, 0),
           'levels', coalesce(levels.rows, '[]'::jsonb)
         ),
         'calls', coalesce(calls.rows, '[]'::jsonb)
       )
into result
from pg_temp.spb_telemetry_invocation invocation
left join lateral (
  select count(*)::int8 as search_calls,
         case when count(distinct call.runtime_branch) = 1
              then min(call.runtime_branch) else 'mixed' end as runtime_branch,
         bool_or(call.overflowed) as overflowed,
         bool_or(call.fallback_executed) as fallback_executed,
         sum(call.scheduler_actions)::int8 as scheduler_actions,
         sum(call.candidate_edges)::int8 as candidate_edges,
         sum(call.distinct_new_nodes)::int8 as distinct_new_nodes,
         max(call.seen_peak)::int8 as seen_peak,
         max(call.frontier_peak)::int8 as frontier_peak,
         max(call.queue_peak)::int8 as queue_peak,
         max(call.predecessor_peak)::int8 as predecessor_peak,
         sum(call.meeting_candidates)::int8 as meeting_candidates,
         min(call.frozen_distance)::int4 as frozen_distance,
         sum(call.witness_rows)::int8 as witness_rows
  from pg_temp.spb_telemetry_call call
  where call.invocation_id = invocation.invocation_id
) call_totals on true
left join lateral (
  select jsonb_agg(jsonb_build_object(
           'search_id', level.search_id,
           'action_index', level.action_index,
           'side', level.side,
           'action', level.action,
           'depth', level.depth,
           'frontier_rows', level.frontier_rows,
           'candidate_edges', level.candidate_edges,
           'distinct_new_nodes', level.distinct_new_nodes,
           'seen_rows', level.seen_rows,
           'queue_rows', level.queue_rows,
           'predecessor_rows', level.predecessor_rows,
           'meeting_candidates', level.meeting_candidates
         ) order by level.search_id, level.action_index) as rows
  from pg_temp.spb_telemetry_level level
  where level.invocation_id = invocation.invocation_id
) levels on true
left join lateral (
  select jsonb_agg(to_jsonb(call) - 'invocation_id' order by call.search_id) as rows
  from pg_temp.spb_telemetry_call call
  where call.invocation_id = invocation.invocation_id
) calls on true
  where invocation.invocation_id = target_invocation_id;
  return result;
end;
$$
  language plpgsql
  stable
  strict;

create or replace function public.clear_bidirectional_shortest_path_diagnostic_v1(target_invocation_id text)
  returns void as
$$
begin
  if to_regclass('pg_temp.spb_telemetry_invocation') is not null then
    delete from pg_temp.spb_telemetry_level where invocation_id = target_invocation_id;
    delete from pg_temp.spb_telemetry_call where invocation_id = target_invocation_id;
    delete from pg_temp.spb_telemetry_invocation where invocation_id = target_invocation_id;
  end if;
  if nullif(current_setting('dawgs.spb_diagnostic_invocation_id', true), '') = target_invocation_id then
    perform set_config('dawgs.spb_diagnostic_invocation_id', '', true);
  end if;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public._start_bidirectional_shortest_path_diagnostic_call_v1(
                                                         target_invocation_id text,
                                                         target_scheduler text,
                                                         target_state_limit int8,
                                                         target_frontier_limit int8,
                                                         target_predecessor_limit int8,
                                                         target_source_id int8,
                                                         target_target_id int8)
  returns int8 as
$$
declare
  target_search_id int8;
begin
  if target_invocation_id is null then
    return null;
  end if;
  if to_regclass('pg_temp.spb_telemetry_invocation') is null then
    raise exception using errcode = '55000', message = 'bidirectional shortest-path diagnostic replay was not initialized on this session';
  end if;

  update pg_temp.spb_telemetry_invocation invocation
  set scheduler = coalesce(invocation.scheduler, target_scheduler),
      state_limit = coalesce(invocation.state_limit, target_state_limit),
      frontier_limit = coalesce(invocation.frontier_limit, target_frontier_limit),
      predecessor_limit = coalesce(invocation.predecessor_limit, target_predecessor_limit),
      next_search_id = invocation.next_search_id + 1
  where invocation.invocation_id = target_invocation_id
    and (invocation.scheduler is null or invocation.scheduler = target_scheduler)
    and (invocation.state_limit is null or invocation.state_limit = target_state_limit)
    and (invocation.frontier_limit is null or invocation.frontier_limit = target_frontier_limit)
    and (invocation.predecessor_limit is null or invocation.predecessor_limit = target_predecessor_limit)
  returning invocation.next_search_id into target_search_id;

  if target_search_id is null then
    raise exception using
      errcode = '55000',
      message = 'bidirectional shortest-path diagnostic invocation is missing or mixes scheduler/cap identities';
  end if;

  insert into pg_temp.spb_telemetry_call(invocation_id, search_id, source_id, target_id)
  values (target_invocation_id, target_search_id, target_source_id, target_target_id);
  return target_search_id;
end;
$$
  language plpgsql
  volatile;

create or replace function public._record_bidirectional_shortest_path_diagnostic_level_v1(
                                                         target_invocation_id text,
                                                         target_search_id int8,
                                                         target_action_index int8,
                                                         target_side text,
                                                         target_action text,
                                                         target_depth int4,
                                                         target_frontier_rows int8,
                                                         target_candidate_edges int8,
                                                         target_distinct_new_nodes int8,
                                                         target_seen_rows int8,
                                                         target_queue_rows int8,
                                                         target_predecessor_rows int8,
                                                         target_meeting_candidates int8)
  returns void as
$$
begin
  insert into pg_temp.spb_telemetry_level(
    invocation_id, search_id, action_index, side, action, depth,
    frontier_rows, candidate_edges, distinct_new_nodes, seen_rows,
    queue_rows, predecessor_rows, meeting_candidates)
  values (
    target_invocation_id, target_search_id, target_action_index, target_side,
    target_action, target_depth, target_frontier_rows, target_candidate_edges,
    target_distinct_new_nodes, target_seen_rows, target_queue_rows,
    target_predecessor_rows, target_meeting_candidates);
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public._finish_bidirectional_shortest_path_diagnostic_call_v1(
                                                         target_invocation_id text,
                                                         target_search_id int8,
                                                         target_runtime_branch text,
                                                         target_scheduler_actions int8,
                                                         target_candidate_edges int8,
                                                         target_distinct_new_nodes int8,
                                                         target_seen_peak int8,
                                                         target_frontier_peak int8,
                                                         target_queue_peak int8,
                                                         target_predecessor_peak int8,
                                                         target_meeting_candidates int8,
                                                         target_frozen_distance int4,
                                                         target_witness_rows int8,
                                                         target_overflowed bool,
                                                         target_fallback_executed bool)
  returns void as
$$
begin
  update pg_temp.spb_telemetry_call call
  set runtime_branch = target_runtime_branch,
      scheduler_actions = target_scheduler_actions,
      candidate_edges = target_candidate_edges,
      distinct_new_nodes = target_distinct_new_nodes,
      seen_peak = target_seen_peak,
      frontier_peak = target_frontier_peak,
      queue_peak = target_queue_peak,
      predecessor_peak = target_predecessor_peak,
      meeting_candidates = target_meeting_candidates,
      frozen_distance = target_frozen_distance,
      witness_rows = target_witness_rows,
      overflowed = target_overflowed,
      fallback_executed = target_fallback_executed
  where call.invocation_id = target_invocation_id
    and call.search_id = target_search_id;

  if not found then
    raise exception using errcode = '55000', message = 'bidirectional shortest-path diagnostic call is missing';
  end if;
end;
$$
  language plpgsql
  volatile;

-- shortest_path_bidirectional_compact_v1 is the common typed kernel for the
-- B1 and B2 tournament arms. Queue-head depths are lower bounds on every
-- undiscovered source/target distance. Once their sum is at least the best
-- completed meeting distance, no unexpanded pair can produce a shorter path.
-- B1 applies this proof after deterministic one-node alternation; B2 applies it
-- only between complete-level expansions. Merely finding an intersection is
-- never a termination condition.
--
-- Admission is fail-closed. Candidate state is materialized with LIMIT cap+1
-- before any seen/front/predecessor mutation. If total seen rows, queued
-- frontier rows, or retained predecessors exceed their independent bound, the
-- function invokes exact S4 before returning any candidate row. VOLATILE
-- PL/pgSQL statements do not provide one transaction snapshot at READ
-- COMMITTED, so the kernel rejects that isolation level. At REPEATABLE READ or
-- SERIALIZABLE, candidate search and nested S4 fallback observe the same
-- transaction snapshot; spb_/spd_ state remains disjoint.
create or replace function public.shortest_path_bidirectional_compact_v1(
                                                         target_graph_id int4,
                                                         source_id int8,
                                                         target_id int8,
                                                         min_depth int4,
                                                         max_depth int4,
                                                         edge_kind_ids int2[],
                                                         inbound bool,
                                                         state_limit int8,
                                                         frontier_limit int8,
                                                         predecessor_limit int8,
                                                         scheduler text)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
#variable_conflict use_column
declare
  chosen_side char(1);
  strict_side char(1) := 'f';
  forward_depth int4;
  backward_depth int4;
  forward_width int8;
  backward_width int8;
  forward_tail int8 := 0;
  backward_tail int8 := 0;
  seen_rows int8;
  active_rows int8;
  frontier_rows int8;
  predecessor_rows int8;
  candidate_rows int8;
  admission_limit int8;
  candidate_meeting int8;
  candidate_distance int4;
  best_meeting int8;
  best_distance int4;
  emitted_count int8;
  overflowed bool := false;
  telemetry_invocation_id text := nullif(current_setting('dawgs.spb_diagnostic_invocation_id', true), '');
  telemetry_search_id int8;
  telemetry_action_index int8 := 0;
  telemetry_action_depth int4 := 0;
  telemetry_action_candidate_edges int8 := 0;
  telemetry_action_meetings int8 := 0;
  telemetry_scheduler_actions int8 := 0;
  telemetry_candidate_edges int8 := 0;
  telemetry_distinct_new_nodes int8 := 0;
  telemetry_seen_peak int8 := 0;
  telemetry_frontier_peak int8 := 0;
  telemetry_queue_peak int8 := 0;
  telemetry_predecessor_peak int8 := 0;
  telemetry_meeting_candidates int8 := 0;
begin
  if source_id is null or target_id is null or max_depth < min_depth then
    return;
  end if;
  if scheduler <> 'strict_alternating_node' and scheduler <> 'smaller_current_level' then
    raise exception using errcode = '22023', message = 'unknown compact bidirectional shortest-path scheduler';
  end if;
  if min_depth <> 0 and min_depth <> 1 then
    raise exception using errcode = '22023', message = 'compact bidirectional shortest path requires min_depth = 0 or 1';
  end if;
  if max_depth > 64 then
    raise exception using errcode = '22023', message = 'compact bidirectional shortest path requires max_depth <= 64';
  end if;
  if state_limit <= 0 or frontier_limit <= 0 or predecessor_limit <= 0 then
    raise exception using errcode = '22023', message = 'compact bidirectional shortest path requires positive state, frontier, and predecessor limits';
  end if;
  if current_setting('transaction_isolation') <> 'repeatable read'
     and current_setting('transaction_isolation') <> 'serializable' then
    raise exception using
      errcode = '25001',
      message = 'compact bidirectional shortest path requires REPEATABLE READ or SERIALIZABLE transaction isolation';
  end if;

  telemetry_search_id = public._start_bidirectional_shortest_path_diagnostic_call_v1(
    telemetry_invocation_id, scheduler, state_limit, frontier_limit,
    predecessor_limit, source_id, target_id);

  -- Exact zero-hop preflight precedes workspace allocation.
  if source_id = target_id then
    if min_depth = 0 then
      return query select source_id, target_id, 0::int4, true, false, array []::int8[];
      get diagnostics emitted_count = row_count;
      if telemetry_search_id is not null then
        telemetry_action_index = telemetry_action_index + 1;
        perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          'none', 'preflight_zero_hop', 0, 0, 0, 0, 0, 0, 0, emitted_count);
        perform public._finish_bidirectional_shortest_path_diagnostic_call_v1(
          telemetry_invocation_id, telemetry_search_id, 'zero_hop_preflight',
          0, 0, 0, 0, 0, 0, 0, emitted_count, 0, emitted_count, false, false);
      end if;
      perform public.record_requested_traversal_runtime_attestation_v1('zero_hop_preflight', false, 'SP-S4');
      return;
    end if;
    perform public.shortest_path_self_endpoint_error(source_id, target_id);
  end if;

  -- Exact one-hop preflight chooses the same deterministic edge ordering as S4.
  if min_depth <= 1 and max_depth >= 1 then
    if not inbound then
      return query
        select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
        from edge e
        where e.graph_id = target_graph_id
          and e.start_id = source_id and e.end_id = target_id
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        order by e.id limit 1;
    else
      return query
        select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
        from edge e
        where e.graph_id = target_graph_id
          and e.end_id = source_id and e.start_id = target_id
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        order by e.id limit 1;
    end if;
    get diagnostics emitted_count = row_count;
    if emitted_count > 0 then
      if telemetry_search_id is not null then
        if not inbound then
          select count(*) into telemetry_action_candidate_edges
          from edge e
          where e.graph_id = target_graph_id
            and e.start_id = source_id and e.end_id = target_id
            and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids));
        else
          select count(*) into telemetry_action_candidate_edges
          from edge e
          where e.graph_id = target_graph_id
            and e.end_id = source_id and e.start_id = target_id
            and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids));
        end if;
        telemetry_candidate_edges = telemetry_candidate_edges + telemetry_action_candidate_edges;
        telemetry_meeting_candidates = telemetry_meeting_candidates + telemetry_action_candidate_edges;
        telemetry_action_index = telemetry_action_index + 1;
        perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          'none', 'preflight_one_hop', 1, 0, telemetry_action_candidate_edges,
          0, 0, 0, 0, telemetry_action_candidate_edges);
        perform public._finish_bidirectional_shortest_path_diagnostic_call_v1(
          telemetry_invocation_id, telemetry_search_id, 'one_hop_preflight',
          0, telemetry_candidate_edges, 0, 0, 0, 0, 0,
          telemetry_meeting_candidates, 1, emitted_count, false, false);
      end if;
      perform public.record_requested_traversal_runtime_attestation_v1('one_hop_preflight', false, 'SP-S4');
      return;
    end if;
  end if;

  -- Exact two-hop preflight retains relationship uniqueness and public order.
  if min_depth <= 2 and max_depth >= 2 then
    if not inbound then
      return query
        select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.start_id = e1.end_id
        where e1.graph_id = target_graph_id
          and e1.start_id = source_id and e2.end_id = target_id and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        order by e1.id, e2.id limit 1;
    else
      return query
        select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.end_id = e1.start_id
        where e1.graph_id = target_graph_id
          and e1.end_id = source_id and e2.start_id = target_id and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        order by e1.id, e2.id limit 1;
    end if;
    get diagnostics emitted_count = row_count;
    if emitted_count > 0 then
      if telemetry_search_id is not null then
        if not inbound then
          select count(*) * 2 into telemetry_action_candidate_edges
          from edge e1
          join edge e2 on e2.graph_id = target_graph_id and e2.start_id = e1.end_id
          where e1.graph_id = target_graph_id
            and e1.start_id = source_id and e2.end_id = target_id and e1.id <> e2.id
            and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
            and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids));
        else
          select count(*) * 2 into telemetry_action_candidate_edges
          from edge e1
          join edge e2 on e2.graph_id = target_graph_id and e2.end_id = e1.start_id
          where e1.graph_id = target_graph_id
            and e1.end_id = source_id and e2.start_id = target_id and e1.id <> e2.id
            and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
            and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids));
        end if;
        telemetry_candidate_edges = telemetry_candidate_edges + telemetry_action_candidate_edges;
        telemetry_action_meetings = telemetry_action_candidate_edges / 2;
        telemetry_meeting_candidates = telemetry_meeting_candidates + telemetry_action_meetings;
        telemetry_action_index = telemetry_action_index + 1;
        perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          'none', 'preflight_two_hop', 2, 0, telemetry_action_candidate_edges,
          0, 0, 0, 0, telemetry_action_meetings);
        perform public._finish_bidirectional_shortest_path_diagnostic_call_v1(
          telemetry_invocation_id, telemetry_search_id, 'two_hop_preflight',
          0, telemetry_candidate_edges, 0, 0, 0, 0, 0,
          telemetry_meeting_candidates, 2, emitted_count, false, false);
      end if;
      perform public.record_requested_traversal_runtime_attestation_v1('two_hop_preflight', false, 'SP-S4');
      return;
    end if;
  end if;
  if max_depth <= 2 then
    if telemetry_search_id is not null then
      telemetry_action_index = telemetry_action_index + 1;
      perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
        telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
        'none', 'preflight_no_path', max_depth, 0, 0, 0, 0, 0, 0, 0);
      perform public._finish_bidirectional_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'preflight_no_path',
        0, 0, 0, 0, 0, 0, 0, 0, null, 0, false, false);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('preflight_no_path', false, 'SP-S4');
    return;
  end if;

  -- Both roots count toward seen and frontier admission. Overflow falls back
  -- before allocating or exposing candidate state.
  if state_limit < 2 or frontier_limit < 2 then
    overflowed = true;
    if telemetry_search_id is not null then
      telemetry_action_index = telemetry_action_index + 1;
      perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
        telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
        'none', 'root_admission', 0, 2, 0, 0, 2, 2, 0, 0);
      telemetry_frontier_peak = 2;
      telemetry_queue_peak = 2;
    end if;
  else
    perform public.reset_bidirectional_shortest_path_workspace();
    insert into pg_temp.spb_front(side, node_id, depth, queue_order)
    values ('f', source_id, 0, 0), ('b', target_id, 0, 0);
    insert into pg_temp.spb_seen(side, node_id, depth)
    values ('f', source_id, 0), ('b', target_id, 0);
    telemetry_seen_peak = 2;
    telemetry_frontier_peak = 2;
    telemetry_queue_peak = 2;
  end if;

  while not overflowed loop
    select min(depth), count(*) filter (where depth = (select min(depth) from pg_temp.spb_front where side = 'f'))
      into forward_depth, forward_width
      from pg_temp.spb_front where side = 'f';
    select min(depth), count(*) filter (where depth = (select min(depth) from pg_temp.spb_front where side = 'b'))
      into backward_depth, backward_width
      from pg_temp.spb_front where side = 'b';

    if forward_depth is null or backward_depth is null then
      exit;
    end if;

    -- Dijkstra/BFS lower bound over the two next accepted queue depths.
    if best_distance is not null and forward_depth + backward_depth >= best_distance then
      exit;
    end if;

    truncate table pg_temp.spb_active, pg_temp.spb_candidate;
    if scheduler = 'strict_alternating_node' then
      chosen_side = strict_side;
      if (chosen_side = 'f' and forward_width = 0) or (chosen_side = 'b' and backward_width = 0) then
        chosen_side = case chosen_side when 'f' then 'b' else 'f' end;
      end if;
      strict_side = case chosen_side when 'f' then 'b' else 'f' end;

      insert into pg_temp.spb_active(side, node_id, depth)
      select side, node_id, depth
      from pg_temp.spb_front
      where side = chosen_side
      order by queue_order
      limit 1;
    else
      -- B2 expands the complete smaller current level. Equality always chooses
      -- the forward side, freezing the tie break across artifacts.
      chosen_side = case when forward_width <= backward_width then 'f' else 'b' end;
      insert into pg_temp.spb_active(side, node_id, depth)
      select side, node_id, depth
      from pg_temp.spb_front
      where side = chosen_side
        and depth = case chosen_side when 'f' then forward_depth else backward_depth end
      order by queue_order;
    end if;

    delete from pg_temp.spb_front front
    using pg_temp.spb_active active
    where front.side = active.side and front.node_id = active.node_id;

    telemetry_scheduler_actions = telemetry_scheduler_actions + 1;
    telemetry_action_candidate_edges = 0;
    telemetry_action_meetings = 0;
    select min(depth) into telemetry_action_depth from pg_temp.spb_active;

    if not exists (select 1 from pg_temp.spb_active where depth < max_depth) then
      if telemetry_search_id is not null then
        select count(*) into seen_rows from pg_temp.spb_seen;
        select count(*) into active_rows from pg_temp.spb_active;
        select count(*) into frontier_rows from pg_temp.spb_front;
        select count(*) into predecessor_rows from pg_temp.spb_predecessor;
        telemetry_action_index = telemetry_action_index + 1;
        telemetry_seen_peak = greatest(telemetry_seen_peak, seen_rows);
        telemetry_frontier_peak = greatest(telemetry_frontier_peak, active_rows + frontier_rows);
        telemetry_queue_peak = greatest(telemetry_queue_peak, frontier_rows);
        telemetry_predecessor_peak = greatest(telemetry_predecessor_peak, predecessor_rows);
        perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          chosen_side::text,
          case scheduler when 'strict_alternating_node' then 'dequeue_node' else 'expand_level' end,
          telemetry_action_depth, active_rows + frontier_rows, 0, 0,
          seen_rows, frontier_rows, predecessor_rows, 0);
      end if;
      continue;
    end if;

    select count(*) into seen_rows from pg_temp.spb_seen;
    select count(*) into active_rows from pg_temp.spb_active;
    select count(*) into frontier_rows from pg_temp.spb_front;
    select count(*) into predecessor_rows from pg_temp.spb_predecessor;
    admission_limit = least(state_limit - seen_rows,
                            frontier_limit - active_rows - frontier_rows,
                            predecessor_limit - predecessor_rows);
    if admission_limit < 0 then
      overflowed = true;
      if telemetry_search_id is not null then
        telemetry_action_index = telemetry_action_index + 1;
        telemetry_seen_peak = greatest(telemetry_seen_peak, seen_rows);
        telemetry_frontier_peak = greatest(telemetry_frontier_peak, active_rows + frontier_rows);
        telemetry_queue_peak = greatest(telemetry_queue_peak, frontier_rows);
        telemetry_predecessor_peak = greatest(telemetry_predecessor_peak, predecessor_rows);
        perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          chosen_side::text,
          case scheduler when 'strict_alternating_node' then 'dequeue_node' else 'expand_level' end,
          telemetry_action_depth, active_rows + frontier_rows, 0, 0,
          seen_rows, frontier_rows, predecessor_rows, 0);
      end if;
      exit;
    end if;

    -- Candidate selection is graph scoped, ID only, and bounded at cap+1.
    -- DISTINCT ON freezes one predecessor/successor before workspace mutation.
    if chosen_side = 'f' and not inbound then
      if telemetry_search_id is not null then
        select count(*) into telemetry_action_candidate_edges
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'f' and seen.node_id = e.end_id);
      end if;
      insert into pg_temp.spb_candidate(side, node_id, depth, adjacent_id, edge_id)
      select 'f', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select distinct on (e.end_id) e.end_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'f' and seen.node_id = e.end_id)
        order by e.end_id, e.id, active.node_id
        limit admission_limit + 1
      ) candidate;
    elsif chosen_side = 'f' and inbound then
      if telemetry_search_id is not null then
        select count(*) into telemetry_action_candidate_edges
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'f' and seen.node_id = e.start_id);
      end if;
      insert into pg_temp.spb_candidate(side, node_id, depth, adjacent_id, edge_id)
      select 'f', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select distinct on (e.start_id) e.start_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'f' and seen.node_id = e.start_id)
        order by e.start_id, e.id, active.node_id
        limit admission_limit + 1
      ) candidate;
    elsif chosen_side = 'b' and not inbound then
      if telemetry_search_id is not null then
        select count(*) into telemetry_action_candidate_edges
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'b' and seen.node_id = e.start_id);
      end if;
      insert into pg_temp.spb_candidate(side, node_id, depth, adjacent_id, edge_id)
      select 'b', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select distinct on (e.start_id) e.start_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'b' and seen.node_id = e.start_id)
        order by e.start_id, e.id, active.node_id
        limit admission_limit + 1
      ) candidate;
    else
      if telemetry_search_id is not null then
        select count(*) into telemetry_action_candidate_edges
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'b' and seen.node_id = e.end_id);
      end if;
      insert into pg_temp.spb_candidate(side, node_id, depth, adjacent_id, edge_id)
      select 'b', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select distinct on (e.end_id) e.end_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.spb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.spb_seen seen where seen.side = 'b' and seen.node_id = e.end_id)
        order by e.end_id, e.id, active.node_id
        limit admission_limit + 1
      ) candidate;
    end if;

    select count(*) into candidate_rows from pg_temp.spb_candidate;
    if telemetry_search_id is not null then
      select count(*) into telemetry_action_meetings
      from pg_temp.spb_candidate candidate
      join pg_temp.spb_seen opposite
        on opposite.node_id = candidate.node_id and opposite.side <> candidate.side
      where candidate.depth + opposite.depth between min_depth and max_depth;
      telemetry_candidate_edges = telemetry_candidate_edges + telemetry_action_candidate_edges;
      telemetry_distinct_new_nodes = telemetry_distinct_new_nodes + candidate_rows;
      telemetry_meeting_candidates = telemetry_meeting_candidates + telemetry_action_meetings;
      telemetry_seen_peak = greatest(telemetry_seen_peak, seen_rows + candidate_rows);
      telemetry_frontier_peak = greatest(telemetry_frontier_peak, active_rows + frontier_rows + candidate_rows);
      telemetry_queue_peak = greatest(telemetry_queue_peak, frontier_rows + candidate_rows);
      telemetry_predecessor_peak = greatest(telemetry_predecessor_peak, predecessor_rows + candidate_rows);
      telemetry_action_index = telemetry_action_index + 1;
      perform public._record_bidirectional_shortest_path_diagnostic_level_v1(
        telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
        chosen_side::text,
        case scheduler when 'strict_alternating_node' then 'dequeue_node' else 'expand_level' end,
        telemetry_action_depth, active_rows + frontier_rows + candidate_rows,
        telemetry_action_candidate_edges, candidate_rows, seen_rows + candidate_rows,
        frontier_rows + candidate_rows, predecessor_rows + candidate_rows,
        telemetry_action_meetings);
    end if;
    if seen_rows + candidate_rows > state_limit
       or active_rows + frontier_rows + candidate_rows > frontier_limit
       or predecessor_rows + candidate_rows > predecessor_limit then
      overflowed = true;
      exit;
    end if;

    insert into pg_temp.spb_predecessor(side, node_id, depth, adjacent_id, edge_id)
    select side, node_id, depth, adjacent_id, edge_id
    from pg_temp.spb_candidate
    order by side, node_id;
    insert into pg_temp.spb_seen(side, node_id, depth)
    select side, node_id, depth from pg_temp.spb_candidate order by side, node_id;

    if chosen_side = 'f' then
      insert into pg_temp.spb_front(side, node_id, depth, queue_order)
      select side, node_id, depth,
             forward_tail + row_number() over (order by edge_id, node_id, adjacent_id)
      from pg_temp.spb_candidate;
      forward_tail = forward_tail + candidate_rows;
    else
      insert into pg_temp.spb_front(side, node_id, depth, queue_order)
      select side, node_id, depth,
             backward_tail + row_number() over (order by edge_id, node_id, adjacent_id)
      from pg_temp.spb_candidate;
      backward_tail = backward_tail + candidate_rows;
    end if;

    candidate_meeting = null;
    candidate_distance = null;
    select candidate.node_id, candidate.depth + opposite.depth
      into candidate_meeting, candidate_distance
      from pg_temp.spb_candidate candidate
      join pg_temp.spb_seen opposite
        on opposite.node_id = candidate.node_id and opposite.side <> candidate.side
      where candidate.depth + opposite.depth between min_depth and max_depth
      order by candidate.depth + opposite.depth, candidate.node_id
      limit 1;
    if candidate_distance is not null
       and (best_distance is null
            or candidate_distance < best_distance
            or (candidate_distance = best_distance and candidate_meeting < best_meeting)) then
      best_distance = candidate_distance;
      best_meeting = candidate_meeting;
    end if;
  end loop;

  if overflowed then
    return query
      select fallback.root_id, fallback.next_id, fallback.depth,
             fallback.satisfied, fallback.is_cycle, fallback.path
      from public.shortest_path_compact(target_graph_id, source_id, target_id,
                                        min_depth, max_depth, edge_kind_ids,
                                        inbound, state_limit) fallback;
    get diagnostics emitted_count = row_count;
    if telemetry_search_id is not null then
      perform public._finish_bidirectional_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'exact_s4_fallback',
        telemetry_scheduler_actions, telemetry_candidate_edges,
        telemetry_distinct_new_nodes, telemetry_seen_peak,
        telemetry_frontier_peak, telemetry_queue_peak,
        telemetry_predecessor_peak, telemetry_meeting_candidates,
        best_distance, emitted_count, true, true);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('exact_s4_fallback', true, 'SP-S4');
    return;
  end if;
  if best_distance is null then
    if telemetry_search_id is not null then
      perform public._finish_bidirectional_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'search_no_path',
        telemetry_scheduler_actions, telemetry_candidate_edges,
        telemetry_distinct_new_nodes, telemetry_seen_peak,
        telemetry_frontier_peak, telemetry_queue_peak,
        telemetry_predecessor_peak, telemetry_meeting_candidates,
        null, 0, false, false);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('search_no_path', false, 'SP-S4');
    return;
  end if;

  -- Path arrays exist only at the late output boundary. Forward predecessor
  -- edges are prepended back to source; backward successor edges are appended
  -- toward target, preserving logical source-to-target order for both physical
  -- edge orientations.
  return query
    with recursive
    forward_witness(node_id, edge_ids) as (
      select best_meeting, array []::int8[]
      union all
      select predecessor.adjacent_id,
             array[predecessor.edge_id]::int8[] || forward_witness.edge_ids
      from forward_witness
      join pg_temp.spb_predecessor predecessor
        on predecessor.side = 'f' and predecessor.node_id = forward_witness.node_id
    ),
    backward_witness(node_id, edge_ids) as (
      select best_meeting, array []::int8[]
      union all
      select successor.adjacent_id,
             backward_witness.edge_ids || successor.edge_id
      from backward_witness
      join pg_temp.spb_predecessor successor
        on successor.side = 'b' and successor.node_id = backward_witness.node_id
    )
    select source_id, target_id, best_distance, true, false,
           forward_witness.edge_ids || backward_witness.edge_ids
    from forward_witness
    join backward_witness on forward_witness.node_id = source_id
                         and backward_witness.node_id = target_id
    limit 1;
  get diagnostics emitted_count = row_count;
  if telemetry_search_id is not null then
    perform public._finish_bidirectional_shortest_path_diagnostic_call_v1(
      telemetry_invocation_id, telemetry_search_id, 'bidirectional_search',
      telemetry_scheduler_actions, telemetry_candidate_edges,
      telemetry_distinct_new_nodes, telemetry_seen_peak,
      telemetry_frontier_peak, telemetry_queue_peak,
      telemetry_predecessor_peak, telemetry_meeting_candidates,
      best_distance, emitted_count, false, false);
  end if;
  perform public.record_requested_traversal_runtime_attestation_v1('bidirectional_search', false, 'SP-S4');
end;
$$
  language plpgsql
  volatile
  strict
  cost 100
  set recursive_worktable_factor = 1
  rows 1;

-- B1 freezes Neo4j-4.4-style strict one-node alternation behind a typed
-- wrapper so scheduler identity is not inferred from generated SQL text.
create or replace function public.shortest_path_b1_strict_alternating(
                                                         target_graph_id int4,
                                                         source_id int8,
                                                         target_id int8,
                                                         min_depth int4,
                                                         max_depth int4,
                                                         edge_kind_ids int2[],
                                                         inbound bool,
                                                         state_limit int8,
                                                         frontier_limit int8,
                                                         predecessor_limit int8)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
select *
from public.shortest_path_bidirectional_compact_v1(
  target_graph_id, source_id, target_id, min_depth, max_depth,
  edge_kind_ids, inbound, state_limit, frontier_limit, predecessor_limit,
  'strict_alternating_node');
$$
  language sql
  volatile
  strict
  cost 100
  rows 1;

-- B2 expands a complete current level from the smaller side, with a stable
-- forward-side tie break.
create or replace function public.shortest_path_b2_smaller_current_level(
                                                         target_graph_id int4,
                                                         source_id int8,
                                                         target_id int8,
                                                         min_depth int4,
                                                         max_depth int4,
                                                         edge_kind_ids int2[],
                                                         inbound bool,
                                                         state_limit int8,
                                                         frontier_limit int8,
                                                         predecessor_limit int8)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
select *
from public.shortest_path_bidirectional_compact_v1(
  target_graph_id, source_id, target_id, min_depth, max_depth,
  edge_kind_ids, inbound, state_limit, frontier_limit, predecessor_limit,
  'smaller_current_level');
$$
  language sql
  volatile
  strict
  cost 100
  rows 1;

-- Compact bidirectional all-shortest-path candidates use a workspace that is
-- disjoint from both the production ASP-A1 spd_* state and singleton SP spb_*
-- state. Discovery, relationship-distinct predecessor retention, path-count
-- calculation, and staged output therefore have separately measurable shapes.
create or replace function public.ensure_bidirectional_all_shortest_path_workspace()
  returns void as
$$
declare
  expected_version constant int4 := 1;
  present_version int4;
begin
  if to_regclass('pg_temp.asb_workspace_version') is not null then
    select version into present_version from pg_temp.asb_workspace_version limit 1;
  end if;

  if to_regclass('pg_temp.asb_workspace_version') is not null
     and present_version is distinct from expected_version then
    drop table if exists pg_temp.asb_output;
    drop table if exists pg_temp.asb_path_count;
    drop table if exists pg_temp.asb_predecessor;
    drop table if exists pg_temp.asb_candidate_predecessor;
    drop table if exists pg_temp.asb_candidate_node;
    drop table if exists pg_temp.asb_active;
    drop table if exists pg_temp.asb_seen;
    drop table if exists pg_temp.asb_front;
    drop table if exists pg_temp.asb_workspace_version;
  end if;

  if to_regclass('pg_temp.asb_workspace_version') is null then
    create temporary table asb_workspace_version
    (
      version int4 not null primary key
    ) on commit preserve rows;

    create temporary table asb_front
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      queue_order int8 not null,
      primary key (side, node_id),
      unique (side, queue_order),
      check (side in ('f', 'b'))
    ) on commit preserve rows;
    create index asb_front_side_depth_order_index
      on asb_front using btree (side, depth, queue_order);

    create temporary table asb_seen
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      primary key (side, node_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;
    create index asb_seen_node_side_depth_index
      on asb_seen using btree (node_id, side, depth);

    create temporary table asb_active
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      primary key (side, node_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;

    create temporary table asb_candidate_node
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      primary key (side, node_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;

    create temporary table asb_candidate_predecessor
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      adjacent_id int8 not null,
      edge_id int8 not null,
      primary key (side, node_id, depth, adjacent_id, edge_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;

    -- Forward adjacent_id points toward the logical source. Backward
    -- adjacent_id points toward the logical target. Equal-depth rows are not
    -- collapsed: every relationship-distinct shortest predecessor/successor
    -- is retained.
    create temporary table asb_predecessor
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      adjacent_id int8 not null,
      edge_id int8 not null,
      primary key (side, node_id, depth, adjacent_id, edge_id),
      check (side in ('f', 'b'))
    ) on commit preserve rows;
    create index asb_predecessor_node_side_depth_index
      on asb_predecessor using btree (node_id, side, depth);
    create index asb_predecessor_adjacent_side_depth_index
      on asb_predecessor using btree (adjacent_id, side, depth);

    create temporary table asb_path_count
    (
      side char(1) not null,
      node_id int8 not null,
      depth int4 not null,
      path_count int8 not null,
      primary key (side, node_id),
      check (side in ('f', 'b')),
      check (path_count >= 0)
    ) on commit preserve rows;

    create temporary table asb_output
    (
      edge_ids int8[] not null primary key,
      output_bytes int8 not null,
      check (output_bytes >= 0)
    ) on commit preserve rows;

    insert into asb_workspace_version(version) values (expected_version);
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.reset_bidirectional_all_shortest_path_workspace()
  returns void as
$$
begin
  perform public.ensure_bidirectional_all_shortest_path_workspace();
  truncate table pg_temp.asb_front, pg_temp.asb_seen, pg_temp.asb_active,
                 pg_temp.asb_candidate_node, pg_temp.asb_candidate_predecessor,
                 pg_temp.asb_predecessor, pg_temp.asb_path_count,
                 pg_temp.asb_output;
end;
$$
  language plpgsql
  volatile;

-- clear_bidirectional_all_shortest_path_workspace does not allocate state.
-- Overflow paths call it before ASP-A1 so no candidate rows survive into the
-- exact fallback boundary.
create or replace function public.clear_bidirectional_all_shortest_path_workspace()
  returns void as
$$
begin
  if to_regclass('pg_temp.asb_workspace_version') is not null then
    execute 'truncate table pg_temp.asb_front, pg_temp.asb_seen, pg_temp.asb_active, '
            'pg_temp.asb_candidate_node, pg_temp.asb_candidate_predecessor, '
            'pg_temp.asb_predecessor, pg_temp.asb_path_count, pg_temp.asb_output';
  end if;
end;
$$
  language plpgsql
  volatile;

-- Tool-only ASP diagnostic counters use a second versioned, session-local
-- workspace. The transaction-local invocation setting prevents pooled-session
-- reuse from attributing a later call to an earlier replay, while explicit
-- keys make multi-call statements and cleanup independently auditable.
create or replace function public.ensure_bidirectional_all_shortest_path_telemetry_workspace()
  returns void as
$$
declare
  expected_version constant int4 := 1;
  present_version int4;
begin
  if to_regclass('pg_temp.asb_telemetry_workspace_version') is not null then
    select version into present_version
    from pg_temp.asb_telemetry_workspace_version limit 1;
  end if;
  if to_regclass('pg_temp.asb_telemetry_workspace_version') is not null
     and present_version is distinct from expected_version then
    drop table if exists pg_temp.asb_telemetry_level;
    drop table if exists pg_temp.asb_telemetry_call;
    drop table if exists pg_temp.asb_telemetry_invocation;
    drop table if exists pg_temp.asb_telemetry_workspace_version;
  end if;
  if to_regclass('pg_temp.asb_telemetry_workspace_version') is null then
    create temporary table asb_telemetry_workspace_version
    (
      version int4 not null primary key
    ) on commit preserve rows;
    create temporary table asb_telemetry_invocation
    (
      invocation_id text not null primary key,
      schema_version int4 not null,
      scheduler text,
      state_limit int8,
      frontier_limit int8,
      predecessor_limit int8,
      enumeration_limit int8,
      output_bytes_limit int8,
      next_search_id int8 not null default 0,
      check (btrim(invocation_id) <> '')
    ) on commit preserve rows;
    create temporary table asb_telemetry_call
    (
      invocation_id text not null,
      search_id int8 not null,
      source_id int8 not null,
      target_id int8 not null,
      runtime_branch text not null default 'started',
      scheduler_actions int8 not null default 0,
      candidate_edges int8 not null default 0,
      distinct_new_nodes int8 not null default 0,
      seen_peak int8 not null default 0,
      frontier_peak int8 not null default 0,
      queue_peak int8 not null default 0,
      predecessor_peak int8 not null default 0,
      meeting_candidates int8 not null default 0,
      frozen_distance int4,
      witness_rows int8 not null default 0,
      same_depth_predecessor_additions int8 not null default 0,
      meeting_nodes int8 not null default 0,
      cut_depth int4,
      path_count_estimate int8 not null default 0,
      path_count_saturated bool not null default false,
      enumerated_candidates int8 not null default 0,
      duplicate_rejects int8 not null default 0,
      output_paths int8 not null default 0,
      output_edge_cells int8 not null default 0,
      output_bytes int8 not null default 0,
      overflowed bool not null default false,
      fallback_executed bool not null default false,
      primary key (invocation_id, search_id)
    ) on commit preserve rows;
    create temporary table asb_telemetry_level
    (
      invocation_id text not null,
      search_id int8 not null,
      action_index int8 not null,
      side text not null,
      action text not null,
      depth int4 not null,
      frontier_rows int8 not null,
      candidate_edges int8 not null,
      distinct_new_nodes int8 not null,
      seen_rows int8 not null,
      queue_rows int8 not null,
      predecessor_rows int8 not null,
      meeting_candidates int8 not null,
      primary key (invocation_id, search_id, action_index)
    ) on commit preserve rows;
    insert into asb_telemetry_workspace_version(version) values (expected_version);
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.begin_bidirectional_all_shortest_path_diagnostic_v1(invocation_id text)
  returns void as
$$
begin
  if invocation_id is null or btrim(invocation_id) = '' or length(invocation_id) > 256 then
    raise exception using errcode = '22023', message = 'bidirectional all-shortest-path diagnostic invocation ID must contain 1 to 256 characters';
  end if;
  perform public.ensure_bidirectional_all_shortest_path_telemetry_workspace();
  delete from pg_temp.asb_telemetry_level where asb_telemetry_level.invocation_id = begin_bidirectional_all_shortest_path_diagnostic_v1.invocation_id;
  delete from pg_temp.asb_telemetry_call where asb_telemetry_call.invocation_id = begin_bidirectional_all_shortest_path_diagnostic_v1.invocation_id;
  delete from pg_temp.asb_telemetry_invocation where asb_telemetry_invocation.invocation_id = begin_bidirectional_all_shortest_path_diagnostic_v1.invocation_id;
  insert into pg_temp.asb_telemetry_invocation(invocation_id, schema_version)
  values (invocation_id, 1);
  perform set_config('dawgs.asb_diagnostic_invocation_id', invocation_id, true);
end;
$$
  language plpgsql
  volatile;

create or replace function public.read_bidirectional_all_shortest_path_diagnostic_v1(target_invocation_id text)
  returns jsonb as
$$
declare
  result jsonb;
begin
  select jsonb_build_object(
         'schema_version', invocation.schema_version,
         'invocation_id', invocation.invocation_id,
         'scheduler', invocation.scheduler,
         'state_limit', invocation.state_limit,
         'frontier_limit', invocation.frontier_limit,
         'predecessor_limit', invocation.predecessor_limit,
         'enumeration_limit', invocation.enumeration_limit,
         'output_bytes_limit', invocation.output_bytes_limit,
         'search_calls', coalesce(call_totals.search_calls, 0),
         'runtime_branch', coalesce(call_totals.runtime_branch, 'missing'),
         'overflowed', coalesce(call_totals.overflowed, false),
         'fallback_executed', coalesce(call_totals.fallback_executed, false),
         'counters', jsonb_build_object(
           'scheduler_actions', coalesce(call_totals.scheduler_actions, 0),
           'candidate_edges', coalesce(call_totals.candidate_edges, 0),
           'distinct_new_nodes', coalesce(call_totals.distinct_new_nodes, 0),
           'seen_peak', coalesce(call_totals.seen_peak, 0),
           'frontier_peak', coalesce(call_totals.frontier_peak, 0),
           'queue_peak', coalesce(call_totals.queue_peak, 0),
           'predecessor_peak', coalesce(call_totals.predecessor_peak, 0),
           'meeting_candidates', coalesce(call_totals.meeting_candidates, 0),
           'frozen_distance', coalesce(call_totals.frozen_distance, -1),
           'witness_rows', coalesce(call_totals.witness_rows, 0),
           'same_depth_predecessor_additions', coalesce(call_totals.same_depth_predecessor_additions, 0),
           'meeting_nodes', coalesce(call_totals.meeting_nodes, 0),
           'cut_depth', coalesce(call_totals.cut_depth, -1),
           'path_count_estimate', coalesce(call_totals.path_count_estimate, 0),
           'path_count_saturated', coalesce(call_totals.path_count_saturated, false),
           'enumerated_candidates', coalesce(call_totals.enumerated_candidates, 0),
           'duplicate_rejects', coalesce(call_totals.duplicate_rejects, 0),
           'output_paths', coalesce(call_totals.output_paths, 0),
           'output_edge_cells', coalesce(call_totals.output_edge_cells, 0),
           'output_bytes', coalesce(call_totals.output_bytes, 0),
           'levels', coalesce(levels.rows, '[]'::jsonb)
         ),
         'calls', coalesce(calls.rows, '[]'::jsonb)
       ) into result
  from pg_temp.asb_telemetry_invocation invocation
  left join lateral (
    select count(*)::int8 as search_calls,
           case when count(distinct call.runtime_branch) = 1
                then min(call.runtime_branch) else 'mixed' end as runtime_branch,
           bool_or(call.overflowed) as overflowed,
           bool_or(call.fallback_executed) as fallback_executed,
           sum(call.scheduler_actions)::int8 as scheduler_actions,
           sum(call.candidate_edges)::int8 as candidate_edges,
           sum(call.distinct_new_nodes)::int8 as distinct_new_nodes,
           max(call.seen_peak)::int8 as seen_peak,
           max(call.frontier_peak)::int8 as frontier_peak,
           max(call.queue_peak)::int8 as queue_peak,
           max(call.predecessor_peak)::int8 as predecessor_peak,
           sum(call.meeting_candidates)::int8 as meeting_candidates,
           min(call.frozen_distance)::int4 as frozen_distance,
           sum(call.witness_rows)::int8 as witness_rows,
           sum(call.same_depth_predecessor_additions)::int8 as same_depth_predecessor_additions,
           sum(call.meeting_nodes)::int8 as meeting_nodes,
           min(call.cut_depth)::int4 as cut_depth,
           sum(call.path_count_estimate)::int8 as path_count_estimate,
           bool_or(call.path_count_saturated) as path_count_saturated,
           sum(call.enumerated_candidates)::int8 as enumerated_candidates,
           sum(call.duplicate_rejects)::int8 as duplicate_rejects,
           sum(call.output_paths)::int8 as output_paths,
           sum(call.output_edge_cells)::int8 as output_edge_cells,
           sum(call.output_bytes)::int8 as output_bytes
    from pg_temp.asb_telemetry_call call
    where call.invocation_id = invocation.invocation_id
  ) call_totals on true
  left join lateral (
    select jsonb_agg(jsonb_build_object(
             'search_id', level.search_id,
             'action_index', level.action_index,
             'side', level.side,
             'action', level.action,
             'depth', level.depth,
             'frontier_rows', level.frontier_rows,
             'candidate_edges', level.candidate_edges,
             'distinct_new_nodes', level.distinct_new_nodes,
             'seen_rows', level.seen_rows,
             'queue_rows', level.queue_rows,
             'predecessor_rows', level.predecessor_rows,
             'meeting_candidates', level.meeting_candidates
           ) order by level.search_id, level.action_index) as rows
    from pg_temp.asb_telemetry_level level
    where level.invocation_id = invocation.invocation_id
  ) levels on true
  left join lateral (
    select jsonb_agg(to_jsonb(call) - 'invocation_id' order by call.search_id) as rows
    from pg_temp.asb_telemetry_call call
    where call.invocation_id = invocation.invocation_id
  ) calls on true
  where invocation.invocation_id = target_invocation_id;
  return result;
end;
$$
  language plpgsql
  stable
  strict;

create or replace function public.clear_bidirectional_all_shortest_path_diagnostic_v1(target_invocation_id text)
  returns void as
$$
begin
  if to_regclass('pg_temp.asb_telemetry_invocation') is not null then
    delete from pg_temp.asb_telemetry_level where invocation_id = target_invocation_id;
    delete from pg_temp.asb_telemetry_call where invocation_id = target_invocation_id;
    delete from pg_temp.asb_telemetry_invocation where invocation_id = target_invocation_id;
  end if;
  if nullif(current_setting('dawgs.asb_diagnostic_invocation_id', true), '') = target_invocation_id then
    perform set_config('dawgs.asb_diagnostic_invocation_id', '', true);
  end if;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public._start_bidirectional_all_shortest_path_diagnostic_call_v1(
                                                         target_invocation_id text,
                                                         target_scheduler text,
                                                         target_state_limit int8,
                                                         target_frontier_limit int8,
                                                         target_predecessor_limit int8,
                                                         target_enumeration_limit int8,
                                                         target_output_bytes_limit int8,
                                                         target_source_id int8,
                                                         target_target_id int8)
  returns int8 as
$$
declare
  target_search_id int8;
begin
  if target_invocation_id is null then
    return null;
  end if;
  if to_regclass('pg_temp.asb_telemetry_invocation') is null then
    raise exception using errcode = '55000', message = 'bidirectional all-shortest-path diagnostic replay was not initialized on this session';
  end if;
  update pg_temp.asb_telemetry_invocation invocation
  set scheduler = coalesce(invocation.scheduler, target_scheduler),
      state_limit = coalesce(invocation.state_limit, target_state_limit),
      frontier_limit = coalesce(invocation.frontier_limit, target_frontier_limit),
      predecessor_limit = coalesce(invocation.predecessor_limit, target_predecessor_limit),
      enumeration_limit = coalesce(invocation.enumeration_limit, target_enumeration_limit),
      output_bytes_limit = coalesce(invocation.output_bytes_limit, target_output_bytes_limit),
      next_search_id = invocation.next_search_id + 1
  where invocation.invocation_id = target_invocation_id
    and (invocation.scheduler is null or invocation.scheduler = target_scheduler)
    and (invocation.state_limit is null or invocation.state_limit = target_state_limit)
    and (invocation.frontier_limit is null or invocation.frontier_limit = target_frontier_limit)
    and (invocation.predecessor_limit is null or invocation.predecessor_limit = target_predecessor_limit)
    and (invocation.enumeration_limit is null or invocation.enumeration_limit = target_enumeration_limit)
    and (invocation.output_bytes_limit is null or invocation.output_bytes_limit = target_output_bytes_limit)
  returning invocation.next_search_id into target_search_id;
  if target_search_id is null then
    raise exception using errcode = '55000', message = 'bidirectional all-shortest-path diagnostic invocation is missing or mixes scheduler/cap identities';
  end if;
  insert into pg_temp.asb_telemetry_call(invocation_id, search_id, source_id, target_id)
  values (target_invocation_id, target_search_id, target_source_id, target_target_id);
  return target_search_id;
end;
$$
  language plpgsql
  volatile;

create or replace function public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
                                                         target_invocation_id text,
                                                         target_search_id int8,
                                                         target_action_index int8,
                                                         target_side text,
                                                         target_action text,
                                                         target_depth int4,
                                                         target_frontier_rows int8,
                                                         target_candidate_edges int8,
                                                         target_distinct_new_nodes int8,
                                                         target_seen_rows int8,
                                                         target_queue_rows int8,
                                                         target_predecessor_rows int8,
                                                         target_meeting_candidates int8)
  returns void as
$$
begin
  insert into pg_temp.asb_telemetry_level(
    invocation_id, search_id, action_index, side, action, depth,
    frontier_rows, candidate_edges, distinct_new_nodes, seen_rows,
    queue_rows, predecessor_rows, meeting_candidates)
  values (
    target_invocation_id, target_search_id, target_action_index, target_side,
    target_action, target_depth, target_frontier_rows, target_candidate_edges,
    target_distinct_new_nodes, target_seen_rows, target_queue_rows,
    target_predecessor_rows, target_meeting_candidates);
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
                                                         target_invocation_id text,
                                                         target_search_id int8,
                                                         target_runtime_branch text,
                                                         target_scheduler_actions int8,
                                                         target_candidate_edges int8,
                                                         target_distinct_new_nodes int8,
                                                         target_seen_peak int8,
                                                         target_frontier_peak int8,
                                                         target_queue_peak int8,
                                                         target_predecessor_peak int8,
                                                         target_meeting_candidates int8,
                                                         target_frozen_distance int4,
                                                         target_witness_rows int8,
                                                         target_same_depth_predecessor_additions int8,
                                                         target_meeting_nodes int8,
                                                         target_cut_depth int4,
                                                         target_path_count_estimate int8,
                                                         target_path_count_saturated bool,
                                                         target_enumerated_candidates int8,
                                                         target_duplicate_rejects int8,
                                                         target_output_paths int8,
                                                         target_output_edge_cells int8,
                                                         target_output_bytes int8,
                                                         target_overflowed bool,
                                                         target_fallback_executed bool)
  returns void as
$$
begin
  update pg_temp.asb_telemetry_call call
  set runtime_branch = target_runtime_branch,
      scheduler_actions = target_scheduler_actions,
      candidate_edges = target_candidate_edges,
      distinct_new_nodes = target_distinct_new_nodes,
      seen_peak = target_seen_peak,
      frontier_peak = target_frontier_peak,
      queue_peak = target_queue_peak,
      predecessor_peak = target_predecessor_peak,
      meeting_candidates = target_meeting_candidates,
      frozen_distance = target_frozen_distance,
      witness_rows = target_witness_rows,
      same_depth_predecessor_additions = target_same_depth_predecessor_additions,
      meeting_nodes = target_meeting_nodes,
      cut_depth = target_cut_depth,
      path_count_estimate = target_path_count_estimate,
      path_count_saturated = target_path_count_saturated,
      enumerated_candidates = target_enumerated_candidates,
      duplicate_rejects = target_duplicate_rejects,
      output_paths = target_output_paths,
      output_edge_cells = target_output_edge_cells,
      output_bytes = target_output_bytes,
      overflowed = target_overflowed,
      fallback_executed = target_fallback_executed
  where call.invocation_id = target_invocation_id and call.search_id = target_search_id;
  if not found then
    raise exception using errcode = '55000', message = 'bidirectional all-shortest-path diagnostic call is missing';
  end if;
end;
$$
  language plpgsql
  volatile;

-- all_shortest_paths_bidirectional_compact_v1 is restricted to one validated,
-- distinct endpoint pair, minimum depth one, directed traversal, and maximum
-- depth 64. Within that envelope a minimum path cannot repeat a node, so two
-- minimum-node-depth predecessor DAGs preserve relationship-simple Cypher
-- semantics.
--
-- Queue-head depth is a lower bound on every not-yet-completed path from that
-- side. A minimum distance L is proven only when one side is exhausted or the
-- two queue-head depths sum to at least L. The kernel then completes one
-- canonical cut k=floor(L/2): all forward predecessor rows into depth k and
-- all backward successor rows into depth L-k must be complete. Every shortest
-- path crosses exactly one node at this cut and is therefore stitched once,
-- even when the two searches overlap at several depths.
--
-- Discovery nodes/frontier, relationship-distinct predecessors, enumerated
-- arrays, and materialized array bytes have independent cap+1 admissions.
-- Path counts are evaluated over the completed DAG with saturating arithmetic
-- before enumeration. No candidate row is returned until every gate passes.
-- Overflow clears asb_* and invokes exact ASP-A1 in the same top-level
-- statement. REPEATABLE READ or SERIALIZABLE is mandatory because VOLATILE
-- PL/pgSQL statements at READ COMMITTED do not share one statement snapshot.
create or replace function public.all_shortest_paths_bidirectional_compact_v1(
                                                         target_graph_id int4,
                                                         source_id int8,
                                                         target_id int8,
                                                         min_depth int4,
                                                         max_depth int4,
                                                         edge_kind_ids int2[],
                                                         inbound bool,
                                                         state_limit int8,
                                                         frontier_limit int8,
                                                         predecessor_limit int8,
                                                         enumeration_limit int8,
                                                         output_bytes_limit int8,
                                                         scheduler text)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
#variable_conflict use_column
declare
  chosen_side char(1);
  strict_side char(1) := 'f';
  forward_depth int4;
  backward_depth int4;
  forward_ready_depth int4;
  backward_ready_depth int4;
  forward_width int8;
  backward_width int8;
  forward_tail int8 := 0;
  backward_tail int8 := 0;
  seen_rows int8;
  active_rows int8;
  frontier_rows int8;
  predecessor_rows int8;
  candidate_node_rows int8;
  candidate_predecessor_rows int8;
  discovery_admission_limit int8;
  predecessor_admission_limit int8;
  candidate_meeting int8;
  candidate_distance int4;
  best_distance int4;
  cut_depth int4;
  count_depth int4;
  meeting_nodes int8;
  path_array_bytes int8;
  path_count_limit int8;
  path_count_sentinel int8;
  path_count_estimate int8;
  output_rows int8;
  output_bytes int8;
  emitted_count int8 := 0;
  overflowed bool := false;
  telemetry_invocation_id text := nullif(current_setting('dawgs.asb_diagnostic_invocation_id', true), '');
  telemetry_search_id int8;
  telemetry_action_index int8 := 0;
  telemetry_action_depth int4 := 0;
  telemetry_action_candidate_edges int8 := 0;
  telemetry_action_meetings int8 := 0;
  telemetry_scheduler_actions int8 := 0;
  telemetry_candidate_edges int8 := 0;
  telemetry_distinct_new_nodes int8 := 0;
  telemetry_seen_peak int8 := 0;
  telemetry_frontier_peak int8 := 0;
  telemetry_queue_peak int8 := 0;
  telemetry_predecessor_peak int8 := 0;
  telemetry_meeting_candidates int8 := 0;
  telemetry_same_depth_predecessors int8 := 0;
  telemetry_path_count_saturated bool := false;
  telemetry_enumerated_candidates int8 := 0;
  telemetry_duplicate_rejects int8 := 0;
begin
  if source_id is null or target_id is null or max_depth < 1 then
    return;
  end if;
  if scheduler <> 'strict_alternating_node' and scheduler <> 'smaller_current_level' then
    raise exception using errcode = '22023', message = 'unknown compact bidirectional all-shortest-path scheduler';
  end if;
  if min_depth <> 1 then
    raise exception using errcode = '22023', message = 'compact bidirectional all-shortest paths requires min_depth = 1';
  end if;
  if max_depth > 64 then
    raise exception using errcode = '22023', message = 'compact bidirectional all-shortest paths requires max_depth <= 64';
  end if;
  if state_limit <= 0 or frontier_limit <= 0 or predecessor_limit <= 0
     or enumeration_limit <= 0 or output_bytes_limit <= 0
     or enumeration_limit = 9223372036854775807
     or output_bytes_limit = 9223372036854775807 then
    raise exception using errcode = '22023', message = 'compact bidirectional all-shortest paths requires positive bounded limits below int8 maximum';
  end if;
  if current_setting('transaction_isolation') <> 'repeatable read'
     and current_setting('transaction_isolation') <> 'serializable' then
    raise exception using
      errcode = '25001',
      message = 'compact bidirectional all-shortest paths requires REPEATABLE READ or SERIALIZABLE transaction isolation';
  end if;
  if source_id = target_id then
    perform public.shortest_path_self_endpoint_error(source_id, target_id);
  end if;

  telemetry_search_id = public._start_bidirectional_all_shortest_path_diagnostic_call_v1(
    telemetry_invocation_id, scheduler, state_limit, frontier_limit,
    predecessor_limit, enumeration_limit, output_bytes_limit,
    source_id, target_id);
  -- This non-allocating clear prevents successful shallow preflights, no-path
  -- returns, and exact fallback from inheriting an earlier invocation's state.
  perform public.clear_bidirectional_all_shortest_path_workspace();

  -- Exact depth-one preflight remains outside the candidate workspace. It
  -- returns every relationship-distinct edge only when enumeration and bytes
  -- gates admit the complete multiset.
  path_array_bytes = pg_column_size(array_fill(0::int8, array[1]));
  if path_array_bytes <= 126 then
    path_array_bytes = path_array_bytes - 3;
  end if;
  path_count_limit = least(enumeration_limit, output_bytes_limit / path_array_bytes);
  select count(*) into output_rows
  from (
    select 1
    from edge e
    where e.graph_id = target_graph_id
      and ((not inbound and e.start_id = source_id and e.end_id = target_id)
        or (inbound and e.end_id = source_id and e.start_id = target_id))
      and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
    limit path_count_limit + 1
  ) shallow;
  if output_rows > 0 then
    if telemetry_search_id is not null then
      telemetry_action_index = telemetry_action_index + 1;
      perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
        telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
        'none', 'preflight_one_hop', 1, 0, output_rows, 0, 0, 0, 0,
        output_rows);
    end if;
    if output_rows > path_count_limit then
      return query
        select fallback.root_id, fallback.next_id, fallback.depth,
               fallback.satisfied, fallback.is_cycle, fallback.path
        from public.all_shortest_paths_dag(target_graph_id, source_id, target_id,
                                           min_depth, max_depth, edge_kind_ids,
                                           inbound) fallback;
      get diagnostics emitted_count = row_count;
      if telemetry_search_id is not null then
        perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
          telemetry_invocation_id, telemetry_search_id, 'exact_a1_fallback',
          0, output_rows, 0, 0, 0, 0, 0, output_rows, 1, emitted_count,
          0, 1, 0, output_rows, true, output_rows, 0,
          emitted_count, emitted_count, emitted_count * path_array_bytes,
          true, true);
      end if;
      perform public.record_requested_traversal_runtime_attestation_v1('exact_a1_fallback', true, 'ASP-A1-DAG');
      return;
    end if;
    if not inbound then
      return query
        select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
        from edge e
        where e.graph_id = target_graph_id
          and e.start_id = source_id and e.end_id = target_id
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        order by e.id;
    else
      return query
        select source_id, target_id, 1::int4, true, false, array[e.id]::int8[]
        from edge e
        where e.graph_id = target_graph_id
          and e.end_id = source_id and e.start_id = target_id
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
        order by e.id;
    end if;
    get diagnostics emitted_count = row_count;
    if telemetry_search_id is not null then
      perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'preflight_one_hop',
        0, output_rows, 0, 0, 0, 0, 0, output_rows, 1, emitted_count,
        0, 1, 0, output_rows, false, output_rows, 0,
        emitted_count, emitted_count, emitted_count * path_array_bytes,
        false, false);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('preflight_one_hop', false, 'ASP-A1-DAG');
    return;
  end if;

  -- Exact depth-two preflight similarly stages only a cap+1 scalar count. The
  -- full relationship pair multiset is emitted only after both output gates.
  if max_depth >= 2 then
    path_array_bytes = pg_column_size(array_fill(0::int8, array[2]));
    if path_array_bytes <= 126 then
      path_array_bytes = path_array_bytes - 3;
    end if;
    path_count_limit = least(enumeration_limit, output_bytes_limit / path_array_bytes);
    if not inbound then
      select count(*) into output_rows
      from (
        select 1
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.start_id = e1.end_id
        where e1.graph_id = target_graph_id
          and e1.start_id = source_id and e2.end_id = target_id and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        limit path_count_limit + 1
      ) shallow;
    else
      select count(*) into output_rows
      from (
        select 1
        from edge e1
        join edge e2 on e2.graph_id = target_graph_id and e2.end_id = e1.start_id
        where e1.graph_id = target_graph_id
          and e1.end_id = source_id and e2.start_id = target_id and e1.id <> e2.id
          and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
          and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
        limit path_count_limit + 1
      ) shallow;
    end if;
    if output_rows > 0 then
      if telemetry_search_id is not null then
        telemetry_action_index = telemetry_action_index + 1;
        perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          'none', 'preflight_two_hop', 2, 0, output_rows * 2, 0, 0, 0, 0,
          output_rows);
      end if;
      if output_rows > path_count_limit then
        return query
          select fallback.root_id, fallback.next_id, fallback.depth,
                 fallback.satisfied, fallback.is_cycle, fallback.path
          from public.all_shortest_paths_dag(target_graph_id, source_id, target_id,
                                             min_depth, max_depth, edge_kind_ids,
                                             inbound) fallback;
        get diagnostics emitted_count = row_count;
        if telemetry_search_id is not null then
          perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
            telemetry_invocation_id, telemetry_search_id, 'exact_a1_fallback',
            0, output_rows * 2, 0, 0, 0, 0, 0, output_rows, 2, emitted_count,
            0, 1, 1, output_rows, true, output_rows, 0,
            emitted_count, emitted_count * 2, emitted_count * path_array_bytes,
            true, true);
        end if;
        perform public.record_requested_traversal_runtime_attestation_v1('exact_a1_fallback', true, 'ASP-A1-DAG');
        return;
      end if;
      if not inbound then
        return query
          select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
          from edge e1
          join edge e2 on e2.graph_id = target_graph_id and e2.start_id = e1.end_id
          where e1.graph_id = target_graph_id
            and e1.start_id = source_id and e2.end_id = target_id and e1.id <> e2.id
            and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
            and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
          order by e1.id, e2.id;
      else
        return query
          select source_id, target_id, 2::int4, true, false, array[e1.id, e2.id]::int8[]
          from edge e1
          join edge e2 on e2.graph_id = target_graph_id and e2.end_id = e1.start_id
          where e1.graph_id = target_graph_id
            and e1.end_id = source_id and e2.start_id = target_id and e1.id <> e2.id
            and (cardinality(edge_kind_ids) = 0 or e1.kind_id = any(edge_kind_ids))
            and (cardinality(edge_kind_ids) = 0 or e2.kind_id = any(edge_kind_ids))
          order by e1.id, e2.id;
      end if;
      get diagnostics emitted_count = row_count;
      if telemetry_search_id is not null then
        perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
          telemetry_invocation_id, telemetry_search_id, 'preflight_two_hop',
          0, output_rows * 2, 0, 0, 0, 0, 0, output_rows, 2, emitted_count,
          0, 1, 1, output_rows, false, output_rows, 0,
          emitted_count, emitted_count * 2, emitted_count * path_array_bytes,
          false, false);
      end if;
      perform public.record_requested_traversal_runtime_attestation_v1('preflight_two_hop', false, 'ASP-A1-DAG');
      return;
    end if;
  end if;
  if max_depth <= 2 then
    if telemetry_search_id is not null then
      telemetry_action_index = telemetry_action_index + 1;
      perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
        telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
        'none', 'preflight_no_path', max_depth, 0, 0, 0, 0, 0, 0, 0);
      perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'preflight_no_path',
        0, 0, 0, 0, 0, 0, 0, 0, null, 0,
        0, 0, null, 0, false, 0, 0, 0, 0, 0, false, false);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('preflight_no_path', false, 'ASP-A1-DAG');
    return;
  end if;

  -- The two roots are discovery/frontier state, but not predecessor state.
  if state_limit < 2 or frontier_limit < 2 then
    overflowed = true;
    telemetry_frontier_peak = 2;
    telemetry_queue_peak = 2;
    if telemetry_search_id is not null then
      telemetry_action_index = telemetry_action_index + 1;
      perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
        telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
        'none', 'root_admission', 0, 2, 0, 0, 2, 2, 0, 0);
    end if;
  else
    perform public.reset_bidirectional_all_shortest_path_workspace();
    insert into pg_temp.asb_front(side, node_id, depth, queue_order)
    values ('f', source_id, 0, 0), ('b', target_id, 0, 0);
    insert into pg_temp.asb_seen(side, node_id, depth)
    values ('f', source_id, 0), ('b', target_id, 0);
    telemetry_seen_peak = 2;
    telemetry_frontier_peak = 2;
    telemetry_queue_peak = 2;
  end if;

  while not overflowed loop
    select min(depth) into forward_depth from pg_temp.asb_front where side = 'f';
    select min(depth) into backward_depth from pg_temp.asb_front where side = 'b';
    select count(*) into forward_width from pg_temp.asb_front where side = 'f' and depth = forward_depth;
    select count(*) into backward_width from pg_temp.asb_front where side = 'b' and depth = backward_depth;
    select coalesce(forward_depth, max(depth), 0) into forward_ready_depth
      from pg_temp.asb_seen where side = 'f';
    select coalesce(backward_depth, max(depth), 0) into backward_ready_depth
      from pg_temp.asb_seen where side = 'b';

    if best_distance is null and (forward_depth is null or backward_depth is null) then
      exit;
    end if;

    if best_distance is not null
       and (forward_depth is null or backward_depth is null
            or forward_depth + backward_depth >= best_distance) then
      cut_depth = best_distance / 2;
      if forward_ready_depth >= cut_depth
         and backward_ready_depth >= best_distance - cut_depth then
        exit;
      elsif forward_ready_depth < cut_depth then
        chosen_side = 'f';
      else
        chosen_side = 'b';
      end if;
    elsif scheduler = 'strict_alternating_node' then
      chosen_side = strict_side;
      if (chosen_side = 'f' and forward_depth is null)
         or (chosen_side = 'b' and backward_depth is null) then
        chosen_side = case chosen_side when 'f' then 'b' else 'f' end;
      end if;
      strict_side = case chosen_side when 'f' then 'b' else 'f' end;
    else
      if forward_depth is null then
        chosen_side = 'b';
      elsif backward_depth is null then
        chosen_side = 'f';
      else
        -- Stable equality tie break: forward.
        chosen_side = case when forward_width <= backward_width then 'f' else 'b' end;
      end if;
    end if;

    truncate table pg_temp.asb_active, pg_temp.asb_candidate_node,
                   pg_temp.asb_candidate_predecessor;
    if scheduler = 'strict_alternating_node'
       and not (best_distance is not null
                and (forward_depth is null or backward_depth is null
                     or forward_depth + backward_depth >= best_distance)) then
      insert into pg_temp.asb_active(side, node_id, depth)
      select side, node_id, depth
      from pg_temp.asb_front
      where side = chosen_side
      order by queue_order
      limit 1;
    elsif scheduler = 'strict_alternating_node' then
      -- Cut completion retains node granularity while allowing the incomplete
      -- side to advance consecutively after minimum distance is proven.
      insert into pg_temp.asb_active(side, node_id, depth)
      select side, node_id, depth
      from pg_temp.asb_front
      where side = chosen_side
      order by queue_order
      limit 1;
    else
      insert into pg_temp.asb_active(side, node_id, depth)
      select side, node_id, depth
      from pg_temp.asb_front
      where side = chosen_side
        and depth = case chosen_side when 'f' then forward_depth else backward_depth end
      order by queue_order;
    end if;

    delete from pg_temp.asb_front front
    using pg_temp.asb_active active
    where front.side = active.side and front.node_id = active.node_id;

    telemetry_scheduler_actions = telemetry_scheduler_actions + 1;
    telemetry_action_candidate_edges = 0;
    telemetry_action_meetings = 0;
    select min(depth) into telemetry_action_depth from pg_temp.asb_active;
	if telemetry_search_id is not null then
	  if (chosen_side = 'f' and not inbound) or (chosen_side = 'b' and inbound) then
		select count(*) into telemetry_action_candidate_edges
		from pg_temp.asb_active active
		join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
		where active.depth < max_depth
		  and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids));
	  else
		select count(*) into telemetry_action_candidate_edges
		from pg_temp.asb_active active
		join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
		where active.depth < max_depth
		  and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids));
	  end if;
	end if;

    if not exists (select 1 from pg_temp.asb_active where depth < max_depth) then
      if telemetry_search_id is not null then
        select count(*) into seen_rows from pg_temp.asb_seen;
        select count(*) into active_rows from pg_temp.asb_active;
        select count(*) into frontier_rows from pg_temp.asb_front;
        select count(*) into predecessor_rows from pg_temp.asb_predecessor;
        telemetry_action_index = telemetry_action_index + 1;
        telemetry_seen_peak = greatest(telemetry_seen_peak, seen_rows);
        telemetry_frontier_peak = greatest(telemetry_frontier_peak, active_rows + frontier_rows);
        telemetry_queue_peak = greatest(telemetry_queue_peak, frontier_rows);
        telemetry_predecessor_peak = greatest(telemetry_predecessor_peak, predecessor_rows);
        perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          chosen_side::text,
          case scheduler when 'strict_alternating_node' then 'dequeue_node' else 'expand_level' end,
          telemetry_action_depth, active_rows + frontier_rows, 0, 0,
          seen_rows, frontier_rows, predecessor_rows, 0);
      end if;
      continue;
    end if;

    select count(*) into seen_rows from pg_temp.asb_seen;
    select count(*) into active_rows from pg_temp.asb_active;
    select count(*) into frontier_rows from pg_temp.asb_front;
    select count(*) into predecessor_rows from pg_temp.asb_predecessor;
    discovery_admission_limit = least(state_limit - seen_rows,
                                      frontier_limit - active_rows - frontier_rows);
    if discovery_admission_limit < 0 then
      overflowed = true;
      if telemetry_search_id is not null then
        telemetry_action_index = telemetry_action_index + 1;
        telemetry_seen_peak = greatest(telemetry_seen_peak, seen_rows);
        telemetry_frontier_peak = greatest(telemetry_frontier_peak, active_rows + frontier_rows);
        telemetry_queue_peak = greatest(telemetry_queue_peak, frontier_rows);
        telemetry_predecessor_peak = greatest(telemetry_predecessor_peak, predecessor_rows);
        perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          chosen_side::text,
          case scheduler when 'strict_alternating_node' then 'dequeue_node' else 'expand_level' end,
          telemetry_action_depth, active_rows + frontier_rows, 0, 0,
          seen_rows, frontier_rows, predecessor_rows, 0);
      end if;
      exit;
    end if;

    -- First admit distinct unseen nodes with a discovery cap+1 sentinel.
    if chosen_side = 'f' and not inbound then
      insert into pg_temp.asb_candidate_node(side, node_id, depth)
      select 'f', candidate.node_id, candidate.depth
      from (
        select distinct e.end_id as node_id, active.depth + 1 as depth
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.asb_seen seen where seen.side = 'f' and seen.node_id = e.end_id)
        order by e.end_id
        limit discovery_admission_limit + 1
      ) candidate;
    elsif chosen_side = 'f' and inbound then
      insert into pg_temp.asb_candidate_node(side, node_id, depth)
      select 'f', candidate.node_id, candidate.depth
      from (
        select distinct e.start_id as node_id, active.depth + 1 as depth
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.asb_seen seen where seen.side = 'f' and seen.node_id = e.start_id)
        order by e.start_id
        limit discovery_admission_limit + 1
      ) candidate;
    elsif chosen_side = 'b' and not inbound then
      insert into pg_temp.asb_candidate_node(side, node_id, depth)
      select 'b', candidate.node_id, candidate.depth
      from (
        select distinct e.start_id as node_id, active.depth + 1 as depth
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.asb_seen seen where seen.side = 'b' and seen.node_id = e.start_id)
        order by e.start_id
        limit discovery_admission_limit + 1
      ) candidate;
    else
      insert into pg_temp.asb_candidate_node(side, node_id, depth)
      select 'b', candidate.node_id, candidate.depth
      from (
        select distinct e.end_id as node_id, active.depth + 1 as depth
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and not exists (select 1 from pg_temp.asb_seen seen where seen.side = 'b' and seen.node_id = e.end_id)
        order by e.end_id
        limit discovery_admission_limit + 1
      ) candidate;
    end if;

    select count(*) into candidate_node_rows from pg_temp.asb_candidate_node;
    if seen_rows + candidate_node_rows > state_limit
       or active_rows + frontier_rows + candidate_node_rows > frontier_limit then
      overflowed = true;
      if telemetry_search_id is not null then
        select count(*) into telemetry_action_meetings
        from pg_temp.asb_candidate_node candidate
        join pg_temp.asb_seen opposite
          on opposite.node_id = candidate.node_id and opposite.side <> candidate.side
        where candidate.depth + opposite.depth between min_depth and max_depth;
        telemetry_candidate_edges = telemetry_candidate_edges + telemetry_action_candidate_edges;
        telemetry_distinct_new_nodes = telemetry_distinct_new_nodes + candidate_node_rows;
        telemetry_meeting_candidates = telemetry_meeting_candidates + telemetry_action_meetings;
        telemetry_seen_peak = greatest(telemetry_seen_peak, seen_rows + candidate_node_rows);
        telemetry_frontier_peak = greatest(telemetry_frontier_peak, active_rows + frontier_rows + candidate_node_rows);
        telemetry_queue_peak = greatest(telemetry_queue_peak, frontier_rows + candidate_node_rows);
        telemetry_action_index = telemetry_action_index + 1;
        perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
          telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
          chosen_side::text,
          case scheduler when 'strict_alternating_node' then 'dequeue_node' else 'expand_level' end,
          telemetry_action_depth, active_rows + frontier_rows + candidate_node_rows,
          telemetry_action_candidate_edges, candidate_node_rows,
          seen_rows + candidate_node_rows, frontier_rows + candidate_node_rows,
          predecessor_rows, telemetry_action_meetings);
      end if;
      exit;
    end if;

    predecessor_admission_limit = predecessor_limit - predecessor_rows;
    if predecessor_admission_limit < 0 then
      overflowed = true;
      exit;
    end if;

    -- Then retain every relationship-distinct edge into a newly discovered or
    -- already-seen node at the same minimum depth. This second admission is
    -- independent of distinct-node discovery.
    if chosen_side = 'f' and not inbound then
      insert into pg_temp.asb_candidate_predecessor(side, node_id, depth, adjacent_id, edge_id)
      select 'f', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select e.end_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        left join pg_temp.asb_seen seen on seen.side = 'f' and seen.node_id = e.end_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and (seen.node_id is null or seen.depth = active.depth + 1)
          and (seen.node_id is not null or exists (
                 select 1 from pg_temp.asb_candidate_node admitted
                 where admitted.side = 'f' and admitted.node_id = e.end_id))
        order by e.end_id, e.id, active.node_id
        limit predecessor_admission_limit + 1
      ) candidate;
    elsif chosen_side = 'f' and inbound then
      insert into pg_temp.asb_candidate_predecessor(side, node_id, depth, adjacent_id, edge_id)
      select 'f', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select e.start_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        left join pg_temp.asb_seen seen on seen.side = 'f' and seen.node_id = e.start_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and (seen.node_id is null or seen.depth = active.depth + 1)
          and (seen.node_id is not null or exists (
                 select 1 from pg_temp.asb_candidate_node admitted
                 where admitted.side = 'f' and admitted.node_id = e.start_id))
        order by e.start_id, e.id, active.node_id
        limit predecessor_admission_limit + 1
      ) candidate;
    elsif chosen_side = 'b' and not inbound then
      insert into pg_temp.asb_candidate_predecessor(side, node_id, depth, adjacent_id, edge_id)
      select 'b', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select e.start_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.end_id = active.node_id
        left join pg_temp.asb_seen seen on seen.side = 'b' and seen.node_id = e.start_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and (seen.node_id is null or seen.depth = active.depth + 1)
          and (seen.node_id is not null or exists (
                 select 1 from pg_temp.asb_candidate_node admitted
                 where admitted.side = 'b' and admitted.node_id = e.start_id))
        order by e.start_id, e.id, active.node_id
        limit predecessor_admission_limit + 1
      ) candidate;
    else
      insert into pg_temp.asb_candidate_predecessor(side, node_id, depth, adjacent_id, edge_id)
      select 'b', candidate.node_id, candidate.depth, candidate.adjacent_id, candidate.edge_id
      from (
        select e.end_id as node_id, active.depth + 1 as depth,
               active.node_id as adjacent_id, e.id as edge_id
        from pg_temp.asb_active active
        join edge e on e.graph_id = target_graph_id and e.start_id = active.node_id
        left join pg_temp.asb_seen seen on seen.side = 'b' and seen.node_id = e.end_id
        where active.depth < max_depth
          and (cardinality(edge_kind_ids) = 0 or e.kind_id = any(edge_kind_ids))
          and (seen.node_id is null or seen.depth = active.depth + 1)
          and (seen.node_id is not null or exists (
                 select 1 from pg_temp.asb_candidate_node admitted
                 where admitted.side = 'b' and admitted.node_id = e.end_id))
        order by e.end_id, e.id, active.node_id
        limit predecessor_admission_limit + 1
      ) candidate;
    end if;

    select count(*) into candidate_predecessor_rows
      from pg_temp.asb_candidate_predecessor;
    if telemetry_search_id is not null then
      select count(*) into telemetry_action_meetings
      from pg_temp.asb_candidate_node candidate
      join pg_temp.asb_seen opposite
        on opposite.node_id = candidate.node_id and opposite.side <> candidate.side
      where candidate.depth + opposite.depth between min_depth and max_depth;
      telemetry_candidate_edges = telemetry_candidate_edges + telemetry_action_candidate_edges;
      telemetry_distinct_new_nodes = telemetry_distinct_new_nodes + candidate_node_rows;
      telemetry_meeting_candidates = telemetry_meeting_candidates + telemetry_action_meetings;
      telemetry_same_depth_predecessors = telemetry_same_depth_predecessors
        + greatest(candidate_predecessor_rows - candidate_node_rows, 0);
      telemetry_seen_peak = greatest(telemetry_seen_peak, seen_rows + candidate_node_rows);
      telemetry_frontier_peak = greatest(telemetry_frontier_peak, active_rows + frontier_rows + candidate_node_rows);
      telemetry_queue_peak = greatest(telemetry_queue_peak, frontier_rows + candidate_node_rows);
      telemetry_predecessor_peak = greatest(telemetry_predecessor_peak, predecessor_rows + candidate_predecessor_rows);
      telemetry_action_index = telemetry_action_index + 1;
      perform public._record_bidirectional_all_shortest_path_diagnostic_level_v1(
        telemetry_invocation_id, telemetry_search_id, telemetry_action_index,
        chosen_side::text,
        case scheduler when 'strict_alternating_node' then 'dequeue_node' else 'expand_level' end,
        telemetry_action_depth, active_rows + frontier_rows + candidate_node_rows,
        telemetry_action_candidate_edges, candidate_node_rows,
        seen_rows + candidate_node_rows, frontier_rows + candidate_node_rows,
        predecessor_rows + candidate_predecessor_rows,
        telemetry_action_meetings);
    end if;
    if predecessor_rows + candidate_predecessor_rows > predecessor_limit then
      overflowed = true;
      exit;
    end if;

    insert into pg_temp.asb_predecessor(side, node_id, depth, adjacent_id, edge_id)
    select side, node_id, depth, adjacent_id, edge_id
    from pg_temp.asb_candidate_predecessor
    order by side, node_id, edge_id, adjacent_id
    on conflict do nothing;
    insert into pg_temp.asb_seen(side, node_id, depth)
    select side, node_id, depth
    from pg_temp.asb_candidate_node
    order by side, node_id
    on conflict do nothing;

    if chosen_side = 'f' then
      insert into pg_temp.asb_front(side, node_id, depth, queue_order)
      select side, node_id, depth,
             forward_tail + row_number() over (order by node_id)
      from pg_temp.asb_candidate_node;
      forward_tail = forward_tail + candidate_node_rows;
    else
      insert into pg_temp.asb_front(side, node_id, depth, queue_order)
      select side, node_id, depth,
             backward_tail + row_number() over (order by node_id)
      from pg_temp.asb_candidate_node;
      backward_tail = backward_tail + candidate_node_rows;
    end if;

    candidate_meeting = null;
    candidate_distance = null;
    select candidate.node_id, candidate.depth + opposite.depth
      into candidate_meeting, candidate_distance
      from pg_temp.asb_candidate_node candidate
      join pg_temp.asb_seen opposite
        on opposite.node_id = candidate.node_id and opposite.side <> candidate.side
      where candidate.depth + opposite.depth between min_depth and max_depth
      order by candidate.depth + opposite.depth, candidate.node_id
      limit 1;
    if candidate_distance is not null
       and (best_distance is null or candidate_distance < best_distance) then
      best_distance = candidate_distance;
    end if;
  end loop;

  if overflowed then
    perform public.clear_bidirectional_all_shortest_path_workspace();
    return query
      select fallback.root_id, fallback.next_id, fallback.depth,
             fallback.satisfied, fallback.is_cycle, fallback.path
      from public.all_shortest_paths_dag(target_graph_id, source_id, target_id,
                                         min_depth, max_depth, edge_kind_ids,
                                         inbound) fallback;
    get diagnostics emitted_count = row_count;
    if telemetry_search_id is not null then
      perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'exact_a1_fallback',
        telemetry_scheduler_actions, telemetry_candidate_edges,
        telemetry_distinct_new_nodes, telemetry_seen_peak,
        telemetry_frontier_peak, telemetry_queue_peak,
        telemetry_predecessor_peak, telemetry_meeting_candidates,
        best_distance, emitted_count, telemetry_same_depth_predecessors,
        coalesce(meeting_nodes, 0), cut_depth, coalesce(path_count_estimate, 0),
        telemetry_path_count_saturated, telemetry_enumerated_candidates,
        telemetry_duplicate_rejects, emitted_count,
        emitted_count * coalesce(best_distance, 0), coalesce(output_bytes, 0),
        true, true);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('exact_a1_fallback', true, 'ASP-A1-DAG');
    return;
  end if;
  if best_distance is null then
    perform public.clear_bidirectional_all_shortest_path_workspace();
    if telemetry_search_id is not null then
      perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'search_no_path',
        telemetry_scheduler_actions, telemetry_candidate_edges,
        telemetry_distinct_new_nodes, telemetry_seen_peak,
        telemetry_frontier_peak, telemetry_queue_peak,
        telemetry_predecessor_peak, telemetry_meeting_candidates,
        null, 0, telemetry_same_depth_predecessors, 0, null, 0, false,
        0, 0, 0, 0, 0, false, false);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('search_no_path', false, 'ASP-A1-DAG');
    return;
  end if;

  cut_depth = best_distance / 2;
  select count(*) into meeting_nodes
  from pg_temp.asb_seen forward_seen
  join pg_temp.asb_seen backward_seen
    on backward_seen.node_id = forward_seen.node_id and backward_seen.side = 'b'
  where forward_seen.side = 'f' and forward_seen.depth = cut_depth
    and backward_seen.depth = best_distance - cut_depth;
  if meeting_nodes = 0 then
    overflowed = true;
  end if;

  -- Saturating dynamic programming over each half-DAG bounds enumeration and
  -- bytes before any edge array is materialized.
  if not overflowed then
    path_array_bytes = pg_column_size(array_fill(0::int8, array[best_distance]));
    if path_array_bytes <= 126 then
      path_array_bytes = path_array_bytes - 3;
    end if;
    path_count_limit = least(enumeration_limit, output_bytes_limit / path_array_bytes);
    path_count_sentinel = path_count_limit + 1;
    truncate table pg_temp.asb_path_count, pg_temp.asb_output;
    insert into pg_temp.asb_path_count(side, node_id, depth, path_count)
    values ('f', source_id, 0, 1), ('b', target_id, 0, 1);

    for count_depth in 1..cut_depth loop
      insert into pg_temp.asb_path_count(side, node_id, depth, path_count)
      select 'f', predecessor.node_id, count_depth,
             least(path_count_sentinel::numeric,
                   sum(adjacent.path_count::numeric))::int8
      from pg_temp.asb_predecessor predecessor
      join pg_temp.asb_path_count adjacent
        on adjacent.side = 'f' and adjacent.node_id = predecessor.adjacent_id
       and adjacent.depth = count_depth - 1
      where predecessor.side = 'f' and predecessor.depth = count_depth
      group by predecessor.node_id;
    end loop;
    for count_depth in 1..(best_distance - cut_depth) loop
      insert into pg_temp.asb_path_count(side, node_id, depth, path_count)
      select 'b', predecessor.node_id, count_depth,
             least(path_count_sentinel::numeric,
                   sum(adjacent.path_count::numeric))::int8
      from pg_temp.asb_predecessor predecessor
      join pg_temp.asb_path_count adjacent
        on adjacent.side = 'b' and adjacent.node_id = predecessor.adjacent_id
       and adjacent.depth = count_depth - 1
      where predecessor.side = 'b' and predecessor.depth = count_depth
      group by predecessor.node_id;
    end loop;

    select least(path_count_sentinel::numeric,
                 coalesce(sum(least(path_count_sentinel::numeric,
                                    forward_count.path_count::numeric
                                    * backward_count.path_count::numeric)), 0))::int8
      into path_count_estimate
      from pg_temp.asb_path_count forward_count
      join pg_temp.asb_path_count backward_count
        on backward_count.side = 'b' and backward_count.node_id = forward_count.node_id
       and backward_count.depth = best_distance - cut_depth
      where forward_count.side = 'f' and forward_count.depth = cut_depth;
    telemetry_path_count_saturated = path_count_estimate >= path_count_sentinel;
    if path_count_estimate > path_count_limit or path_count_estimate = 0 then
      overflowed = true;
    end if;
  end if;

  if not overflowed then
    insert into pg_temp.asb_output(edge_ids, output_bytes)
    with recursive
    meeting(node_id) as materialized (
      select forward_seen.node_id
      from pg_temp.asb_seen forward_seen
      join pg_temp.asb_seen backward_seen
        on backward_seen.node_id = forward_seen.node_id and backward_seen.side = 'b'
      where forward_seen.side = 'f' and forward_seen.depth = cut_depth
        and backward_seen.depth = best_distance - cut_depth
    ),
    forward_paths(meeting_id, node_id, path_depth, edge_ids) as (
      select meeting.node_id, meeting.node_id, cut_depth, array []::int8[]
      from meeting
      union all
      select forward_paths.meeting_id, predecessor.adjacent_id,
             forward_paths.path_depth - 1,
             array[predecessor.edge_id]::int8[] || forward_paths.edge_ids
      from forward_paths
      join pg_temp.asb_predecessor predecessor
        on predecessor.side = 'f' and predecessor.node_id = forward_paths.node_id
       and predecessor.depth = forward_paths.path_depth
    ),
    backward_paths(meeting_id, node_id, path_depth, edge_ids) as (
      select meeting.node_id, meeting.node_id, best_distance - cut_depth,
             array []::int8[]
      from meeting
      union all
      select backward_paths.meeting_id, successor.adjacent_id,
             backward_paths.path_depth - 1,
             backward_paths.edge_ids || successor.edge_id
      from backward_paths
      join pg_temp.asb_predecessor successor
        on successor.side = 'b' and successor.node_id = backward_paths.node_id
       and successor.depth = backward_paths.path_depth
    ),
    stitched(edge_ids) as (
      select forward_paths.edge_ids || backward_paths.edge_ids
      from forward_paths
      join backward_paths using (meeting_id)
      where forward_paths.node_id = source_id and forward_paths.path_depth = 0
        and backward_paths.node_id = target_id and backward_paths.path_depth = 0
    )
    select staged.edge_ids, pg_column_size(staged.edge_ids)::int8
    from (
      select distinct stitched.edge_ids
      from stitched
      where cardinality(stitched.edge_ids) = best_distance
        and cardinality(stitched.edge_ids) = (
          select count(distinct path_edge.edge_id)
          from unnest(stitched.edge_ids) path_edge(edge_id))
      order by stitched.edge_ids
      limit enumeration_limit + 1
    ) staged;

    select count(*), coalesce(sum(asb_output.output_bytes), 0)
      into output_rows, output_bytes
      from pg_temp.asb_output;
    telemetry_enumerated_candidates = output_rows;
	telemetry_duplicate_rejects = greatest(coalesce(path_count_estimate, 0) - output_rows, 0);
    if output_rows > enumeration_limit or output_bytes > output_bytes_limit
       or output_rows <> path_count_estimate then
      overflowed = true;
    end if;
  end if;

  if overflowed then
    perform public.clear_bidirectional_all_shortest_path_workspace();
    return query
      select fallback.root_id, fallback.next_id, fallback.depth,
             fallback.satisfied, fallback.is_cycle, fallback.path
      from public.all_shortest_paths_dag(target_graph_id, source_id, target_id,
                                         min_depth, max_depth, edge_kind_ids,
                                         inbound) fallback;
    get diagnostics emitted_count = row_count;
    if telemetry_search_id is not null then
      perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
        telemetry_invocation_id, telemetry_search_id, 'exact_a1_fallback',
        telemetry_scheduler_actions, telemetry_candidate_edges,
        telemetry_distinct_new_nodes, telemetry_seen_peak,
        telemetry_frontier_peak, telemetry_queue_peak,
        telemetry_predecessor_peak, telemetry_meeting_candidates,
        best_distance, emitted_count, telemetry_same_depth_predecessors,
        coalesce(meeting_nodes, 0), cut_depth, coalesce(path_count_estimate, 0),
        telemetry_path_count_saturated, telemetry_enumerated_candidates,
        telemetry_duplicate_rejects, emitted_count,
        emitted_count * coalesce(best_distance, 0), coalesce(output_bytes, 0),
        true, true);
    end if;
    perform public.record_requested_traversal_runtime_attestation_v1('exact_a1_fallback', true, 'ASP-A1-DAG');
    return;
  end if;

  return query
    select source_id, target_id, best_distance, true, false, output.edge_ids
    from pg_temp.asb_output output
    order by output.edge_ids;
  get diagnostics emitted_count = row_count;
  perform public.clear_bidirectional_all_shortest_path_workspace();
  if telemetry_search_id is not null then
    perform public._finish_bidirectional_all_shortest_path_diagnostic_call_v1(
      telemetry_invocation_id, telemetry_search_id, 'bidirectional_search',
      telemetry_scheduler_actions, telemetry_candidate_edges,
      telemetry_distinct_new_nodes, telemetry_seen_peak,
      telemetry_frontier_peak, telemetry_queue_peak,
      telemetry_predecessor_peak, telemetry_meeting_candidates,
      best_distance, emitted_count, telemetry_same_depth_predecessors,
      meeting_nodes, cut_depth, path_count_estimate,
      telemetry_path_count_saturated, telemetry_enumerated_candidates,
      telemetry_duplicate_rejects, emitted_count,
      emitted_count * best_distance, output_bytes, false, false);
  end if;
  perform public.record_requested_traversal_runtime_attestation_v1('bidirectional_search', false, 'ASP-A1-DAG');
end;
$$
  language plpgsql
  volatile
  strict
  cost 100
  set recursive_worktable_factor = 1
  rows 100;

create or replace function public.all_shortest_paths_b1_strict_alternating(
                                                         target_graph_id int4,
                                                         source_id int8,
                                                         target_id int8,
                                                         min_depth int4,
                                                         max_depth int4,
                                                         edge_kind_ids int2[],
                                                         inbound bool,
                                                         state_limit int8,
                                                         frontier_limit int8,
                                                         predecessor_limit int8,
                                                         enumeration_limit int8,
                                                         output_bytes_limit int8)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
select *
from public.all_shortest_paths_bidirectional_compact_v1(
  target_graph_id, source_id, target_id, min_depth, max_depth,
  edge_kind_ids, inbound, state_limit, frontier_limit, predecessor_limit,
  enumeration_limit, output_bytes_limit, 'strict_alternating_node');
$$
  language sql
  volatile
  strict
  cost 100
  rows 100;

create or replace function public.all_shortest_paths_b2_smaller_current_level(
                                                         target_graph_id int4,
                                                         source_id int8,
                                                         target_id int8,
                                                         min_depth int4,
                                                         max_depth int4,
                                                         edge_kind_ids int2[],
                                                         inbound bool,
                                                         state_limit int8,
                                                         frontier_limit int8,
                                                         predecessor_limit int8,
                                                         enumeration_limit int8,
                                                         output_bytes_limit int8)
  returns table
          (
            root_id int8,
            next_id int8,
            depth int4,
            satisfied bool,
            is_cycle bool,
            path int8[]
          )
as
$$
select *
from public.all_shortest_paths_bidirectional_compact_v1(
  target_graph_id, source_id, target_id, min_depth, max_depth,
  edge_kind_ids, inbound, state_limit, frontier_limit, predecessor_limit,
  enumeration_limit, output_bytes_limit, 'smaller_current_level');
$$
  language sql
  volatile
  strict
  cost 100
  rows 100;

create or replace function public.bsp_workspace_fragment(fragment text)
  returns text as
$$
select replace(
         replace(
           replace(
             case
               when position('pg_temp.bsp_' in fragment) > 0 then fragment
               else replace(
                      replace(
                        replace(
                          replace(
                            replace(
                              replace(
                                replace(fragment,
                                  'on conflict on constraint forward_visited_pkey', 'on conflict on constraint bsp_forward_visited_pkey'),
                                'on conflict on constraint backward_visited_pkey', 'on conflict on constraint bsp_backward_visited_pkey'),
                              'forward_visited', 'pg_temp.bsp_forward_visited'),
                            'backward_visited', 'pg_temp.bsp_backward_visited'),
                          'forward_front', 'pg_temp.bsp_forward_front'),
                        'backward_front', 'pg_temp.bsp_backward_front'),
                      'next_front', 'pg_temp.bsp_next_front')
             end,
             'traversal_root_filter', 'pg_temp.bsp_root_filter'),
           'traversal_terminal_filter', 'pg_temp.bsp_terminal_filter'),
         'traversal_pair_filter', 'pg_temp.bsp_pair_filter');
$$
  language sql
  immutable
  parallel safe
  strict;

-- The bidirectional shortest-path workspace is session-local and survives
-- transaction boundaries. Warm calls retain the table and index OIDs and only
-- clear row state. The version marker lets upgrades rebuild the known object
-- set without touching unrelated temporary objects in the session.
create or replace function public.ensure_bsp_core_workspace()
  returns void as
$$
declare
  expected_version constant int4 := 1;
  present_version int4;
begin
  if to_regclass('pg_temp.bsp_workspace_version') is not null then
    select version into present_version from pg_temp.bsp_workspace_version limit 1;
  end if;

  if present_version is not null and present_version is distinct from expected_version then
    drop table if exists pg_temp.bsp_resolved_pairs;
    drop table if exists pg_temp.bsp_unresolved_pairs;
    drop table if exists pg_temp.bsp_pair_filter;
    drop table if exists pg_temp.bsp_terminal_filter;
    drop table if exists pg_temp.bsp_root_filter;
    drop table if exists pg_temp.bsp_backward_visited;
    drop table if exists pg_temp.bsp_forward_visited;
    drop table if exists pg_temp.bsp_backward_front;
    drop table if exists pg_temp.bsp_next_front;
    drop table if exists pg_temp.bsp_forward_front;
    drop table if exists pg_temp.bsp_workspace_version;
  end if;

  if to_regclass('pg_temp.bsp_workspace_version') is null then
    create temporary table bsp_workspace_version
    (
      version int4 not null primary key
    ) on commit preserve rows;

    create temporary table bsp_forward_front
    (
      root_id int8 not null, next_id int8 not null, depth int4 not null,
      satisfied bool, is_cycle bool not null, path int8[] not null
    ) on commit preserve rows;
    create index bsp_forward_front_next_id_index on bsp_forward_front using btree (next_id);
    create index bsp_forward_front_root_id_next_id_index on bsp_forward_front using btree (root_id, next_id);

    create temporary table bsp_backward_front
    (
      root_id int8 not null, next_id int8 not null, depth int4 not null,
      satisfied bool, is_cycle bool not null, path int8[] not null
    ) on commit preserve rows;
    create index bsp_backward_front_next_id_index on bsp_backward_front using btree (next_id);
    create index bsp_backward_front_root_id_next_id_index on bsp_backward_front using btree (root_id, next_id);

    create temporary table bsp_next_front
    (
      root_id int8 not null, next_id int8 not null, depth int4 not null,
      satisfied bool, is_cycle bool not null, path int8[] not null
    ) on commit preserve rows;
    create index bsp_next_front_next_id_index on bsp_next_front using btree (next_id);
    create index bsp_next_front_root_id_next_id_index on bsp_next_front using btree (root_id, next_id);

    create temporary table bsp_forward_visited
    (
      root_id int8 not null,
      id int8 not null,
      constraint bsp_forward_visited_pkey primary key (root_id, id)
    ) on commit preserve rows;

    create temporary table bsp_backward_visited
    (
      root_id int8 not null,
      id int8 not null,
      constraint bsp_backward_visited_pkey primary key (root_id, id)
    ) on commit preserve rows;

    insert into bsp_workspace_version(version) values (expected_version);
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.ensure_bsp_generic_workspace()
  returns void as
$$
begin
  perform public.ensure_bsp_core_workspace();

  if to_regclass('pg_temp.bsp_root_filter') is null then
    create temporary table bsp_root_filter
    (
      id int8 not null primary key
    ) on commit preserve rows;
    create temporary table bsp_terminal_filter
    (
      id int8 not null primary key
    ) on commit preserve rows;
    create temporary table bsp_pair_filter
    (
      root_id int8 not null,
      terminal_id int8 not null,
      primary key (root_id, terminal_id)
    ) on commit preserve rows;
    create index bsp_pair_filter_terminal_id_root_id_index on bsp_pair_filter using btree (terminal_id, root_id);

    create temporary table bsp_unresolved_pairs
    (
      root_id int8 not null,
      terminal_id int8 not null,
      constraint bsp_unresolved_pairs_pkey primary key (root_id, terminal_id)
    ) on commit preserve rows;
    create index bsp_unresolved_pairs_terminal_id_root_id_index on bsp_unresolved_pairs using btree (terminal_id, root_id);

    create temporary table bsp_resolved_pairs
    (
      root_id int8 not null, next_id int8 not null, depth int4 not null,
      satisfied bool, is_cycle bool not null, path int8[] not null,
      constraint bsp_resolved_pairs_pkey primary key (root_id, next_id)
    ) on commit preserve rows;
  end if;
end;
$$
  language plpgsql
  volatile;

create or replace function public.reset_bsp_workspace(include_generic bool)
  returns void as
$$
begin
  if include_generic then
    perform public.ensure_bsp_generic_workspace();
    truncate table pg_temp.bsp_forward_front, pg_temp.bsp_backward_front, pg_temp.bsp_next_front,
                   pg_temp.bsp_forward_visited, pg_temp.bsp_backward_visited,
                   pg_temp.bsp_root_filter, pg_temp.bsp_terminal_filter, pg_temp.bsp_pair_filter,
                   pg_temp.bsp_unresolved_pairs, pg_temp.bsp_resolved_pairs;
  else
    perform public.ensure_bsp_core_workspace();
    truncate table pg_temp.bsp_forward_front, pg_temp.bsp_backward_front, pg_temp.bsp_next_front,
                   pg_temp.bsp_forward_visited, pg_temp.bsp_backward_visited;
  end if;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.load_bsp_filter_tables(root_filter text, terminal_filter text, pair_filter text)
  returns void as
$$
begin
  if length(pair_filter) > 0 then
    execute replace(pair_filter, 'traversal_pair_filter', 'pg_temp.bsp_pair_filter');
  end if;
  if length(root_filter) > 0 then
    execute replace(root_filter, 'traversal_root_filter', 'pg_temp.bsp_root_filter');
  elsif length(pair_filter) > 0 then
    insert into pg_temp.bsp_root_filter
    select distinct root_id from pg_temp.bsp_pair_filter
    on conflict (id) do nothing;
  end if;
  if length(terminal_filter) > 0 then
    execute replace(terminal_filter, 'traversal_terminal_filter', 'pg_temp.bsp_terminal_filter');
  elsif length(pair_filter) > 0 then
    insert into pg_temp.bsp_terminal_filter
    select distinct terminal_id from pg_temp.bsp_pair_filter
    on conflict (id) do nothing;
  end if;

  analyze pg_temp.bsp_root_filter;
  analyze pg_temp.bsp_terminal_filter;
  analyze pg_temp.bsp_pair_filter;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.create_bidirectional_pathspace_tables()
  returns void as
$$
begin
  perform create_unidirectional_pathspace_tables();

  create temporary table if not exists backward_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit preserve rows;

  create index if not exists backward_front_next_id_index on backward_front using btree (next_id);
  create index if not exists backward_front_satisfied_index on backward_front using btree (root_id, next_id, depth) where satisfied;
  create index if not exists backward_front_is_cycle_index on backward_front using btree (root_id, next_id) where is_cycle;

  truncate table backward_front;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.create_bidirectional_pair_pathspace_indexes()
  returns void as
$$
begin
  create index if not exists forward_front_root_id_next_id_index on forward_front using btree (root_id, next_id);
  create index if not exists backward_front_root_id_next_id_index on backward_front using btree (root_id, next_id);
  create index if not exists next_front_root_id_next_id_index on next_front using btree (root_id, next_id);
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.create_bidirectional_shortest_path_tables()
  returns void as
$$
begin
  create temporary table if not exists forward_visited
  (
    root_id int8 not null,
    id      int8 not null,
    primary key (root_id, id)
  ) on commit preserve rows;

  create temporary table if not exists backward_visited
  (
    root_id int8 not null,
    id      int8 not null,
    primary key (root_id, id)
  ) on commit preserve rows;

  truncate table forward_visited, backward_visited;

  perform create_bidirectional_pathspace_tables();
  perform create_bidirectional_pair_pathspace_indexes();
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.swap_forward_front()
  returns void as
$$
begin

  truncate table forward_front;
  insert into forward_front select * from next_front;
  truncate table next_front;

  delete from forward_front r where r.is_cycle;
  delete from forward_front r where r.satisfied is null;
  delete from forward_front r where not r.satisfied and not exists(select 1 from edge e where e.start_id = r.next_id);

  return;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.swap_backward_front()
  returns void as
$$
begin

  truncate table backward_front;
  insert into backward_front select * from next_front;
  truncate table next_front;

  delete from backward_front r where r.is_cycle;
  delete from backward_front r where r.satisfied is null;
  delete from backward_front r where not r.satisfied and not exists(select 1 from edge e where e.end_id = r.next_id);

  return;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                            root_ids int8[], terminal_ids int8[], path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
#variable_conflict use_column
declare
  forward_front_depth   int4 := 0;
  terminal_filter_count int8 := 0;
  inserted_path_count   int8 := 0;
  paths_count           int8 := 0;
begin
  raise debug 'Shortest Path Harness starting';

  -- Create all tables necessary to drive traversal
  perform create_unidirectional_shortest_path_tables();
  perform create_traversal_filter_tables(root_ids, terminal_ids);
  select count(*) into terminal_filter_count from traversal_terminal_filter;

  while forward_front_depth < max_depth and
        (path_limit <= 0 or paths_count < path_limit) and
        (forward_front_depth = 0 or exists(select 1 from forward_front))
    loop
      if forward_front_depth = 0 then
        execute forward_primer using root_ids, terminal_ids;

        -- Insert all root nodes as visited
        insert into visited (root_id, id) select distinct f.root_id, f.root_id from next_front f on conflict on constraint visited_pkey do nothing;
      else
        execute forward_recursive using root_ids, terminal_ids;
      end if;

      forward_front_depth = forward_front_depth + 1;

      -- Swap the next_front table into the forward_front
      -- Remove cycles and non-conformant satisfaction checks
      delete from next_front f where f.is_cycle;
      delete from next_front f where f.satisfied is null;
      delete from next_front f using visited v where f.root_id = v.root_id and f.next_id = v.id;

      raise debug 'Expansion step %', forward_front_depth;

      -- Insert new newly visited nodes into the visited table
      insert into visited (root_id, id) select distinct f.root_id, f.next_id from next_front f on conflict on constraint visited_pkey do nothing;

      -- Copy pathspace over into the next front
      truncate table forward_front;

      insert into forward_front
      select distinct on (f.root_id, f.next_id) f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
      from next_front f
      order by f.root_id, f.next_id, f.depth;

      -- Copy newly satisfied paths into the path table
      if path_limit > 0 then
        insert into paths
        select f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from forward_front f
        where f.satisfied
        limit path_limit - paths_count;
      else
        insert into paths
        select f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from forward_front f
        where f.satisfied;
      end if;
      get diagnostics inserted_path_count = row_count;
      paths_count = paths_count + inserted_path_count;

      if terminal_filter_count > 0 then
        insert into resolved_roots (root_id)
        select p.root_id
        from paths p
        group by p.root_id
        having count(distinct p.next_id) >= terminal_filter_count
        on conflict on constraint resolved_roots_pkey do nothing;

        delete from forward_front f using resolved_roots r where f.root_id = r.root_id;
      end if;

      -- Empty the next front last to capture the next expansion
      truncate table next_front;
    end loop;

  if path_limit > 0 then
    return query select * from paths limit path_limit;
  else
    return query select * from paths;
  end if;

  -- This bare return is not an error. This closes this function's resultset, and the return above will
  -- be treated as a yield and continue execution once the result cursor is exhausted.
  return;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                            root_ids int8[], terminal_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_sp_harness(forward_primer, forward_recursive, max_depth, root_ids, terminal_ids, 0::int8);
$$
  language sql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                            root_ids int8[], path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_sp_harness(forward_primer, forward_recursive, max_depth, root_ids, array []::int8[], path_limit);
$$
  language sql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                            root_filter text, terminal_filter text, path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
#variable_conflict use_column
declare
  forward_front_depth   int4 := 0;
  terminal_filter_count int8 := 0;
  inserted_path_count   int8 := 0;
  paths_count           int8 := 0;
begin
  raise debug 'Shortest Path Harness starting';

  -- Create all tables necessary to drive traversal
  perform create_unidirectional_shortest_path_tables();
  perform create_traversal_filter_tables(root_filter, terminal_filter);
  select count(*) into terminal_filter_count from traversal_terminal_filter;

  while forward_front_depth < max_depth and
        (path_limit <= 0 or paths_count < path_limit) and
        (forward_front_depth = 0 or exists(select 1 from forward_front))
    loop
      if forward_front_depth = 0 then
        execute forward_primer;

        -- Insert all root nodes as visited
        insert into visited (root_id, id) select distinct f.root_id, f.root_id from next_front f on conflict on constraint visited_pkey do nothing;
      else
        execute forward_recursive;
      end if;

      forward_front_depth = forward_front_depth + 1;

      -- Swap the next_front table into the forward_front
      -- Remove cycles and non-conformant satisfaction checks
      delete from next_front f where f.is_cycle;
      delete from next_front f where f.satisfied is null;
      delete from next_front f using visited v where f.root_id = v.root_id and f.next_id = v.id;

      raise debug 'Expansion step %', forward_front_depth;

      -- Insert new newly visited nodes into the visited table
      insert into visited (root_id, id) select distinct f.root_id, f.next_id from next_front f on conflict on constraint visited_pkey do nothing;

      -- Copy pathspace over into the next front
      truncate table forward_front;

      insert into forward_front
      select distinct on (f.root_id, f.next_id) f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
      from next_front f
      order by f.root_id, f.next_id, f.depth;

      -- Copy newly satisfied paths into the path table
      if path_limit > 0 then
        insert into paths
        select f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from forward_front f
        where f.satisfied
        limit path_limit - paths_count;
      else
        insert into paths
        select f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from forward_front f
        where f.satisfied;
      end if;
      get diagnostics inserted_path_count = row_count;
      paths_count = paths_count + inserted_path_count;

      if terminal_filter_count > 0 then
        insert into resolved_roots (root_id)
        select p.root_id
        from paths p
        group by p.root_id
        having count(distinct p.next_id) >= terminal_filter_count
        on conflict on constraint resolved_roots_pkey do nothing;

        delete from forward_front f using resolved_roots r where f.root_id = r.root_id;
      end if;

      -- Empty the next front last to capture the next expansion
      truncate table next_front;
    end loop;

  if path_limit > 0 then
    return query select * from paths limit path_limit;
  else
    return query select * from paths;
  end if;

  -- This bare return is not an error. This closes this function's resultset, and the return above will
  -- be treated as a yield and continue execution once the result cursor is exhausted.
  return;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                            root_filter text, terminal_filter text)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_sp_harness(forward_primer, forward_recursive, max_depth, root_filter, terminal_filter, 0::int8);
$$
  language sql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                            root_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_sp_harness(forward_primer, forward_recursive, max_depth, root_ids, array []::int8[]);
$$
  language sql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                            path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_sp_harness(forward_primer, forward_recursive, max_depth, array []::int8[], array []::int8[], path_limit);
$$
  language sql
  volatile
  strict;

create or replace function public.unidirectional_sp_harness(forward_primer text, forward_recursive text, max_depth int4)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_sp_harness(forward_primer, forward_recursive, max_depth, array []::int8[], array []::int8[]);
$$
  language sql
  volatile
  strict;

create or replace function public.unidirectional_asp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                             root_ids int8[], terminal_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
#variable_conflict use_column
declare
  forward_front_depth int4 := 0;
begin
  raise debug 'unidirectional_asp_harness start';

  -- Defines two tables to represent pathspace of the recursive expansion
  perform create_unidirectional_pathspace_tables();
  perform create_traversal_filter_tables(root_ids, terminal_ids);

  while forward_front_depth < max_depth and (forward_front_depth = 0 or exists(select 1 from forward_front))
    loop
    -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
    -- recursive expansion query
      if forward_front_depth = 0 then
        execute forward_primer using root_ids, terminal_ids;
      else
        execute forward_recursive using root_ids, terminal_ids;
      end if;

      forward_front_depth = forward_front_depth + 1;

      raise debug 'Expansion step %', forward_front_depth;

      -- Check to see if the root front is satisfied
      if exists(select 1 from next_front r where r.satisfied) then
        -- Return all satisfied paths from the next front
        return query select * from next_front r where r.satisfied;
        exit;
      end if;

      -- Swap the next_front table into the forward_front
      perform swap_forward_front();
    end loop;

  -- This bare return is not an error. This closes this function's resultset, and the return above will
  -- be treated as a yield and continue execution once the result cursor is exhausted.
  return;
end;
$$
  language plpgsql volatile
                   strict;

create or replace function public.unidirectional_asp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                             root_filter text, terminal_filter text)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
#variable_conflict use_column
declare
  forward_front_depth int4 := 0;
begin
  raise debug 'unidirectional_asp_harness start';

  -- Defines two tables to represent pathspace of the recursive expansion
  perform create_unidirectional_pathspace_tables();
  perform create_traversal_filter_tables(root_filter, terminal_filter);

  while forward_front_depth < max_depth and (forward_front_depth = 0 or exists(select 1 from forward_front))
    loop
    -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
    -- recursive expansion query
      if forward_front_depth = 0 then
        execute forward_primer;
      else
        execute forward_recursive;
      end if;

      forward_front_depth = forward_front_depth + 1;

      raise debug 'Expansion step %', forward_front_depth;

      -- Check to see if the root front is satisfied
      if exists(select 1 from next_front r where r.satisfied) then
        -- Return all satisfied paths from the next front
        return query select * from next_front r where r.satisfied;
        exit;
      end if;

      -- Swap the next_front table into the forward_front
      perform swap_forward_front();
    end loop;

  -- This bare return is not an error. This closes this function's resultset, and the return above will
  -- be treated as a yield and continue execution once the result cursor is exhausted.
  return;
end;
$$
  language plpgsql volatile
                   strict;

create or replace function public.unidirectional_asp_harness(forward_primer text, forward_recursive text, max_depth int4,
                                                             root_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_asp_harness(forward_primer, forward_recursive, max_depth, root_ids, array []::int8[]);
$$
  language sql volatile
               strict;

create or replace function public.unidirectional_asp_harness(forward_primer text, forward_recursive text, max_depth int4)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.unidirectional_asp_harness(forward_primer, forward_recursive, max_depth, array []::int8[], array []::int8[]);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_asp_harness(forward_primer text, forward_recursive text,
                                                            backward_primer text,
                                                            backward_recursive text, max_depth int4,
                                                            root_ids int8[], terminal_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
#variable_conflict use_column
declare
  forward_front_depth  int4 := 0;
  backward_front_depth int4 := 0;
  forward_front_count  int8 := 0;
  backward_front_count int8 := 0;
  next_front_count     int8 := 0;
  matched_count        int8 := 0;
begin
  raise debug 'bidirectional_asp_harness start';

  -- Defines three tables to represent pathspace of the recursive expansion
  perform create_bidirectional_pathspace_tables();
  perform create_traversal_filter_tables(root_ids, terminal_ids);

  while forward_front_depth + backward_front_depth < max_depth and
        (forward_front_depth = 0 or forward_front_count > 0) and
        (backward_front_depth = 0 or backward_front_count > 0)
    loop
      -- Check to expand the smaller of the two frontiers, or if both are the same size prefer the forward frontier
      if forward_front_depth = 0 or (backward_front_depth > 0 and forward_front_count <= backward_front_count) then
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if forward_front_depth = 0 then
          execute forward_primer using root_ids, terminal_ids;
        else
          execute forward_recursive using root_ids, terminal_ids;
        end if;

        get diagnostics next_front_count = row_count;
        forward_front_depth = forward_front_depth + 1;

        raise debug 'Forward expansion as step % - Available Root Paths %', forward_front_depth + backward_front_depth, next_front_count;

        -- Check to see if the next frontier is satisfied
        if exists(select 1 from next_front r where r.satisfied) then
          return query select * from next_front r where r.satisfied;
          exit;
        end if;

        -- Swap the next_front table into the forward_front
        perform swap_forward_front();
        forward_front_count = next_front_count;
      else
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if backward_front_depth = 0 then
          execute backward_primer using root_ids, terminal_ids;
        else
          execute backward_recursive using root_ids, terminal_ids;
        end if;

        get diagnostics next_front_count = row_count;
        backward_front_depth = backward_front_depth + 1;
        raise debug 'Backward expansion as step % - Available Terminal Paths %', forward_front_depth + backward_front_depth, next_front_count;

        -- Check to see if the next frontier is satisfied
        if exists(select 1 from next_front r where r.satisfied) then
          return query select r.next_id,
                              r.root_id,
                              r.depth,
                              r.satisfied,
                              r.is_cycle,
                              r.path
                       from next_front r
                       where r.satisfied;
          exit;
        end if;

        -- Swap the next_front table into the backward_front
        perform swap_backward_front();
        backward_front_count = next_front_count;
      end if;

      -- Zip the path arrays together treating midpoint matches as satisfied
      return query select f.root_id,
                          b.root_id,
                          f.depth + b.depth,
                          true,
                          false,
                          f.path || b.path
                   from forward_front f
                          join backward_front b on f.next_id = b.next_id;
      get diagnostics matched_count = row_count;

      if matched_count > 0 then
        exit;
      end if;
    end loop;

  -- This bare return is not an error. This closes this function's result set and the return above will
  -- be treated as a yield and continue execution once the results cursor is exhausted.
  return;
end;
$$
  language plpgsql volatile
                   strict;

create or replace function public.bidirectional_asp_harness(forward_primer text, forward_recursive text,
                                                            backward_primer text,
                                                            backward_recursive text, max_depth int4,
                                                            root_filter text, terminal_filter text, pair_filter text)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
#variable_conflict use_column
declare
  forward_front_depth  int4 := 0;
  backward_front_depth int4 := 0;
  forward_front_count  int8 := 0;
  backward_front_count int8 := 0;
  next_front_count     int8 := 0;
  deleted_count        int8 := 0;
  use_pair_filter      bool := length(pair_filter) > 0;
  matched_count        int8 := 0;
begin
  raise debug 'bidirectional_asp_harness start';

  -- Defines three tables to represent pathspace of the recursive expansion
  perform create_bidirectional_pathspace_tables();
  perform create_traversal_filter_tables(root_filter, terminal_filter, pair_filter);

  if use_pair_filter then
    perform create_bidirectional_pair_pathspace_indexes();
  end if;

  create temporary table if not exists unresolved_pairs
  (
    root_id     int8 not null,
    terminal_id int8 not null,
    primary key (root_id, terminal_id)
  ) on commit preserve rows;

  create index if not exists unresolved_pairs_terminal_id_root_id_index on unresolved_pairs using btree (terminal_id, root_id);

  create temporary table if not exists resolved_pair_depths
  (
    root_id     int8 not null,
    terminal_id int8 not null,
    depth       int4 not null,
    primary key (root_id, terminal_id)
  ) on commit preserve rows;

  create temporary table if not exists resolved_paths
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit preserve rows;

  truncate table unresolved_pairs, resolved_pair_depths, resolved_paths;

  if use_pair_filter then
    insert into unresolved_pairs (root_id, terminal_id)
    select distinct root_id, terminal_id
    from traversal_pair_filter
    on conflict on constraint unresolved_pairs_pkey do nothing;
  end if;

  while forward_front_depth + backward_front_depth < max_depth and
        (not use_pair_filter or exists(select 1 from unresolved_pairs)) and
        (forward_front_depth = 0 or forward_front_count > 0) and
        (backward_front_depth = 0 or backward_front_count > 0)
    loop
      -- Check to expand the smaller of the two frontiers, or if both are the same size prefer the forward frontier
      if forward_front_depth = 0 or (backward_front_depth > 0 and forward_front_count <= backward_front_count) then
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if forward_front_depth = 0 then
          execute forward_primer;
        else
          execute forward_recursive;
        end if;

        get diagnostics next_front_count = row_count;
        forward_front_depth = forward_front_depth + 1;

        raise debug 'Forward expansion as step % - Available Root Paths %', forward_front_depth + backward_front_depth, next_front_count;

        -- Check to see if the next frontier is satisfied
        if exists(select 1 from next_front r where r.satisfied) then
          if use_pair_filter then
            with inserted_depths as (
              insert into resolved_pair_depths (root_id, terminal_id, depth)
                select distinct r.root_id, r.next_id, r.depth
                from next_front r
                       join unresolved_pairs p on p.root_id = r.root_id and p.terminal_id = r.next_id
                where r.satisfied
                on conflict on constraint resolved_pair_depths_pkey do nothing
                returning root_id, terminal_id, depth
            )
            insert
            into resolved_paths (root_id, next_id, depth, satisfied, is_cycle, path)
            select r.root_id, r.next_id, r.depth, r.satisfied, r.is_cycle, r.path
            from next_front r
                   join inserted_depths p on p.root_id = r.root_id and
                                             p.terminal_id = r.next_id and
                                             p.depth = r.depth
            where r.satisfied;
            get diagnostics matched_count = row_count;

            if matched_count > 0 then
              delete
              from unresolved_pairs p
                using resolved_pair_depths r
              where p.root_id = r.root_id
                and p.terminal_id = r.terminal_id;

              delete from next_front f where not exists(select 1 from unresolved_pairs p where p.root_id = f.root_id);
              get diagnostics deleted_count = row_count;
              next_front_count = next_front_count - deleted_count;

              delete from backward_front b where not exists(select 1 from unresolved_pairs p where p.terminal_id = b.root_id);
              get diagnostics deleted_count = row_count;
              backward_front_count = backward_front_count - deleted_count;
            end if;
          else
            return query select * from next_front r where r.satisfied;
            exit;
          end if;
        end if;

        -- Swap the next_front table into the forward_front
        perform swap_forward_front();
        forward_front_count = next_front_count;
      else
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if backward_front_depth = 0 then
          execute backward_primer;
        else
          execute backward_recursive;
        end if;

        get diagnostics next_front_count = row_count;
        backward_front_depth = backward_front_depth + 1;
        raise debug 'Backward expansion as step % - Available Terminal Paths %', forward_front_depth + backward_front_depth, next_front_count;

        -- Check to see if the next frontier is satisfied
        if exists(select 1 from next_front r where r.satisfied) then
          if use_pair_filter then
            with inserted_depths as (
              insert into resolved_pair_depths (root_id, terminal_id, depth)
                select distinct r.next_id, r.root_id, r.depth
                from next_front r
                       join unresolved_pairs p on p.root_id = r.next_id and p.terminal_id = r.root_id
                where r.satisfied
                on conflict on constraint resolved_pair_depths_pkey do nothing
                returning root_id, terminal_id, depth
            )
            insert
            into resolved_paths (root_id, next_id, depth, satisfied, is_cycle, path)
            select r.next_id, r.root_id, r.depth, r.satisfied, r.is_cycle, r.path
            from next_front r
                   join inserted_depths p on p.root_id = r.next_id and
                                             p.terminal_id = r.root_id and
                                             p.depth = r.depth
            where r.satisfied;
            get diagnostics matched_count = row_count;

            if matched_count > 0 then
              delete
              from unresolved_pairs p
                using resolved_pair_depths r
              where p.root_id = r.root_id
                and p.terminal_id = r.terminal_id;

              delete from next_front f where not exists(select 1 from unresolved_pairs p where p.terminal_id = f.root_id);
              get diagnostics deleted_count = row_count;
              next_front_count = next_front_count - deleted_count;

              delete from forward_front f where not exists(select 1 from unresolved_pairs p where p.root_id = f.root_id);
              get diagnostics deleted_count = row_count;
              forward_front_count = forward_front_count - deleted_count;
            end if;
          else
            return query select r.next_id,
                                r.root_id,
                                r.depth,
                                r.satisfied,
                                r.is_cycle,
                                r.path
                         from next_front r
                         where r.satisfied;
            exit;
          end if;
        end if;

        -- Swap the next_front table into the backward_front
        perform swap_backward_front();
        backward_front_count = next_front_count;
      end if;

      -- Check to see if the two frontiers meet somewhere in the middle
      if use_pair_filter then
        -- Zip the path arrays together treating the matches as satisfied
        with inserted_depths as (
          insert into resolved_pair_depths (root_id, terminal_id, depth)
            select p.root_id, p.terminal_id, midpoint.depth
            from unresolved_pairs p
                   join lateral (
              select f.depth + b.depth as depth
              from forward_front f
                     join backward_front b on b.root_id = p.terminal_id and b.next_id = f.next_id
              where f.root_id = p.root_id
              order by f.depth + b.depth
              limit 1
              ) midpoint on true
            on conflict on constraint resolved_pair_depths_pkey do nothing
            returning root_id, terminal_id, depth
        )
        insert
        into resolved_paths (root_id, next_id, depth, satisfied, is_cycle, path)
        select p.root_id,
               p.terminal_id,
               p.depth,
               true,
               false,
               midpoint.path
        from inserted_depths p
               join lateral (
          select f.path || b.path as path
          from forward_front f
                 join backward_front b on b.root_id = p.terminal_id and b.next_id = f.next_id
          where f.root_id = p.root_id
            and f.depth + b.depth = p.depth
          ) midpoint on true;
        get diagnostics matched_count = row_count;

        if matched_count > 0 then
          delete
          from unresolved_pairs p
            using resolved_pair_depths r
          where p.root_id = r.root_id
            and p.terminal_id = r.terminal_id;

          delete from forward_front f where not exists(select 1 from unresolved_pairs p where p.root_id = f.root_id);
          get diagnostics deleted_count = row_count;
          forward_front_count = forward_front_count - deleted_count;

          delete from backward_front b where not exists(select 1 from unresolved_pairs p where p.terminal_id = b.root_id);
          get diagnostics deleted_count = row_count;
          backward_front_count = backward_front_count - deleted_count;
        end if;
      else
        -- Zip the path arrays together treating the matches as satisfied
        return query select f.root_id,
                            b.root_id,
                            f.depth + b.depth,
                            true,
                            false,
                            f.path || b.path
                     from forward_front f
                            join backward_front b on f.next_id = b.next_id;
        get diagnostics matched_count = row_count;

        if matched_count > 0 then
          exit;
        end if;
      end if;
    end loop;

  if use_pair_filter then
    return query select *
                 from resolved_paths
                 order by root_id, next_id, depth;
  end if;

  -- This bare return is not an error. This closes this function's result set and the return above will
  -- be treated as a yield and continue execution once the results cursor is exhausted.
  return;
end;
$$
  language plpgsql volatile
                   strict;

create or replace function public.bidirectional_asp_harness(forward_primer text, forward_recursive text,
                                                            backward_primer text,
                                                            backward_recursive text, max_depth int4,
                                                            root_filter text, terminal_filter text)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_asp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, ''::text);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_asp_harness(forward_primer text, forward_recursive text,
                                                            backward_primer text,
                                                            backward_recursive text, max_depth int4,
                                                            root_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_asp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_ids, array []::int8[]);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_asp_harness(forward_primer text, forward_recursive text,
                                                            backward_primer text,
                                                            backward_recursive text, max_depth int4)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_asp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, array []::int8[], array []::int8[]);
$$
  language sql volatile
               strict;

drop function if exists public._bidirectional_sp_harness(text, text, text, text, int4, text, text, int8[], int8[], bool);
drop function if exists public._bidirectional_sp_harness(text, text, text, text, int4, text, text, text, int8[], int8[], bool);
drop function if exists public._bidirectional_sp_harness(text, text, text, text, int4, text, text, text, int8[], int8[], int8, bool);
drop function if exists public._bidirectional_sp_harness(text, text, text, text, int4, text, text, text, int8[], int8[], int8, bool, bool);

-- _bidirectional_sp_harness implements the shortest-path bidirectional BFS in two control paths selected by
-- `use_array_parameters`:
--   * `use_array_parameters = true`: the primer/recursive queries reference $1/$2 parameter placeholders bound to
--     `root_ids` and `terminal_ids`; `root_filter`, `terminal_filter` and `pair_filter` are ignored.
--   * `use_array_parameters = false`: the primer/recursive queries are self-contained and `root_filter`,
--     `terminal_filter`, and `pair_filter` are executed (when non-empty) to materialize traversal filter tables that
--     the primer/recursive queries join against.
-- When `pair_filter` is non-empty (and `use_array_parameters` is false) the harness runs in pair-filter mode: each
-- (root, terminal) pair tracks its own resolution and pruning, allowing per-pair early termination. Otherwise it runs
-- in batch mode and emits the first satisfied frontier.
create or replace function public._bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                            backward_primer text,
                                                            backward_recursive text, max_depth int4,
                                                            root_filter text, terminal_filter text, pair_filter text,
                                                            root_ids int8[], terminal_ids int8[],
                                                            path_limit int8,
                                                            allow_zero_depth bool,
                                                            use_array_parameters bool)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
#variable_conflict use_column
declare
  forward_front_depth  int4 := 0;
  backward_front_depth int4 := 0;
  forward_front_count  int8 := 0;
  backward_front_count int8 := 0;
  next_front_count     int8 := 0;
  deleted_count        int8 := 0;
  use_pair_filter      bool := not use_array_parameters and length(pair_filter) > 0;
  matched_count        int8 := 0;
  resolved_pairs_count int8 := 0;
  unresolved_pairs_remaining bool := true;
begin
  raise debug 'bidirectional_sp_harness start';


  -- Validate the lean array mode before allocating its session workspace.
  -- NULL endpoints represent an empty endpoint relation. Equal singleton IDs
  -- retain the existing shortest-path error contract.
  if use_array_parameters then
    if cardinality(root_ids) = 0 or cardinality(terminal_ids) = 0 or
       root_ids[1] is null or terminal_ids[1] is null then
      return;
    end if;
    if cardinality(root_ids) = 1 and cardinality(terminal_ids) = 1 and root_ids[1] = terminal_ids[1] then
      if allow_zero_depth then
        return query select root_ids[1], terminal_ids[1], 0::int4, true, false, array []::int8[];
        return;
      else
        perform public.shortest_path_self_endpoint_error(root_ids[1], terminal_ids[1]);
      end if;
    end if;
  end if;


  -- Array-parameter calls (including the proven singleton lowering) need only
  -- the frontier/visited core. Text-filter calls lazily add pair/filter state.
  perform public.reset_bsp_workspace(not use_array_parameters);

  if not use_array_parameters then
    perform public.load_bsp_filter_tables(root_filter, terminal_filter, pair_filter);
  end if;

  if use_pair_filter then
    insert into pg_temp.bsp_unresolved_pairs (root_id, terminal_id)
    select distinct root_id, terminal_id
    from pg_temp.bsp_pair_filter
    on conflict on constraint bsp_unresolved_pairs_pkey do nothing;

    if allow_zero_depth then
      insert into pg_temp.bsp_resolved_pairs (root_id, next_id, depth, satisfied, is_cycle, path)
      select root_id, terminal_id, 0::int4, true, false, array []::int8[]
      from pg_temp.bsp_unresolved_pairs
      where root_id = terminal_id
      on conflict on constraint bsp_resolved_pairs_pkey do nothing;
      get diagnostics resolved_pairs_count = row_count;

      delete from pg_temp.bsp_unresolved_pairs where root_id = terminal_id;
    end if;

    select exists(select 1 from pg_temp.bsp_unresolved_pairs) into unresolved_pairs_remaining;
  end if;

  -- Pair-filter mode keeps expanding until each requested pair is resolved or
  -- the limit is met. Batch mode returns from inside the loop as soon as the
  -- current BFS depth produces results.
  while forward_front_depth + backward_front_depth < max_depth and
        (path_limit <= 0 or resolved_pairs_count < path_limit) and
        unresolved_pairs_remaining and
        (forward_front_depth = 0 or forward_front_count > 0) and
        (backward_front_depth = 0 or backward_front_count > 0)
    loop
      if forward_front_depth = 0 or (backward_front_depth > 0 and forward_front_count <= backward_front_count) then
        if forward_front_depth = 0 then
          if use_array_parameters then
            execute public.bsp_workspace_fragment(forward_primer) using root_ids, terminal_ids;
          else
            execute public.bsp_workspace_fragment(forward_primer);
          end if;

          get diagnostics next_front_count = row_count;

          insert into pg_temp.bsp_forward_visited (root_id, id)
          select distinct f.root_id, f.root_id
          from pg_temp.bsp_next_front f
          on conflict on constraint bsp_forward_visited_pkey do nothing;
        else
          if use_array_parameters then
            execute public.bsp_workspace_fragment(forward_recursive) using root_ids, terminal_ids;
          else
            execute public.bsp_workspace_fragment(forward_recursive);
          end if;

          get diagnostics next_front_count = row_count;
        end if;

        forward_front_depth = forward_front_depth + 1;

        delete from pg_temp.bsp_next_front f where f.is_cycle;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from pg_temp.bsp_next_front f where f.satisfied is null;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from pg_temp.bsp_next_front f using pg_temp.bsp_forward_visited v where f.root_id = v.root_id and f.next_id = v.id;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        raise debug 'Forward shortest expansion as step % - Available Root Paths %', forward_front_depth + backward_front_depth, next_front_count;

        truncate table pg_temp.bsp_forward_front;

        insert into pg_temp.bsp_forward_front
        select distinct on (f.root_id, f.next_id) f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from pg_temp.bsp_next_front f
        order by f.root_id, f.next_id, f.depth;
        get diagnostics forward_front_count = row_count;

        truncate table pg_temp.bsp_next_front;

        insert into pg_temp.bsp_forward_visited (root_id, id)
        select f.root_id, f.next_id
        from pg_temp.bsp_forward_front f
        on conflict on constraint bsp_forward_visited_pkey do nothing;

        if exists(select 1 from pg_temp.bsp_forward_front r where r.satisfied) then
          if use_pair_filter then
            -- A direct forward hit resolves only the requested pairs it satisfies.
            -- Frontiers for completed roots/terminals are pruned below.
            insert into pg_temp.bsp_resolved_pairs (root_id, next_id, depth, satisfied, is_cycle, path)
            select distinct on (r.root_id, r.next_id) r.root_id,
                                                      r.next_id,
                                                      r.depth,
                                                      r.satisfied,
                                                      r.is_cycle,
                                                      r.path
            from pg_temp.bsp_forward_front r
                   join pg_temp.bsp_unresolved_pairs p on p.root_id = r.root_id and p.terminal_id = r.next_id
            where r.satisfied
            order by r.root_id, r.next_id, r.depth
            on conflict on constraint bsp_resolved_pairs_pkey do nothing;
            get diagnostics matched_count = row_count;
            resolved_pairs_count = resolved_pairs_count + matched_count;

            delete
            from pg_temp.bsp_unresolved_pairs p
              using pg_temp.bsp_resolved_pairs r
            where p.root_id = r.root_id
              and p.terminal_id = r.next_id;
            select exists(select 1 from pg_temp.bsp_unresolved_pairs) into unresolved_pairs_remaining;

            delete from pg_temp.bsp_forward_front f where not exists(select 1 from pg_temp.bsp_unresolved_pairs p where p.root_id = f.root_id);
            get diagnostics deleted_count = row_count;
            forward_front_count = forward_front_count - deleted_count;

            delete from pg_temp.bsp_backward_front b where not exists(select 1 from pg_temp.bsp_unresolved_pairs p where p.terminal_id = b.root_id);
            get diagnostics deleted_count = row_count;
            backward_front_count = backward_front_count - deleted_count;
          else
            -- Without pair tracking, the first satisfied frontier is the shortest
            -- frontier for this batch, so return it immediately.
            return query select distinct on (r.root_id, r.next_id) r.root_id,
                                                                    r.next_id,
                                                                    r.depth,
                                                                    r.satisfied,
                                                                    r.is_cycle,
                                                                    r.path
                         from pg_temp.bsp_forward_front r
                         where r.satisfied
                         order by r.root_id, r.next_id, r.depth
                         limit case when path_limit > 0 then path_limit else null end;
            exit;
          end if;
        end if;
      else
        if backward_front_depth = 0 then
          if use_array_parameters then
            execute public.bsp_workspace_fragment(backward_primer) using root_ids, terminal_ids;
          else
            execute public.bsp_workspace_fragment(backward_primer);
          end if;

          get diagnostics next_front_count = row_count;

          insert into pg_temp.bsp_backward_visited (root_id, id)
          select distinct f.root_id, f.root_id
          from pg_temp.bsp_next_front f
          on conflict on constraint bsp_backward_visited_pkey do nothing;
        else
          if use_array_parameters then
            execute public.bsp_workspace_fragment(backward_recursive) using root_ids, terminal_ids;
          else
            execute public.bsp_workspace_fragment(backward_recursive);
          end if;

          get diagnostics next_front_count = row_count;
        end if;

        backward_front_depth = backward_front_depth + 1;

        delete from pg_temp.bsp_next_front f where f.is_cycle;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from pg_temp.bsp_next_front f where f.satisfied is null;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from pg_temp.bsp_next_front f using pg_temp.bsp_backward_visited v where f.root_id = v.root_id and f.next_id = v.id;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        raise debug 'Backward shortest expansion as step % - Available Terminal Paths %', forward_front_depth + backward_front_depth, next_front_count;

        truncate table pg_temp.bsp_backward_front;

        insert into pg_temp.bsp_backward_front
        select distinct on (f.root_id, f.next_id) f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from pg_temp.bsp_next_front f
        order by f.root_id, f.next_id, f.depth;
        get diagnostics backward_front_count = row_count;

        truncate table pg_temp.bsp_next_front;

        insert into pg_temp.bsp_backward_visited (root_id, id)
        select f.root_id, f.next_id
        from pg_temp.bsp_backward_front f
        on conflict on constraint bsp_backward_visited_pkey do nothing;

        if exists(select 1 from pg_temp.bsp_backward_front r where r.satisfied) then
          if use_pair_filter then
            -- Symmetric direct hit from the terminal side; swap root/terminal
            -- columns back into the function's result shape.
            insert into pg_temp.bsp_resolved_pairs (root_id, next_id, depth, satisfied, is_cycle, path)
            select distinct on (r.next_id, r.root_id) r.next_id,
                                                      r.root_id,
                                                      r.depth,
                                                      r.satisfied,
                                                      r.is_cycle,
                                                      r.path
            from pg_temp.bsp_backward_front r
                   join pg_temp.bsp_unresolved_pairs p on p.root_id = r.next_id and p.terminal_id = r.root_id
            where r.satisfied
            order by r.next_id, r.root_id, r.depth
            on conflict on constraint bsp_resolved_pairs_pkey do nothing;
            get diagnostics matched_count = row_count;
            resolved_pairs_count = resolved_pairs_count + matched_count;

            delete
            from pg_temp.bsp_unresolved_pairs p
              using pg_temp.bsp_resolved_pairs r
            where p.root_id = r.root_id
              and p.terminal_id = r.next_id;
            select exists(select 1 from pg_temp.bsp_unresolved_pairs) into unresolved_pairs_remaining;

            delete from pg_temp.bsp_backward_front f where not exists(select 1 from pg_temp.bsp_unresolved_pairs p where p.terminal_id = f.root_id);
            get diagnostics deleted_count = row_count;
            backward_front_count = backward_front_count - deleted_count;

            delete from pg_temp.bsp_forward_front f where not exists(select 1 from pg_temp.bsp_unresolved_pairs p where p.root_id = f.root_id);
            get diagnostics deleted_count = row_count;
            forward_front_count = forward_front_count - deleted_count;
          else
            return query select distinct on (r.next_id, r.root_id) r.next_id,
                                                                    r.root_id,
                                                                    r.depth,
                                                                    r.satisfied,
                                                                    r.is_cycle,
                                                                    r.path
                         from pg_temp.bsp_backward_front r
                         where r.satisfied
                         order by r.next_id, r.root_id, r.depth
                         limit case when path_limit > 0 then path_limit else null end;
            exit;
          end if;
        end if;
      end if;

      if use_pair_filter then
        -- For unresolved pairs that meet in the middle, keep one shortest
        -- stitched path per pair and leave already-resolved pairs untouched.
        insert into pg_temp.bsp_resolved_pairs (root_id, next_id, depth, satisfied, is_cycle, path)
        select p.root_id,
               p.terminal_id,
               midpoint.depth,
               true,
               false,
               midpoint.path
        from pg_temp.bsp_unresolved_pairs p
               join lateral (
          select f.depth + b.depth as depth,
                 f.path || b.path as path
          from pg_temp.bsp_forward_front f
                 join pg_temp.bsp_backward_front b on b.root_id = p.terminal_id and b.next_id = f.next_id
          where f.root_id = p.root_id
          order by f.depth + b.depth
          limit 1
          ) midpoint on true
        on conflict on constraint bsp_resolved_pairs_pkey do nothing;
        get diagnostics matched_count = row_count;
        resolved_pairs_count = resolved_pairs_count + matched_count;

        if matched_count > 0 then
          delete
          from pg_temp.bsp_unresolved_pairs p
            using pg_temp.bsp_resolved_pairs r
          where p.root_id = r.root_id
            and p.terminal_id = r.next_id;
          select exists(select 1 from pg_temp.bsp_unresolved_pairs) into unresolved_pairs_remaining;

          delete from pg_temp.bsp_forward_front f where not exists(select 1 from pg_temp.bsp_unresolved_pairs p where p.root_id = f.root_id);
          get diagnostics deleted_count = row_count;
          forward_front_count = forward_front_count - deleted_count;

          delete from pg_temp.bsp_backward_front b where not exists(select 1 from pg_temp.bsp_unresolved_pairs p where p.terminal_id = b.root_id);
          get diagnostics deleted_count = row_count;
          backward_front_count = backward_front_count - deleted_count;
        end if;
      else
        return query select distinct on (f.root_id, b.root_id) f.root_id,
                                                               b.root_id,
                                                               f.depth + b.depth,
                                                               true,
                                                               false,
                                                               f.path || b.path
                     from pg_temp.bsp_forward_front f
                            join pg_temp.bsp_backward_front b on f.next_id = b.next_id
                     order by f.root_id, b.root_id, f.depth + b.depth
                     limit case when path_limit > 0 then path_limit else null end;
        get diagnostics matched_count = row_count;

        if matched_count > 0 then
          exit;
        end if;
      end if;
    end loop;

  if use_pair_filter then
    -- Pair mode accumulates results during expansion so it can keep searching
    -- for unresolved pairs after the first frontier-level success.
    if path_limit > 0 then
      return query select *
                   from pg_temp.bsp_resolved_pairs
                   order by root_id, next_id, depth
                   limit path_limit;
    else
      return query select *
                   from pg_temp.bsp_resolved_pairs
                   order by root_id, next_id, depth;
    end if;
  end if;

  return;
end;
$$
  language plpgsql volatile
                   strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_ids int8[], terminal_ids int8[], path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, ''::text, ''::text, ''::text, root_ids, terminal_ids, path_limit, false, true);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_ids int8[], terminal_ids int8[],
                                                           allow_zero_depth bool, path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, ''::text, ''::text, ''::text, root_ids, terminal_ids, path_limit, allow_zero_depth, true);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_ids int8[], terminal_ids int8[],
                                                           allow_zero_depth bool)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_ids, terminal_ids, allow_zero_depth, 0::int8);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_ids int8[], terminal_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_ids, terminal_ids, 0::int8);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text, path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, ''::text, array []::int8[], array []::int8[], path_limit, false, false);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text,
                                                           allow_zero_depth bool, path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, ''::text, array []::int8[], array []::int8[], path_limit, allow_zero_depth, false);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text,
                                                           allow_zero_depth bool)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, allow_zero_depth, 0::int8);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, 0::int8);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text, pair_filter text,
                                                           path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, pair_filter, array []::int8[], array []::int8[], path_limit, false, false);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text, pair_filter text,
                                                           allow_zero_depth bool, path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, pair_filter, array []::int8[], array []::int8[], path_limit, allow_zero_depth, false);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text, pair_filter text,
                                                           allow_zero_depth bool)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, pair_filter, allow_zero_depth, 0::int8);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_filter text, terminal_filter text, pair_filter text)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, pair_filter, 0::int8);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_ids int8[], path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_ids, array []::int8[], path_limit);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, array []::int8[], array []::int8[]);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           path_limit int8)
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, array []::int8[], array []::int8[], path_limit);
$$
  language sql volatile
               strict;

create or replace function public.bidirectional_sp_harness(forward_primer text, forward_recursive text,
                                                           backward_primer text,
                                                           backward_recursive text, max_depth int4,
                                                           root_ids int8[])
  returns table
          (
            root_id   int8,
            next_id   int8,
            depth     int4,
            satisfied bool,
            is_cycle  bool,
            path      int8[]
          )
as
$$
select *
from public.bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_ids, 0::int8);
$$
  language sql volatile
               strict;

-- graphbench_s1_distance_bfs is the typed, array-resident SP-S1 distance
-- prototype. It is additive and benchmark-only: production translation does
-- not call it. The caller must transparently restart a correct fallback when
-- overflow is true.
create or replace function public.graphbench_s1_distance_bfs(target_graph_id int4, start_id int8, terminal_id int8,
                                                              min_depth int4, max_depth int4, edge_kind_ids int2[],
                                                              inbound bool, state_limit int4)
  returns table
          (
            depth          int4,
            matched        bool,
            overflow       bool,
            examined_edges int8,
            retained_nodes int4
          )
as
$$
#variable_conflict use_variable
declare
  current_depth  int4 := 0;
  frontier       int8[] := array[start_id]::int8[];
  next_frontier  int8[];
  visited        int8[] := array[start_id]::int8[];
  edge_count     int8;
begin
  depth := null;
  matched := false;
  overflow := false;
  examined_edges := 0;
  retained_nodes := 1;

  if state_limit < 1 then
    overflow := true;
    return next;
    return;
  end if;

  if start_id = terminal_id and min_depth = 0 then
    depth := 0;
    matched := true;
    return next;
    return;
  end if;

  while current_depth < max_depth and cardinality(frontier) > 0 loop
    select
      coalesce(array_agg(distinct candidate.next_id order by candidate.next_id)
        filter (where not candidate.next_id = any(visited)), array[]::int8[]),
      count(*)
    into next_frontier, edge_count
    from (
      select case when inbound then edge.start_id else edge.end_id end as next_id
      from unnest(frontier) as active(node_id)
      join edge on edge.graph_id = target_graph_id
        and ((not inbound and edge.start_id = active.node_id)
          or (inbound and edge.end_id = active.node_id))
      where cardinality(edge_kind_ids) = 0 or edge.kind_id = any(edge_kind_ids)
    ) candidate;

    examined_edges := examined_edges + edge_count;
    current_depth := current_depth + 1;

    if terminal_id = any(next_frontier) and current_depth >= min_depth then
      depth := current_depth;
      matched := true;
      retained_nodes := cardinality(visited) + cardinality(next_frontier);
      return next;
      return;
    end if;

    if cardinality(visited) + cardinality(next_frontier) > state_limit then
      overflow := true;
      retained_nodes := cardinality(visited);
      return next;
      return;
    end if;

    visited := visited || next_frontier;
    frontier := next_frontier;
    retained_nodes := cardinality(visited);
  end loop;

  return next;
end;
$$
  language plpgsql
  volatile
  strict;
