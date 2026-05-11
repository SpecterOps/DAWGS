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
-- node partitions and therefore require the graph_id value of each node as part of the delete statement.
create or replace function delete_node_edges() returns trigger as
$$
begin
  delete from edge where start_id = OLD.id or end_id = OLD.id;
  return null;
end
$$
  language plpgsql
  volatile
  strict;

-- Drop and create the delete_node_edges trigger for the delete_node_edges() plpgsql function. See the function comment
-- for more information.
drop trigger if exists delete_node_edges on node;
create trigger delete_node_edges
  after delete
  on node
  for each row
execute procedure delete_node_edges();


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

-- Covering indexes for traversal joins. The INCLUDE columns allow index-only scans for the common case where
-- the join needs (id, start_id, end_id, kind_id) without fetching from the heap. The standalone start_id,
-- end_id, and kind_id indexes are intentionally omitted: the composite indexes satisfy left-prefix lookups
-- on start_id or end_id alone, and kind_id is never queried in isolation during traversal.
create index if not exists edge_start_id_kind_id_id_end_id_index on edge using btree (start_id, kind_id) include (id, end_id);
create index if not exists edge_end_id_kind_id_id_start_id_index on edge using btree (end_id, kind_id) include (id, start_id);

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

create or replace function public.nodes_to_path(nodes variadic int8[]) returns pathComposite as
$$
select row (array_agg(distinct (n.id, n.kind_ids, n.properties)::nodeComposite)::nodeComposite[],
         array []::edgeComposite[])::pathComposite
from node n
where n.id = any (nodes);
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.edges_to_path(path variadic int8[]) returns pathComposite as
$$
select row (
  (select array_agg(distinct (n.id, n.kind_ids, n.properties)::nodeComposite)
   from node n
   where n.id in (
     select start_id from edge where id = any(path)
     union
     select end_id from edge where id = any(path)
   )),
  (select array_agg(distinct (r.id, r.start_id, r.end_id, r.kind_id, r.properties)::edgeComposite)
   from edge r
   where r.id = any(path))
)::pathComposite;
$$
  language sql
  immutable
  parallel safe
  strict;

create or replace function public.ordered_edges_to_path(root nodeComposite, edges edgeComposite[], known_nodes nodeComposite[]) returns pathComposite as
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
    left join node n on n.id = ordered_node.id and known_node.node is null
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

create or replace function public.create_unidirectional_pathspace_tables()
  returns void as
$$
begin
  -- The path column is not used as a primary key. Deduplication is handled by DISTINCT ON clauses in the
  -- harness functions. Removing the PK on the variable-length int8[] array eliminates O(n)-key B-tree
  -- maintenance that grows with traversal depth.
  create temporary table forward_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit drop;

  create temporary table next_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit drop;

  create index forward_front_next_id_index on forward_front using btree (next_id);
  create index forward_front_satisfied_index on forward_front using btree (root_id, next_id, depth) where satisfied;
  create index forward_front_is_cycle_index on forward_front using btree (root_id, next_id) where is_cycle;

  create index next_front_next_id_index on next_front using btree (next_id);
  create index next_front_satisfied_index on next_front using btree (root_id, next_id, depth) where satisfied;
  create index next_front_is_cycle_index on next_front using btree (root_id, next_id) where is_cycle;
end;
$$
  language plpgsql
  volatile
  strict;


create or replace function public.create_unidirectional_shortest_path_tables()
  returns void as
$$
begin
  create temporary table visited
  (
    root_id int8 not null,
    id      int8 not null,
    primary key (root_id, id)
  ) on commit drop;

  create temporary table paths
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit drop;

  create temporary table resolved_roots
  (
    root_id int8 not null,
    primary key (root_id)
  ) on commit drop;

  perform create_unidirectional_pathspace_tables();

  create index forward_front_root_id_next_id_index on forward_front using btree (root_id, next_id);
  create index next_front_root_id_next_id_index on next_front using btree (root_id, next_id);
  create index paths_root_id_next_id_index on paths using btree (root_id, next_id);
end;
$$
  language plpgsql
  volatile
  strict;

-- create_traversal_filter_tables materializes the root, terminal and pair filter sets into temporary tables that the
-- harness functions join against. The tables use `on commit drop`, so a single transaction can only host one harness
-- invocation that depends on these tables; concurrent or sequential expansions in the same transaction will conflict
-- on the temporary table names.
create or replace function public.create_traversal_filter_tables()
  returns void as
$$
begin
  create temporary table if not exists traversal_root_filter
  (
    id int8 not null,
    primary key (id)
  ) on commit drop;

  create temporary table if not exists traversal_terminal_filter
  (
    id int8 not null,
    primary key (id)
  ) on commit drop;

  create temporary table if not exists traversal_pair_filter
  (
    root_id     int8 not null,
    terminal_id int8 not null,
    primary key (root_id, terminal_id)
  ) on commit drop;

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

create or replace function public.create_bidirectional_pathspace_tables()
  returns void as
$$
begin
  perform create_unidirectional_pathspace_tables();

  create temporary table backward_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit drop;

  create index backward_front_next_id_index on backward_front using btree (next_id);
  create index backward_front_satisfied_index on backward_front using btree (root_id, next_id, depth) where satisfied;
  create index backward_front_is_cycle_index on backward_front using btree (root_id, next_id) where is_cycle;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.create_bidirectional_pair_pathspace_indexes()
  returns void as
$$
begin
  create index forward_front_root_id_next_id_index on forward_front using btree (root_id, next_id);
  create index backward_front_root_id_next_id_index on backward_front using btree (root_id, next_id);
  create index next_front_root_id_next_id_index on next_front using btree (root_id, next_id);
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.create_bidirectional_shortest_path_tables()
  returns void as
$$
begin
  create temporary table forward_visited
  (
    root_id int8 not null,
    id      int8 not null,
    primary key (root_id, id)
  ) on commit drop;

  create temporary table backward_visited
  (
    root_id int8 not null,
    id      int8 not null,
    primary key (root_id, id)
  ) on commit drop;

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
  alter table forward_front
    rename to forward_front_old;
  alter table next_front
    rename to forward_front;
  alter table forward_front_old
    rename to next_front;

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
  alter table backward_front
    rename to backward_front_old;
  alter table next_front
    rename to backward_front;
  alter table backward_front_old
    rename to next_front;

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

  create temporary table unresolved_pairs
  (
    root_id     int8 not null,
    terminal_id int8 not null,
    primary key (root_id, terminal_id)
  ) on commit drop;

  create index unresolved_pairs_terminal_id_root_id_index on unresolved_pairs using btree (terminal_id, root_id);

  create temporary table resolved_pair_depths
  (
    root_id     int8 not null,
    terminal_id int8 not null,
    depth       int4 not null,
    primary key (root_id, terminal_id)
  ) on commit drop;

  create temporary table resolved_paths
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit drop;

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
begin
  raise debug 'bidirectional_sp_harness start';

  perform create_bidirectional_shortest_path_tables();

  if use_array_parameters then
    perform create_traversal_filter_tables(root_ids, terminal_ids);
  else
    perform create_traversal_filter_tables(root_filter, terminal_filter, pair_filter);
  end if;

  create temporary table unresolved_pairs
  (
    root_id     int8 not null,
    terminal_id int8 not null,
    primary key (root_id, terminal_id)
  ) on commit drop;

  create index unresolved_pairs_terminal_id_root_id_index on unresolved_pairs using btree (terminal_id, root_id);

  create temporary table resolved_pairs
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null,
    primary key (root_id, next_id)
  ) on commit drop;

  if use_pair_filter then
    insert into unresolved_pairs (root_id, terminal_id)
    select distinct root_id, terminal_id
    from traversal_pair_filter
    on conflict on constraint unresolved_pairs_pkey do nothing;
  end if;

  -- Pair-filter mode keeps expanding until each requested pair is resolved or
  -- the limit is met. Batch mode returns from inside the loop as soon as the
  -- current BFS depth produces results.
  while forward_front_depth + backward_front_depth < max_depth and
        (path_limit <= 0 or resolved_pairs_count < path_limit) and
        (not use_pair_filter or exists(select 1 from unresolved_pairs)) and
        (forward_front_depth = 0 or forward_front_count > 0) and
        (backward_front_depth = 0 or backward_front_count > 0)
    loop
      if forward_front_depth = 0 or (backward_front_depth > 0 and forward_front_count <= backward_front_count) then
        if forward_front_depth = 0 then
          if use_array_parameters then
            execute forward_primer using root_ids, terminal_ids;
          else
            execute forward_primer;
          end if;

          get diagnostics next_front_count = row_count;

          insert into forward_visited (root_id, id)
          select distinct f.root_id, f.root_id
          from next_front f
          on conflict on constraint forward_visited_pkey do nothing;
        else
          if use_array_parameters then
            execute forward_recursive using root_ids, terminal_ids;
          else
            execute forward_recursive;
          end if;

          get diagnostics next_front_count = row_count;
        end if;

        forward_front_depth = forward_front_depth + 1;

        delete from next_front f where f.is_cycle;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from next_front f where f.satisfied is null;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from next_front f using forward_visited v where f.root_id = v.root_id and f.next_id = v.id;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        raise debug 'Forward shortest expansion as step % - Available Root Paths %', forward_front_depth + backward_front_depth, next_front_count;

        truncate table forward_front;

        insert into forward_front
        select distinct on (f.root_id, f.next_id) f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from next_front f
        order by f.root_id, f.next_id, f.depth;
        get diagnostics forward_front_count = row_count;

        truncate table next_front;

        insert into forward_visited (root_id, id)
        select f.root_id, f.next_id
        from forward_front f
        on conflict on constraint forward_visited_pkey do nothing;

        if exists(select 1 from forward_front r where r.satisfied) then
          if use_pair_filter then
            -- A direct forward hit resolves only the requested pairs it satisfies.
            -- Frontiers for completed roots/terminals are pruned below.
            insert into resolved_pairs (root_id, next_id, depth, satisfied, is_cycle, path)
            select distinct on (r.root_id, r.next_id) r.root_id,
                                                      r.next_id,
                                                      r.depth,
                                                      r.satisfied,
                                                      r.is_cycle,
                                                      r.path
            from forward_front r
                   join unresolved_pairs p on p.root_id = r.root_id and p.terminal_id = r.next_id
            where r.satisfied
            order by r.root_id, r.next_id, r.depth
            on conflict on constraint resolved_pairs_pkey do nothing;
            get diagnostics matched_count = row_count;
            resolved_pairs_count = resolved_pairs_count + matched_count;

            delete
            from unresolved_pairs p
              using resolved_pairs r
            where p.root_id = r.root_id
              and p.terminal_id = r.next_id;

            delete from forward_front f where not exists(select 1 from unresolved_pairs p where p.root_id = f.root_id);
            get diagnostics deleted_count = row_count;
            forward_front_count = forward_front_count - deleted_count;

            delete from backward_front b where not exists(select 1 from unresolved_pairs p where p.terminal_id = b.root_id);
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
                         from forward_front r
                         where r.satisfied
                         order by r.root_id, r.next_id, r.depth
                         limit case when path_limit > 0 then path_limit else null end;
            exit;
          end if;
        end if;
      else
        if backward_front_depth = 0 then
          if use_array_parameters then
            execute backward_primer using root_ids, terminal_ids;
          else
            execute backward_primer;
          end if;

          get diagnostics next_front_count = row_count;

          insert into backward_visited (root_id, id)
          select distinct f.root_id, f.root_id
          from next_front f
          on conflict on constraint backward_visited_pkey do nothing;
        else
          if use_array_parameters then
            execute backward_recursive using root_ids, terminal_ids;
          else
            execute backward_recursive;
          end if;

          get diagnostics next_front_count = row_count;
        end if;

        backward_front_depth = backward_front_depth + 1;

        delete from next_front f where f.is_cycle;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from next_front f where f.satisfied is null;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        delete from next_front f using backward_visited v where f.root_id = v.root_id and f.next_id = v.id;
        get diagnostics deleted_count = row_count;
        next_front_count = next_front_count - deleted_count;

        raise debug 'Backward shortest expansion as step % - Available Terminal Paths %', forward_front_depth + backward_front_depth, next_front_count;

        truncate table backward_front;

        insert into backward_front
        select distinct on (f.root_id, f.next_id) f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
        from next_front f
        order by f.root_id, f.next_id, f.depth;
        get diagnostics backward_front_count = row_count;

        truncate table next_front;

        insert into backward_visited (root_id, id)
        select f.root_id, f.next_id
        from backward_front f
        on conflict on constraint backward_visited_pkey do nothing;

        if exists(select 1 from backward_front r where r.satisfied) then
          if use_pair_filter then
            -- Symmetric direct hit from the terminal side; swap root/terminal
            -- columns back into the function's result shape.
            insert into resolved_pairs (root_id, next_id, depth, satisfied, is_cycle, path)
            select distinct on (r.next_id, r.root_id) r.next_id,
                                                      r.root_id,
                                                      r.depth,
                                                      r.satisfied,
                                                      r.is_cycle,
                                                      r.path
            from backward_front r
                   join unresolved_pairs p on p.root_id = r.next_id and p.terminal_id = r.root_id
            where r.satisfied
            order by r.next_id, r.root_id, r.depth
            on conflict on constraint resolved_pairs_pkey do nothing;
            get diagnostics matched_count = row_count;
            resolved_pairs_count = resolved_pairs_count + matched_count;

            delete
            from unresolved_pairs p
              using resolved_pairs r
            where p.root_id = r.root_id
              and p.terminal_id = r.next_id;

            delete from backward_front f where not exists(select 1 from unresolved_pairs p where p.terminal_id = f.root_id);
            get diagnostics deleted_count = row_count;
            backward_front_count = backward_front_count - deleted_count;

            delete from forward_front f where not exists(select 1 from unresolved_pairs p where p.root_id = f.root_id);
            get diagnostics deleted_count = row_count;
            forward_front_count = forward_front_count - deleted_count;
          else
            return query select distinct on (r.next_id, r.root_id) r.next_id,
                                                                    r.root_id,
                                                                    r.depth,
                                                                    r.satisfied,
                                                                    r.is_cycle,
                                                                    r.path
                         from backward_front r
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
        insert into resolved_pairs (root_id, next_id, depth, satisfied, is_cycle, path)
        select p.root_id,
               p.terminal_id,
               midpoint.depth,
               true,
               false,
               midpoint.path
        from unresolved_pairs p
               join lateral (
          select f.depth + b.depth as depth,
                 f.path || b.path as path
          from forward_front f
                 join backward_front b on b.root_id = p.terminal_id and b.next_id = f.next_id
          where f.root_id = p.root_id
          order by f.depth + b.depth
          limit 1
          ) midpoint on true
        on conflict on constraint resolved_pairs_pkey do nothing;
        get diagnostics matched_count = row_count;
        resolved_pairs_count = resolved_pairs_count + matched_count;

        if matched_count > 0 then
          delete
          from unresolved_pairs p
            using resolved_pairs r
          where p.root_id = r.root_id
            and p.terminal_id = r.next_id;

          delete from forward_front f where not exists(select 1 from unresolved_pairs p where p.root_id = f.root_id);
          get diagnostics deleted_count = row_count;
          forward_front_count = forward_front_count - deleted_count;

          delete from backward_front b where not exists(select 1 from unresolved_pairs p where p.terminal_id = b.root_id);
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
                     from forward_front f
                            join backward_front b on f.next_id = b.next_id
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
                   from resolved_pairs
                   order by root_id, next_id, depth
                   limit path_limit;
    else
      return query select *
                   from resolved_pairs
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
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, ''::text, ''::text, ''::text, root_ids, terminal_ids, path_limit, true);
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
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, ''::text, array []::int8[], array []::int8[], path_limit, false);
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
from public._bidirectional_sp_harness(forward_primer, forward_recursive, backward_primer, backward_recursive, max_depth, root_filter, terminal_filter, pair_filter, array []::int8[], array []::int8[], path_limit, false);
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
