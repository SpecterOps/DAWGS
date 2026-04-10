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

  unique (graph_id, start_id, end_id, kind_id)
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

-- Remove the old graph ID index.
drop index if exists edge_graph_id_index;

-- Index on the start vertex of each edge.
create index if not exists edge_start_id_index on edge using btree (start_id);

-- Index on the start vertex of each edge.
create index if not exists edge_end_id_index on edge using btree (end_id);

-- Index on the kind of each edge.
create index if not exists edge_kind_index on edge using btree (kind_id);

-- Index lookups that include the edge's start or end id along with a filter for the edge type. This is the most
-- common join filter during traversal.
create index if not exists edge_start_kind_index on edge using btree (start_id, kind_id);
create index if not exists edge_end_kind_index on edge using btree (end_id, kind_id);

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
  else
    return array []::text[];
  end if;
end
$$
  language plpgsql
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
-- Read the edge table once via a CTE, then derive both the node set and edge set from it.
-- This replaces three separate scans of the edge table (two for start_id/end_id, one for
-- edge composites) with a single scan, reused by both subqueries below.
with e as (
  select id, start_id, end_id, kind_id, properties
  from edge
  where id = any(path)
)
select row(
  (select array_agg(distinct (n.id, n.kind_ids, n.properties)::nodeComposite)
   from node n
   where n.id in (select start_id from e union all select end_id from e)),
  (select array_agg(distinct (e.id, e.start_id, e.end_id, e.kind_id, e.properties)::edgeComposite)
   from e)
)::pathComposite;
$$
  language sql
  -- stable rather than immutable: this function reads from the edge and node tables,
  -- which can change between transactions.
  stable
  parallel safe
  strict;

create or replace function public.create_unidirectional_pathspace_tables()
  returns void as
$$
begin
  create temporary table forward_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null,
    primary key (path)
  ) on commit drop;

  create temporary table next_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null,
    primary key (path)
  ) on commit drop;

  -- next_id index supports join conditions in recursive expansion queries and the
  -- anti-join used in the swap helpers' dead-end pruning.
  create index forward_front_next_id_index on forward_front using btree (next_id);
  create index next_front_next_id_index on next_front using btree (next_id);
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
    id int8 not null,
    primary key (id)
  ) on commit drop;

  -- No primary key on path: maintaining a B-tree index on a variable-length int8[] array
  -- costs O(depth) per insert and grows with traversal depth. Deduplication is guaranteed
  -- upstream by the DISTINCT ON (next_id) applied before inserting into this table.
  create temporary table paths
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null
  ) on commit drop;

  perform create_unidirectional_pathspace_tables();
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

  -- No primary key on path: same rationale as the paths table above.
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
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.swap_forward_front()
  returns int4 as
$$
declare
  remaining int4;
begin
  alter table forward_front
    rename to forward_front_old;
  alter table next_front
    rename to forward_front;
  alter table forward_front_old
    rename to next_front;

  truncate table next_front;

  -- Remove dead-ends: unsatisfied frontier nodes that have no outgoing edges in the graph
  -- and therefore can never extend a forward path. The forward expansion step joins on
  -- e.start_id = frontier.next_id, so nodes without any outgoing edge (start_id) are
  -- guaranteed dead-ends. Uses the edge_start_id_kind_id_id_end_id_index covering index.
  delete from forward_front r
  where not r.satisfied
    and not exists (select 1 from edge e where e.start_id = r.next_id);

  -- Return the surviving frontier size so callers can cache it without a separate COUNT(*).
  select count(*) into remaining from forward_front;
  return remaining;
end;
$$
  language plpgsql
  volatile
  strict;

create or replace function public.swap_backward_front()
  returns int4 as
$$
declare
  remaining int4;
begin
  alter table backward_front
    rename to backward_front_old;
  alter table next_front
    rename to backward_front;
  alter table backward_front_old
    rename to next_front;

  truncate table next_front;

  -- Remove dead-ends: unsatisfied frontier nodes that have no incoming edges in the graph
  -- and therefore can never extend a backward path. The backward expansion step joins on
  -- e.end_id = frontier.next_id, so nodes without any incoming edge (end_id) are
  -- guaranteed dead-ends. Uses the edge_end_id_kind_id_id_start_id_index covering index.
  delete from backward_front r
  where not r.satisfied
    and not exists (select 1 from edge e where e.end_id = r.next_id);

  -- Return the surviving frontier size so callers can cache it without a separate COUNT(*).
  select count(*) into remaining from backward_front;
  return remaining;
end;
$$
  language plpgsql
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
declare
  forward_front_depth int4 := 0;
begin
  raise debug 'Shortest Path Harness starting';

  -- Create all tables necessary to drive traversal
  perform create_unidirectional_shortest_path_tables();

  while forward_front_depth < max_depth and (forward_front_depth = 0 or exists(select 1 from forward_front))
    loop
      if forward_front_depth = 0 then
        execute forward_primer;

        -- Insert all root nodes as visited
        insert into visited (id) select distinct f.root_id from next_front f on conflict (id) do nothing;
      else
        execute forward_recursive;
      end if;

      forward_front_depth = forward_front_depth + 1;

      -- Remove cycles, non-conformant satisfaction checks, and already-visited nodes.
      delete from next_front f where f.is_cycle or f.satisfied is null;
      delete from next_front f using visited v where f.next_id = v.id;

      -- Avoid unconditional COUNT(*) subqueries in raise debug: they are always evaluated
      -- by PostgreSQL regardless of the effective log level, costing two full table scans
      -- per iteration on every production query.
      raise debug 'Expansion step %', forward_front_depth;

      -- Mark all frontier nodes as visited. Use SELECT DISTINCT rather than DISTINCT ON
      -- since we only need the id column here.
      insert into visited (id) select distinct f.next_id from next_front f on conflict (id) do nothing;

      -- Single-pass split: materialize the DISTINCT ON result once via a writable CTE,
      -- then route satisfied rows to paths and unsatisfied rows to forward_front. This
      -- replaces two separate sequential scans of next_front with one. When a next_id has
      -- both satisfied and unsatisfied rows (same node, different paths) the satisfied row
      -- is preferred so the solution is recorded and expansion stops for that node.
      truncate table forward_front;

      with deduped as (
        select distinct on (next_front.next_id)
          next_front.root_id, next_front.next_id, next_front.depth,
          next_front.satisfied, next_front.is_cycle, next_front.path
        from next_front
        order by next_front.next_id, next_front.satisfied desc
      ),
      ins_paths as (
        insert into paths select * from deduped where deduped.satisfied
      )
      insert into forward_front select * from deduped where not deduped.satisfied;

      -- Empty the next front to prepare for the next expansion step.
      truncate table next_front;
    end loop;

  return query select * from paths;

  -- This bare return is not an error. This closes this function's resultset, and the return above will
  -- be treated as a yield and continue execution once the result cursor is exhausted.
  return;
end;
$$
  language plpgsql
  volatile
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
declare
  forward_front_depth int4 := 0;
  row_count           int4;
begin
  raise debug 'unidirectional_asp_harness start';

  -- Defines two tables to represent pathspace of the recursive expansion
  perform create_unidirectional_pathspace_tables();

  -- Visited table tracks nodes already reached at a shallower depth so that longer
  -- (non-shortest) paths are pruned from the frontier before expansion. Without this,
  -- the same node can be re-expanded at every depth level, causing the frontier to
  -- grow exponentially in dense graphs.
  create temporary table asp_visited
  (
    id int8 not null,
    primary key (id)
  ) on commit drop;

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

      -- Prune cycles, null satisfaction, and nodes already reached at a shallower depth
      -- in a single pass. The visited-set check prevents exponential frontier growth by
      -- ensuring each node is only expanded at the first (shortest) depth it is discovered.
      delete from next_front f
      where f.is_cycle or f.satisfied is null
         or f.next_id in (select id from asp_visited);

      -- Return all satisfied paths from the next front. If any rows were returned,
      -- we are done — exit the loop.
      return query select * from next_front r where r.satisfied;
      GET DIAGNOSTICS row_count = ROW_COUNT;
      if row_count > 0 then
        exit;
      end if;

      -- Mark all current frontier nodes as visited before swapping.
      insert into asp_visited (id) select distinct f.next_id from next_front f on conflict (id) do nothing;

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
declare
  forward_front_depth  int4 := 0;
  backward_front_depth int4 := 0;
  -- Cached row counts for each frontier, now returned directly by the swap functions
  -- instead of requiring a separate COUNT(*) full table scan after each swap. Both
  -- start at 0 so the forward frontier is always expanded first (0 <= 0), which
  -- matches the original preference when frontiers are equal in size.
  forward_front_size   int4 := 0;
  backward_front_size  int4 := 0;
  row_count            int4;
begin
  raise debug 'bidirectional_asp_harness start';

  -- Defines three tables to represent pathspace of the recursive expansion
  perform create_bidirectional_pathspace_tables();

  -- Per-direction visited tables prevent re-expansion of nodes already reached at a
  -- shallower depth, avoiding exponential frontier growth in dense graphs.
  create temporary table forward_asp_visited
  (
    id int8 not null,
    primary key (id)
  ) on commit drop;

  create temporary table backward_asp_visited
  (
    id int8 not null,
    primary key (id)
  ) on commit drop;

  while forward_front_depth + backward_front_depth < max_depth and
        (forward_front_depth = 0 or exists(select 1 from forward_front)) and
        (backward_front_depth = 0 or exists(select 1 from backward_front))
    loop
      -- Expand the smaller frontier; prefer forward when sizes are equal. Sizes are
      -- maintained by the counters returned from the swap functions.
      if forward_front_size <= backward_front_size then
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if forward_front_depth = 0 then
          execute forward_primer;
        else
          execute forward_recursive;
        end if;

        forward_front_depth = forward_front_depth + 1;

        raise debug 'Forward expansion step %', forward_front_depth + backward_front_depth;

        -- Prune cycles, null satisfaction, and nodes already visited in the forward direction
        -- in a single pass.
        delete from next_front f
        where f.is_cycle or f.satisfied is null
           or f.next_id in (select id from forward_asp_visited);

        -- Return all satisfied paths. If any rows were returned, we are done.
        return query select * from next_front r where r.satisfied;
        GET DIAGNOSTICS row_count = ROW_COUNT;
        if row_count > 0 then
          exit;
        end if;

        -- Mark forward frontier nodes as visited before swapping.
        insert into forward_asp_visited (id) select distinct f.next_id from next_front f on conflict (id) do nothing;

        -- Swap the next_front table into the forward_front. The swap function returns
        -- the new frontier size, eliminating a separate COUNT(*) scan.
        select swap_forward_front() into forward_front_size;
      else
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if backward_front_depth = 0 then
          execute backward_primer;
        else
          execute backward_recursive;
        end if;

        backward_front_depth = backward_front_depth + 1;
        raise debug 'Backward expansion step %', forward_front_depth + backward_front_depth;

        -- Prune cycles, null satisfaction, and nodes already visited in the backward direction
        -- in a single pass.
        delete from next_front f
        where f.is_cycle or f.satisfied is null
           or f.next_id in (select id from backward_asp_visited);

        -- Return all satisfied paths. If any rows were returned, we are done.
        return query select * from next_front r where r.satisfied;
        GET DIAGNOSTICS row_count = ROW_COUNT;
        if row_count > 0 then
          exit;
        end if;

        -- Mark backward frontier nodes as visited before swapping.
        insert into backward_asp_visited (id) select distinct f.next_id from next_front f on conflict (id) do nothing;

        -- Swap the next_front table into the backward_front. The swap function returns
        -- the new frontier size, eliminating a separate COUNT(*) scan.
        select swap_backward_front() into backward_front_size;
      end if;

      -- Check to see if the two frontiers meet somewhere in the middle.
      -- Skip when one frontier has not expanded yet since the join would find nothing.
      if forward_front_depth > 0 and backward_front_depth > 0 then
        -- Zip the path arrays together treating the matches as satisfied.
        -- The RETURN QUERY + GET DIAGNOSTICS pattern avoids a separate EXISTS scan.
        return query select f.root_id,
                            b.root_id,
                            f.depth + b.depth,
                            true,
                            false,
                            f.path || b.path
                     from forward_front f
                            join backward_front b on f.next_id = b.next_id;
        GET DIAGNOSTICS row_count = ROW_COUNT;
        if row_count > 0 then
          exit;
        end if;
      end if;
    end loop;

  -- This bare return is not an error. This closes this function's result set and the return above will
  -- be treated as a yield and continue execution once the results cursor is exhausted.
  return;
end;
$$
  language plpgsql volatile
                   strict;
