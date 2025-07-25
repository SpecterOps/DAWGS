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

-- Index on the graph ID of each node.
create index if not exists node_graph_id_index on node using btree (graph_id);

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

-- Index on the graph ID of each edge.
create index if not exists edge_graph_id_index on edge using btree (graph_id);

-- Index on the start vertex of each edge.
create index if not exists edge_start_id_index on edge using btree (start_id);

-- Index on the start vertex of each edge.
create index if not exists edge_end_id_index on edge using btree (end_id);

-- Index on the kind of each edge.
create index if not exists edge_kind_index on edge using btree (kind_id);

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
select row (array_agg(distinct (n.id, n.kind_ids, n.properties)::nodeComposite)::nodeComposite[],
         array_agg(distinct (r.id, r.start_id, r.end_id, r.kind_id, r.properties)::edgeComposite)::edgeComposite[])::pathComposite
from edge r
       join node n on n.id = r.start_id or n.id = r.end_id
where r.id = any (path);
$$
  language sql
  immutable
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

  create index forward_front_next_id_index on forward_front using btree (next_id);
  create index forward_front_satisfied_index on forward_front using btree (satisfied);
  create index forward_front_is_cycle_index on forward_front using btree (is_cycle);

  create index next_front_next_id_index on next_front using btree (next_id);
  create index next_front_satisfied_index on next_front using btree (satisfied);
  create index next_front_is_cycle_index on next_front using btree (is_cycle);
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

  create temporary table paths
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null,
    primary key (path)
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

  create temporary table backward_front
  (
    root_id   int8   not null,
    next_id   int8   not null,
    depth     int4   not null,
    satisfied bool,
    is_cycle  bool   not null,
    path      int8[] not null,
    primary key (path)
  ) on commit drop;

  create index backward_front_next_id_index on backward_front using btree (next_id);
  create index backward_front_satisfied_index on backward_front using btree (satisfied);
  create index backward_front_is_cycle_index on backward_front using btree (is_cycle);
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

  delete
  from forward_front r
  where r.is_cycle
     or r.satisfied is null
     or not r.satisfied and not exists(select 1 from edge e where e.end_id = r.next_id);

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

  delete
  from backward_front r
  where r.is_cycle
     or r.satisfied is null
     or not r.satisfied and not exists(select 1 from edge e where e.start_id = r.next_id);

  return;
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

      -- Swap the next_front table into the forward_front
      -- Remove cycles and non-conformant satisfaction checks
      delete from next_front f using visited v where f.is_cycle or f.satisfied is null or f.next_id = v.id;

      raise debug 'Expansion step % - Available Root Paths % - Num satisfied: %', forward_front_depth, (select count(*) from next_front), (select count(*) from next_front p where p.satisfied);

      -- Insert new newly visited nodes into the visited table
      insert into visited (id) select distinct on (f.next_id) f.next_id from next_front f on conflict (id) do nothing;

      -- Copy pathspace over into the next front
      truncate table forward_front;

      insert into forward_front
      select distinct on (f.next_id) f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
      from next_front f;

      -- Copy newly satisfied paths into the path table
      insert into paths
      select f.root_id, f.next_id, f.depth, f.satisfied, f.is_cycle, f.path
      from forward_front f
      where f.satisfied;

      -- Empty the next front last to capture the next expansion
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
begin
  raise debug 'unidirectional_asp_harness start';

  -- Defines two tables to represent pathspace of the recursive expansion
  perform create_unidirectional_pathspace_tables();

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

      raise debug 'Expansion step % - Available Root Paths % - Num satisfied: %', forward_front_depth, (select count(*) from next_front), (select count(*) from next_front p where p.satisfied);

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
begin
  raise debug 'bidirectional_asp_harness start';

  -- Defines three tables to represent pathspace of the recursive expansion
  perform create_bidirectional_pathspace_tables();

  while forward_front_depth + backward_front_depth < max_depth and
        (forward_front_depth = 0 or exists(select 1 from forward_front)) and
        (backward_front_depth = 0 or exists(select 1 from backward_front))
    loop
      -- Check to expand the smaller of the two frontiers, or if both are the same size prefer the forward frontier
      if (select count(*) from forward_front) <= (select count(*) from backward_front) then
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if forward_front_depth = 0 then
          execute forward_primer;
        else
          execute forward_recursive;
        end if;

        forward_front_depth = forward_front_depth + 1;

        raise debug 'Forward expansion as step % - Available Root Paths % - Num satisfied: %', forward_front_depth + backward_front_depth, (select count(*) from next_front), (select count(*) from next_front p where p.satisfied);

        -- Check to see if the next frontier is satisfied
        if exists(select 1 from next_front r where r.satisfied) then
          return query select * from next_front r where r.satisfied;
          exit;
        end if;

        -- Swap the next_front table into the forward_front
        perform swap_forward_front();
      else
        -- If this is the first expansion of this frontier, perform the primer query - otherwise perform the
        -- recursive expansion query
        if backward_front_depth = 0 then
          execute backward_primer;
        else
          execute backward_recursive;
        end if;

        backward_front_depth = backward_front_depth + 1;
        raise debug 'Backward expansion as step % - Available Terminal Paths % - Num satisfied: %', forward_front_depth + backward_front_depth, (select count(*) from next_front), (select count(*) from next_front p where p.satisfied);

        -- Check to see if the next frontier is satisfied
        if exists(select 1 from next_front r where r.satisfied) then
          return query select * from next_front r where r.satisfied;
          exit;
        end if;

        -- Swap the next_front table into the backward_front
        perform swap_backward_front();
      end if;

      -- Check to see if the two frontiers meet somewhere in the middle
      if exists(select 1
                from forward_front f
                       join backward_front b on f.next_id = b.next_id) then
        -- Zip the path arrays together treating the matches as satisfied
        return query select f.root_id,
                            b.root_id,
                            f.depth + b.depth,
                            true,
                            false,
                            f.path || b.path
                     from forward_front f
                            join backward_front b on f.next_id = b.next_id;
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
