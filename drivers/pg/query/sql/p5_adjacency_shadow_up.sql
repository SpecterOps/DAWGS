-- P5 experimental adjacency materialization. This file is intentionally not
-- part of schema_up.sql: callers must opt in for the feasibility study.

create table if not exists public.p5_adjacency_v1
(
  graph_id   integer  not null,
  direction  smallint not null check (direction in (-1, 1)),
  anchor_id  bigint   not null,
  neighbor_id bigint  not null,
  edge_id    bigint   not null,
  kind_id    smallint not null,

  primary key (graph_id, direction, edge_id),
  foreign key (graph_id) references graph (id) on delete cascade,
  foreign key (edge_id, graph_id) references edge (id, graph_id) on delete cascade
) partition by list (graph_id);

create index if not exists p5_adjacency_v1_lookup_index
  on public.p5_adjacency_v1 (graph_id, direction, anchor_id, kind_id, edge_id)
  include (neighbor_id);

do
$$
declare
  graph_row record;
begin
  for graph_row in select id from graph loop
    execute format(
      'create table if not exists %I partition of public.p5_adjacency_v1 for values in (%s)',
      'p5_adjacency_v1_' || graph_row.id,
      graph_row.id
    );
  end loop;
end
$$;

insert into public.p5_adjacency_v1 (graph_id, direction, anchor_id, neighbor_id, edge_id, kind_id)
select e.graph_id, 1, e.start_id, e.end_id, e.id, e.kind_id
from edge e
union all
select e.graph_id, -1, e.end_id, e.start_id, e.id, e.kind_id
from edge e
on conflict (graph_id, direction, edge_id) do update
  set anchor_id = excluded.anchor_id,
      neighbor_id = excluded.neighbor_id,
      kind_id = excluded.kind_id;

create or replace function public.maintain_p5_adjacency_v1() returns trigger as
$$
begin
  if tg_op = 'DELETE' or tg_op = 'UPDATE' then
    delete from public.p5_adjacency_v1
    where graph_id = old.graph_id
      and edge_id = old.id;
  end if;

  if tg_op = 'INSERT' or tg_op = 'UPDATE' then
    insert into public.p5_adjacency_v1 (graph_id, direction, anchor_id, neighbor_id, edge_id, kind_id)
    values
      (new.graph_id, 1, new.start_id, new.end_id, new.id, new.kind_id),
      (new.graph_id, -1, new.end_id, new.start_id, new.id, new.kind_id);
  end if;

  return null;
end
$$
  language plpgsql
  volatile;

drop trigger if exists p5_adjacency_v1_after_insert on edge;
create trigger p5_adjacency_v1_after_insert
  after insert
  on edge
  for each row
execute procedure public.maintain_p5_adjacency_v1();

drop trigger if exists p5_adjacency_v1_after_delete on edge;
create trigger p5_adjacency_v1_after_delete
  after delete
  on edge
  for each row
execute procedure public.maintain_p5_adjacency_v1();

drop trigger if exists p5_adjacency_v1_after_endpoint_update on edge;
create trigger p5_adjacency_v1_after_endpoint_update
  after update of graph_id, start_id, end_id, kind_id
  on edge
  for each row
execute procedure public.maintain_p5_adjacency_v1();
