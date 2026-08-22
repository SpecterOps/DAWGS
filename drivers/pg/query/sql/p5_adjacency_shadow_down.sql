-- Remove only the opt-in P5 feasibility schema. Core graph storage remains.

drop trigger if exists p5_adjacency_v1_after_insert on edge;
drop trigger if exists p5_adjacency_v1_after_delete on edge;
drop trigger if exists p5_adjacency_v1_after_endpoint_update on edge;
drop function if exists public.maintain_p5_adjacency_v1();
drop table if exists public.p5_adjacency_v1;
