-- Drop triggers
drop trigger if exists delete_node_edges on node;
drop function if exists delete_node_edges;

-- Drop functions
drop function if exists query_perf;
drop function if exists lock_details;
drop function if exists table_sizes;
drop function if exists jsonb_to_text_array;
drop function if exists get_node;
drop function if exists node_prop;
drop function if exists kinds;
drop function if exists has_kind;
drop function if exists mt_get_root;
drop function if exists index_utilization;
drop function if exists _format_asp_where_clause;
drop function if exists _format_asp_query;
drop function if exists asp_harness;
drop function if exists _all_shortest_paths;
drop function if exists all_shortest_paths;
drop function if exists traversal_step;
drop function if exists _format_traversal_continuation_termination;
drop function if exists _format_traversal_query;
drop function if exists _format_traversal_initial_query;
drop function if exists expand_traversal_step;
drop function if exists traverse;
drop function if exists edges_to_path;
drop function if exists traverse_paths;

-- Drop all tables in order of dependency.
drop table if exists node;
drop table if exists edge;
drop table if exists kind;
drop table if exists graph;

-- Remove custom types
do
$$
  begin
    drop type pathComposite;
  exception
    when undefined_object then null;
  end
$$;

do
$$
  begin
    drop type nodeComposite;
  exception
    when undefined_object then null;
  end
$$;

do
$$
  begin
    drop type edgeComposite;
  exception
    when undefined_object then null;
  end
$$;

do
$$
  begin
    drop type _traversal_step;
  exception
    when undefined_object then null;
  end
$$;

-- Pull the tri-gram and intarray extensions.
drop
  extension if exists pg_trgm;
drop
  extension if exists intarray;
drop
  extension if exists pg_stat_statements;
