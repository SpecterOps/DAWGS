package main

const (
	// capturePostgresEnvironmentQuery captures server identity and benchmark session settings.
	capturePostgresEnvironmentQuery = `select version(), current_database(), current_setting('plan_cache_mode'), current_setting('transaction_isolation'), current_setting('work_mem'), current_setting('temp_file_limit'), (select count(*) from graph), pg_postmaster_start_time(), (select oid::int8 from pg_database where datname = current_database()), current_setting('autovacuum')`

	// capturePostgresRelationSizesQuery captures fixture relation sizes and analyze timestamps.
	capturePostgresRelationSizesQuery = `select pg_total_relation_size(format('node_%s', $1::int4)::regclass), pg_total_relation_size(format('edge_%s', $1::int4)::regclass), coalesce((select string_agg(relname || ':' || coalesce(last_analyze::text, 'never'), ',' order by relname) from pg_stat_all_tables where relname in (format('node_%s', $1::int4), format('edge_%s', $1::int4))), '')`

	// matchPostgresAnchorKindQuery checks the declared kind on one resolved physical anchor.
	matchPostgresAnchorKindQuery = `select exists(select 1 from node n join kind k on k.id = any(n.kind_ids) where n.graph_id = $1 and n.id = $2 and k.name = $3)`

	// capturePostgresStorageQuery measures graph node and edge relation sizes.
	capturePostgresStorageQuery = `select pg_total_relation_size(format('node_%s', $1::int4)::regclass), pg_total_relation_size(format('edge_%s', $1::int4)::regclass)`

	// measurePostgresWorkspaceQuery totals temporary workspace while excluding diagnostic relations and their indexes.
	measurePostgresWorkspaceQuery = `
		select coalesce(sum(pg_total_relation_size(c.oid)), 0)::int8
		from pg_class c
		where c.relnamespace = pg_my_temp_schema()
		  and c.relname <> 'traversal_runtime_attestation_v1'
		  and c.relname not like '%telemetry%'
		  and not exists (
		    select 1
		    from pg_index i
		    join pg_class indexed on indexed.oid = i.indrelid
		    where i.indexrelid = c.oid
		      and indexed.relnamespace = pg_my_temp_schema()
		      and (indexed.relname = 'traversal_runtime_attestation_v1' or indexed.relname like '%telemetry%')
		  )
	`
)
