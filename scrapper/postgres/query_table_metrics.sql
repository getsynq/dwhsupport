-- Query to get table metrics including row counts and other metadata
-- This query excludes system tables and temporary tables
select current_database() as "database",  -- Get the current database name
       sch.nspname        as "schema",    -- Get the schema name
       tbl.relname        as "table",     -- Get the table name
       CASE
           -- Analyzed or vacuumed: the planner's estimate, tuples per page scaled to the current size.
           -- Preferred over n_live_tup, which the server loses on a crash or an immediate shutdown and
           -- then reports as 0 until enough changes come in.
           WHEN tbl.reltuples >= 0 AND tbl.relpages > 0 THEN
               round(tbl.reltuples / tbl.relpages *
                     (pg_catalog.pg_relation_size(tbl.oid) / pg_catalog.current_setting('block_size')::int))::bigint
           -- A table or materialized view without a heap holds no rows.
           WHEN tbl.relkind IN ('r', 'm') AND pg_catalog.pg_relation_size(tbl.oid) = 0 THEN 0
           -- A partitioned or foreign table has no heap of its own; ANALYZE sets its reltuples.
           WHEN tbl.reltuples >= 0 AND tbl.relkind IN ('p', 'f') THEN tbl.reltuples::bigint
           -- Never analyzed (reltuples is -1 since Postgres 14, 0 before): the cumulative statistics
           -- are all there is. Their 0 cannot be told from lost statistics, so it is reported as unknown.
           ELSE (SELECT NULLIF(n_live_tup, 0)::bigint
                 FROM pg_catalog.pg_stat_user_tables
                 WHERE relid = tbl.oid)
       END                as "row_count",
       null::timestamp    as "updated_at"  -- Placeholder for last update timestamp
FROM pg_catalog.pg_namespace sch
         join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
WHERE not pg_is_other_temp_schema(sch.oid) -- Exclude temporary schemas from other sessions
  and tbl.relpersistence in ('p', 'u')     -- Only include permanent and unlogged tables
  and tbl.relkind in ('r', 'f', 'p', 'm')  -- Include regular tables, foreign tables, partitioned tables, and materialized views
  AND sch.nspname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system schemas
  /* SYNQ_SCOPE_FILTER */
