-- Query to get table metrics including row counts and other metadata
-- This query excludes system tables and temporary tables
select current_database() as "database",  -- Get the current database name
       sch.nspname        as "schema",    -- Get the schema name
       tbl.relname        as "table",     -- Get the table name
       COALESCE(
               -- First try to get the live tuple count from pg_stat_user_tables
               -- This is the most accurate count but might not be available for all tables
               (SELECT n_live_tup::bigint
                FROM pg_stat_user_tables
                WHERE relid = tbl.oid),
               -- If live tuple count is not available, estimate based on table size
               CASE
                   WHEN tbl.relpages = 0 THEN 0  -- If table has no pages, it's empty
                   ELSE (tbl.reltuples *          -- Use the number of tuples per page
                         (pg_catalog.pg_relation_size(tbl.oid) / pg_catalog.current_setting('block_size')::int))::bigint
                   END
       )                  as "row_count",  -- Final row count (either exact or estimated)
       null::timestamp    as "updated_at"  -- Placeholder for last update timestamp
FROM pg_catalog.pg_namespace sch
         join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
WHERE not pg_is_other_temp_schema(sch.oid) -- Exclude temporary schemas from other sessions
  and tbl.relpersistence in ('p', 'u')     -- Only include permanent and unlogged tables
  and tbl.relkind in ('r', 'f', 'p', 'm')  -- Include regular tables, foreign tables, partitioned tables, and materialized views
  AND sch.nspname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system schemas
  AND tbl.reltuples >= 0  -- Exclude tables with negative tuple counts (which indicate invalid statistics)