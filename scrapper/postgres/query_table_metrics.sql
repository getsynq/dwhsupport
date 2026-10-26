-- Query to get table metrics including row counts and other metadata
-- This query excludes system tables and temporary tables
select current_database() as "database",  -- Get the current database name
       sch.nspname        as "schema",    -- Get the schema name
       tbl.relname        as "table",     -- Get the table name
       CASE
           -- Analyzed or vacuumed: n_live_tup while it agrees with the planner's estimate (tuples per
           -- page scaled to the current size), as it follows every insert and delete and the estimate
           -- moves only when the table gains a page or is analyzed again. The server loses n_live_tup
           -- on a crash or an immediate shutdown and counts again from 0, so one far from the estimate
           -- is not used and the estimate is the row count.
           WHEN tbl.reltuples >= 0 AND tbl.relpages > 0 THEN
               CASE
                   WHEN st.n_live_tup BETWEEN est.row_count / 2 AND est.row_count * 2 THEN st.n_live_tup
                   ELSE est.row_count
               END
           -- A table or materialized view without a heap holds no rows.
           WHEN tbl.relkind IN ('r', 'm') AND pg_catalog.pg_relation_size(tbl.oid) = 0 THEN 0
           -- A partitioned or foreign table has no heap of its own; ANALYZE sets its reltuples.
           WHEN tbl.reltuples >= 0 AND tbl.relkind IN ('p', 'f') THEN tbl.reltuples::bigint
           -- Never analyzed (reltuples is -1 since Postgres 14, 0 before): the cumulative statistics
           -- are all there is. Their 0 cannot be told from lost statistics, so it is reported as unknown.
           ELSE NULLIF(st.n_live_tup, 0)::bigint
       END                as "row_count",
       null::timestamp    as "updated_at"  -- Placeholder for last update timestamp
FROM pg_catalog.pg_namespace sch
         join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
         left join pg_catalog.pg_stat_user_tables st on st.relid = tbl.oid
         cross join lateral (
             SELECT CASE
                        WHEN tbl.reltuples >= 0 AND tbl.relpages > 0 THEN
                            round(tbl.reltuples / tbl.relpages *
                                  (pg_catalog.pg_relation_size(tbl.oid) / pg_catalog.current_setting('block_size')::int))::bigint
                    END AS row_count
             ) est
WHERE not pg_is_other_temp_schema(sch.oid) -- Exclude temporary schemas from other sessions
  and tbl.relpersistence in ('p', 'u')     -- Only include permanent and unlogged tables
  and tbl.relkind in ('r', 'f', 'p', 'm')  -- Include regular tables, foreign tables, partitioned tables, and materialized views
  AND sch.nspname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system schemas
  /* SYNQ_SCOPE_FILTER */
