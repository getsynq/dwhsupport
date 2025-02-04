select
    current_database() as "database",
    sch.nspname as "schema",
    tbl.relname as "table",
    (CASE WHEN tbl.relpages = 0 THEN float8 '0'  -- empty table
          ELSE tbl.reltuples / tbl.relpages END
        * (pg_catalog.pg_relation_size(tbl.oid)
            / pg_catalog.current_setting('block_size')::int)
        )::bigint as "row_count",
    null::timestamp as "updated_at"
FROM
    pg_catalog.pg_namespace sch
        join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
WHERE
    not pg_is_other_temp_schema(sch.oid) -- not a temporary schema belonging to another session
  and tbl.relpersistence in ('p', 'u') -- [p]ermanent table or [u]nlogged table. Exclude [t]emporary tables
  and tbl.relkind in ('r', 'f', 'p', 'm') -- o[r]dinary table, [v]iew, [f]oreign table, [p]artitioned table, [m]aterialized view. Other values are [i]ndex, [S]equence, [c]omposite type, [t]OAST table
  AND sch.nspname NOT IN ('pg_catalog', 'information_schema')
  AND tbl.reltuples >= 0