select
    current_database() as "database",
    sch.nspname as "schema",
    tbl.relname as "table",
    case tbl.relkind
        when 'v' then 'VIEW'
        when 'm' then 'MATERIALIZED VIEW'
        else 'BASE TABLE'
        end as table_type,
    tbl_desc.description as description

from pg_catalog.pg_namespace sch
         join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
         left outer join pg_catalog.pg_description tbl_desc on (tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0)
where
    not pg_is_other_temp_schema(sch.oid) -- not a temporary schema belonging to another session
  and tbl.relpersistence in ('p', 'u') -- [p]ermanent table or [u]nlogged table. Exclude [t]emporary tables
  and tbl.relkind in ('r', 'v', 'f', 'p', 'm') -- o[r]dinary table, [v]iew, [f]oreign table, [p]artitioned table, [m]aterialized view. Other values are [i]ndex, [S]equence, [c]omposite type, [t]OAST table
  AND
    sch.nspname not in ('pg_catalog', 'information_schema')
order by
    database, schema, "table"