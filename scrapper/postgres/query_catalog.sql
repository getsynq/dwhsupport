select current_database()                                  as "database",
       sch.nspname                                         as "schema",
       tbl.relname                                         as "table",
       (tbl.relkind = 'v' OR tbl.relkind = 'm')            as is_view,
       tbl_desc.description as table_comment,
       col.attname                                         as "column",
       col.attnum                                          as "position",
       pg_catalog.format_type(col.atttypid, col.atttypmod) as "type",
       col_desc.description                                as comment

from pg_catalog.pg_namespace sch
         join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
         join pg_catalog.pg_attribute col on col.attrelid = tbl.oid
         left outer join pg_catalog.pg_description tbl_desc on (tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0)
         left outer join pg_catalog.pg_description col_desc
                         on (col_desc.objoid = tbl.oid and col_desc.objsubid = col.attnum)
where not pg_is_other_temp_schema(sch.oid)     -- not a temporary schema belonging to another session
  and tbl.relpersistence in ('p', 'u')         -- [p]ermanent table or [u]nlogged table. Exclude [t]emporary tables
  and tbl.relkind in ('r', 'v', 'f', 'p', 'm') -- o[r]dinary table, [v]iew, [f]oreign table, [p]artitioned table, [m]aterialized view. Other values are [i]ndex, [S]equence, [c]omposite type, [t]OAST table
  and col.attnum > 0                           -- negative numbers are used for system columns such as oid
  and not col.attisdropped                     -- column as not been dropped
  AND sch.nspname not in ('pg_catalog', 'information_schema')
order by database, schema, "table", position
