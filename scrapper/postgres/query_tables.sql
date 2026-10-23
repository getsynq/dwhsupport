select
    current_database() as "database",
    sch.nspname as "schema",
    tbl.relname as "table",
    case tbl.relkind
        when 'v' then 'VIEW'
        when 'm' then 'MATERIALIZED VIEW'
        else 'BASE TABLE'
        end as table_type,
    tbl_desc.description as description,
    fdw.fdwname as foreign_wrapper,
    srv.srvname as foreign_server,
    (select option_value from pg_catalog.pg_options_to_table(srv.srvoptions) where option_name = 'host') as foreign_host,
    (select option_value from pg_catalog.pg_options_to_table(srv.srvoptions) where option_name = 'port') as foreign_port,
    (select option_value from pg_catalog.pg_options_to_table(srv.srvoptions) where option_name = 'dbname') as foreign_database,
    (select option_value from pg_catalog.pg_options_to_table(ft.ftoptions) where option_name = 'schema_name') as foreign_schema,
    (select option_value from pg_catalog.pg_options_to_table(ft.ftoptions) where option_name = 'table_name') as foreign_table

from pg_catalog.pg_namespace sch
         join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
         left outer join pg_catalog.pg_description tbl_desc on (tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0)
         -- matches a [f]oreign table only: the server and wrapper it reads its rows through
         left outer join pg_catalog.pg_foreign_table ft on ft.ftrelid = tbl.oid
         left outer join pg_catalog.pg_foreign_server srv on srv.oid = ft.ftserver
         left outer join pg_catalog.pg_foreign_data_wrapper fdw on fdw.oid = srv.srvfdw
where
    not pg_is_other_temp_schema(sch.oid) -- not a temporary schema belonging to another session
  and tbl.relpersistence in ('p', 'u') -- [p]ermanent table or [u]nlogged table. Exclude [t]emporary tables
  and tbl.relkind in ('r', 'v', 'f', 'p', 'm') -- o[r]dinary table, [v]iew, [f]oreign table, [p]artitioned table, [m]aterialized view. Other values are [i]ndex, [S]equence, [c]omposite type, [t]OAST table
  AND
    sch.nspname not in ('pg_catalog', 'information_schema')
  /* SYNQ_SCOPE_FILTER */
order by
    database, schema, "table"