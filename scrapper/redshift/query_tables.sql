with svv_all_tables as ((((SELECT current_database()::character varying(128)                     AS database_name,
                                  pns.nspname::character varying(128)                            AS schema_name,
                                  pgc.relname::character varying(128)                            AS table_name,
                                  CASE
                                      WHEN pns.nspname ~~ like_escape('pg!_temp!_%'::text, '!'::text)
                                          THEN 'LOCAL TEMPORARY'::text
                                      WHEN pgc.relkind = 'r'::"char" THEN 'TABLE'::text
                                      WHEN pgc.relkind = 'v'::"char" THEN 'VIEW'::text
                                      ELSE NULL::text
                                      END::character varying                                     AS table_type,
                                  array_to_string(pgc.relacl, '~'::text)::character varying(128) AS table_acl,
                                  d.description::character varying                               AS remarks
                           FROM pg_namespace pns
                                    JOIN pg_class pgc ON pgc.relnamespace = pns.oid
                                    LEFT JOIN pg_description d ON pgc.oid = d.objoid AND d.objsubid = 0
                           WHERE (pgc.relkind = 'r'::"char" OR pgc.relkind = 'v'::"char")
                             AND pns.nspname <> 'catalog_history'::name
                             AND pns.nspname <> 'pg_toast'::name
                             AND pns.nspname <> 'pg_internal'::name
                           UNION ALL
                           SELECT btrim(rs_tables.database_name::text)::character varying(128) AS database_name,
                                  btrim(rs_tables.schema_name::text)::character varying(128)   AS schema_name,
                                  btrim(rs_tables.table_name::text)::character varying(128)    AS table_name,
                                  btrim(rs_tables.table_type::text)::character varying(128)    AS table_type,
                                  btrim(rs_tables.table_acl::text)::character varying(128)     AS table_acl,
                                  btrim(rs_tables.remarks::text)::character varying(128)       AS remarks
                           FROM pg_get_shared_redshift_tables() rs_tables(database_name character varying,
                                                                          schema_name character varying,
                                                                          table_name character varying,
                                                                          table_type character varying,
                                                                          table_acl character varying,
                                                                          remarks character varying))
                          UNION ALL
                          SELECT btrim(ext_tables.redshift_database_name::text)::character varying(128) AS database_name,
                                 btrim(ext_tables.schemaname::text)::character varying(128)             AS schema_name,
                                 btrim(ext_tables.tablename::text)::character varying(128)              AS table_name,
                                 'EXTERNAL TABLE'                                                       AS table_type,
                                 NULL::"unknown"                                                        AS table_acl,
                                 NULL::"unknown"                                                        AS remarks
                          FROM pg_get_external_tables() ext_tables(es_or_edb_oid integer,
                                                                   redshift_database_name character varying,
                                                                   schemaname character varying,
                                                                   tablename character varying,
                                                                   "location" character varying,
                                                                   input_format character varying,
                                                                   output_format character varying,
                                                                   serialization_lib character varying,
                                                                   serde_parameters character varying,
                                                                   compressed integer,
                                                                   "parameters" character varying,
                                                                   tabletype character varying))
                         UNION ALL
                         SELECT btrim(ext_tables.redshift_database_name::text)::character varying(128) AS database_name,
                                btrim(ext_tables.schemaname::text)::character varying(128)             AS schema_name,
                                btrim(ext_tables.tablename::text)::character varying(128)              AS table_name,
                                'EXTERNAL TABLE'                                                       AS table_type,
                                NULL::"unknown"                                                        AS table_acl,
                                NULL::"unknown"                                                        AS remarks
                         FROM pg_get_external_database_tables() ext_tables(es_or_edb_oid integer,
                                                                           redshift_database_name character varying,
                                                                           schemaname character varying,
                                                                           tablename character varying,
                                                                           "location" character varying,
                                                                           input_format character varying,
                                                                           output_format character varying,
                                                                           serialization_lib character varying,
                                                                           serde_parameters character varying,
                                                                           compressed integer,
                                                                           "parameters" character varying,
                                                                           tabletype character varying))
                        UNION ALL
                        SELECT btrim(ext_tables.databasename::text)::character varying(128) AS database_name,
                               btrim(ext_tables.schemaname::text)::character varying(128)   AS schema_name,
                               btrim(ext_tables.tablename::text)::character varying(128)    AS table_name,
                               'EXTERNAL TABLE'                                             AS table_type,
                               NULL::"unknown"                                              AS table_acl,
                               NULL::"unknown"                                              AS remarks
                        FROM pg_get_all_external_tables() ext_tables(databasename character varying,
                                                                     schemaname character varying,
                                                                     tablename character varying, esoid integer,
                                                                     "location" character varying,
                                                                     input_format character varying,
                                                                     output_format character varying,
                                                                     serialization_lib character varying,
                                                                     serde_parameters character varying,
                                                                     compressed integer, "parameters" character varying,
                                                                     tabletype character varying))
select distinct tbl.database_name                  as database,
                tbl.schema_name                    as schema,
                tbl.table_name                     as table,
                NVL2(tbl.remarks, tbl.remarks, '') as description,
                -- an external table is owned by whoever owns its external schema
                btrim(coalesce(owner.usename, ext_owner.usename)::text) as owner,
                pci.relcreationtime                as created_at
from svv_all_tables tbl
         left join pg_namespace pns
                   on tbl.table_type <> 'EXTERNAL TABLE' and pns.nspname = tbl.schema_name
         left join pg_class pgc
                   on pgc.relnamespace = pns.oid and pgc.relname = tbl.table_name and pgc.relkind in ('r', 'v')
         left join pg_user owner on owner.usesysid = pgc.relowner
         left join pg_class_info pci on pci.reloid = pgc.oid
         left join svv_external_schemas es
                   on tbl.table_type = 'EXTERNAL TABLE' and es.schemaname = tbl.schema_name
         left join pg_user ext_owner on ext_owner.usesysid = es.esowner
where tbl.schema_name not in ({{system_schemas}})
  -- a materialized view keeps its rows in mv_tbl__<view>__<n> in its own schema, owned by rdsdb (usesysid 1)
  and not (coalesce(pgc.relowner, 0) = 1 and left(tbl.table_name, 8) = 'mv_tbl__')
  and tbl.database_name = $1
  /* SYNQ_SCOPE_FILTER */
order by database, schema, "table"