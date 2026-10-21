-- A materialized view is a pg_class row of relkind 'v' on Redshift, like a
-- view, and pg_get_viewdef gives its whole CREATE MATERIALIZED VIEW. What tells
-- the two apart is that its rule reads its mv_tbl__<view>__<n> table, owned by
-- rdsdb (usesysid 1). SVV_MV_INFO also lists them, but it reads a system table,
-- which Redshift refuses to join with pg_get_viewdef.
WITH materialized_views AS (SELECT DISTINCT r.ev_class AS oid
                            FROM pg_rewrite r
                                     JOIN pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid
                                     JOIN pg_class t ON d.refclassid = 'pg_class'::regclass AND t.oid = d.refobjid
                            WHERE t.relowner = 1
                              AND left(t.relname, 8) = 'mv_tbl__'),
     views AS (SELECT current_database()::information_schema.sql_identifier                        AS table_catalog,
                      nc.nspname::information_schema.sql_identifier                                AS table_schema,
                      c.relname::information_schema.sql_identifier                                 AS table_name,
                      pg_get_viewdef(c.oid)                                                        AS view_definition,
                      mv.oid IS NOT NULL                                                           AS is_materialized_view,
                      'NONE'::information_schema.character_data::information_schema.character_data AS check_option,
                      NULL::information_schema.character_data::information_schema.character_data   AS is_updatable,
                      NULL::information_schema.character_data::information_schema.character_data   AS is_insertable_into
               FROM pg_namespace nc
                        JOIN pg_class c ON c.relnamespace = nc.oid
                        JOIN pg_user u ON u.usesysid = c.relowner
                        LEFT JOIN materialized_views mv ON mv.oid = c.oid
               WHERE c.relkind = 'v'::"char")
SELECT table_catalog                                                         as database,
       table_schema                                                          as schema,
       table_name                                                            as table,
       NOT is_materialized_view                                              as is_view,
       is_materialized_view                                                  as is_materialized_view,
       false                                                                 as is_table,
       CASE WHEN is_materialized_view THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END as table_type,
       view_definition                                                       as sql
FROM views
WHERE view_definition is not null
  AND table_schema not in ({{system_schemas}})
  AND table_catalog = $1
  /* SYNQ_SCOPE_FILTER */
