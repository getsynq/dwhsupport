WITH views AS (SELECT current_database()::information_schema.sql_identifier                        AS table_catalog,
                      nc.nspname::information_schema.sql_identifier                                AS table_schema,
                      c.relname::information_schema.sql_identifier                                 AS table_name,
                      pg_get_viewdef(c.oid)                                                        AS view_definition,
                      'NONE'::information_schema.character_data::information_schema.character_data AS check_option,
                      NULL::information_schema.character_data::information_schema.character_data   AS is_updatable,
                      NULL::information_schema.character_data::information_schema.character_data   AS is_insertable_into
               FROM pg_namespace nc,
                    pg_class c,
                    pg_user u
               WHERE c.relnamespace = nc.oid
                 AND u.usesysid = c.relowner
                 AND c.relkind = 'v'::"char")
SELECT table_catalog   as database,
       table_schema    as schema,
       table_name      as table,
       view_definition as sql
FROM views
WHERE view_definition is not null
  AND table_schema not in ('pg_catalog', 'information_schema')
  AND table_catalog = $1