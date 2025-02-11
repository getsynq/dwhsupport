with relations AS (select t.table_name
                        , t.database_name
                        , t.schema_name
                        , 0     as is_view
                        , t.sql as sql
                   from duckdb_tables() t
                   WHERE not temporary
                     and not internal
                   UNION ALL
                   SELECT v.view_name as table_name
                        , v.database_name
                        , v.schema_name
                        , 1           as is_view
                        , v.sql       as sql
                   from duckdb_views() v
                   where not temporary
                     and not internal)
select r.database_name as "database",
       r.schema_name   as "schema",
       r.table_name    as "table",
       r.is_view as "is_view",
       r.sql as "sql"
FROM relations r
WHERE r.schema_name NOT IN ('information_schema')
  AND r.database_name NOT IN ('sample_data', 'temp', 'system', 'md_information_schema')
and length(r.sql) > 0
ORDER BY r.database_name,
         r.schema_name,
         r.table_name