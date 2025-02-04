with relations AS (select t.table_name
                        , t.database_name
                        , t.schema_name
                        , 'BASE TABLE' as table_type
                        , t.comment    as table_comment
                   from duckdb_tables() t
                   WHERE not temporary and not internal
                   UNION ALL
                   SELECT v.view_name as table_name
                        , v.database_name
                        , v.schema_name
                        , 'VIEW'      as table_type
                        , v.comment   as table_comment
                   from duckdb_views() v
                   where not temporary and not internal)
select r.database_name as "database",
       r.schema_name   as "schema",
       r.table_name    as "table",
       r.table_type    as "table_type",
       r.table_comment as description
FROM relations r
WHERE r.schema_name NOT IN ('information_schema')
  AND r.database_name NOT IN ('sample_data'
    , 'temp'
    , 'system', 'md_information_schema')
ORDER BY
    r.database_name,
    r.schema_name,
    r.table_name