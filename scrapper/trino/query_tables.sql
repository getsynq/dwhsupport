SELECT 
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    t.table_type AS "table_type",
    '' as description,
    t.table_type = 'BASE TABLE' as is_table,
    t.table_type = 'VIEW'  as is_view
FROM {{catalog}}.information_schema.tables t
WHERE t.table_schema NOT IN ('information_schema')