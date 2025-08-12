SELECT 
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    {{table_type_expression}} AS "table_type",
    '' as description,
    {{table_type_expression}} = 'BASE TABLE' as is_table,
    {{table_type_expression}} = 'VIEW'  as is_view
FROM {{catalog}}.information_schema.tables t
{{materialized_views_join}}
WHERE t.table_schema NOT IN ('information_schema')