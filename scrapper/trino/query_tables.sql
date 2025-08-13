SELECT 
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    {{table_type_expression}} AS "table_type",
    {{table_comment_expression}} as description,
    {{is_table_expression}} as is_table,
    {{is_view_expression}} as is_view
FROM {{catalog}}.information_schema.tables t
{{table_comments_join}}
{{materialized_views_join}}
WHERE t.table_schema NOT IN ('information_schema')