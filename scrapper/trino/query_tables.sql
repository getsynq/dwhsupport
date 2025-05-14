SELECT 
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    (case WHEN mv.name IS NOT NULL THEN 'MATERIALIZED VIEW'
    ELSE t.table_type END) AS "table_type",
    c.comment as description,
    mv.name is null AND t.table_type = 'BASE TABLE' as is_table,
    mv.name is not null OR t.table_type = 'VIEW'  as is_view
FROM {{catalog}}.information_schema.tables t
LEFT JOIN system.metadata.table_comments c
  ON t.table_catalog = c.catalog_name
  AND t.table_schema = c.schema_name
  AND t.table_name = c.table_name
LEFT JOIN system.metadata.materialized_views mv
  ON t.table_catalog = mv.catalog_name
    AND t.table_schema = mv.schema_name
    AND t.table_name = mv.name
WHERE t.table_schema NOT IN ('information_schema')