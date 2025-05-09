SELECT 
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    t.table_type,
    c.comment as description,
    case when t.table_type = 'BASE TABLE' then true else false end as is_table,
    case when t.table_type = 'VIEW' then true else false end as is_view
FROM {{catalog}}.information_schema.tables t
LEFT JOIN system.metadata.table_comments c
  ON t.table_catalog = c.catalog_name
  AND t.table_schema = c.schema_name
  AND t.table_name = c.table_name
WHERE t.table_schema NOT IN ('information_schema', 'sys', 'pg_catalog')