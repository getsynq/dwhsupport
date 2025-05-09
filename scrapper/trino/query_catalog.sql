SELECT
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    (t.table_type = 'VIEW') as is_view,
    c.column_name as column,
    c.ordinal_position as position,
    c.data_type as type,
    c.comment as comment,
    tc.comment as table_comment
FROM {{catalog}}.information_schema.tables t
LEFT JOIN {{catalog}}.information_schema.columns c
  ON t.table_catalog = c.table_catalog
  AND t.table_schema = c.table_schema
  AND t.table_name = c.table_name
LEFT JOIN system.metadata.table_comments tc
  ON t.table_catalog = tc.catalog_name
  AND t.table_schema = tc.schema_name
  AND t.table_name = tc.table_name
WHERE t.table_schema NOT IN ('information_schema', 'sys', 'pg_catalog')