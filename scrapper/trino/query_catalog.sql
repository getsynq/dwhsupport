SELECT
    t.table_catalog as database,
    t.table_schema as schema,
    t.table_name as "table",
    c.column_name as column,
    c.ordinal_position as position,
    c.data_type as type,
    c.comment as comment,
    '' as table_comment
FROM {{catalog}}.information_schema.tables t
JOIN {{catalog}}.information_schema.columns c
  ON t.table_catalog = c.table_catalog
  AND t.table_schema = c.table_schema
  AND t.table_name = c.table_name
WHERE t.table_schema NOT IN ('information_schema')