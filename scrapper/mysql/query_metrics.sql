select ''           as "database",
       TABLE_SCHEMA as "schema",
       TABLE_NAME   as "table",
       TABLE_ROWS   as "row_count",
       UPDATE_TIME  as "updated_at"
FROM information_schema.tables
WHERE TABLE_SCHEMA NOT IN ('sys', 'information_schema', 'mysql', 'performance_schema')
  AND (UPDATE_TIME IS NOT NULL OR TABLE_ROWS IS NOT NULL)