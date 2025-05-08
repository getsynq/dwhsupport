SELECT 
    table_catalog as database,
    table_schema as schema,
    table_name as "table",
    table_type,
    case when table_type = 'BASE TABLE' then true else false end as is_table,
    case when table_type = 'VIEW' then true else false end as is_view
FROM {{catalog}}.information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'sys')

UNION ALL

SELECT 
    table_catalog as database,
    table_schema as schema,
    table_name as "table",
    'VIEW' AS table_type,
    false as is_table,
    true as is_view
FROM {{catalog}}.information_schema.views
WHERE table_schema NOT IN ('information_schema', 'sys')
