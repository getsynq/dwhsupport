SELECT
    tc.constraint_catalog AS "database",
    tc.constraint_schema AS "schema",
    tc.table_name AS "table",
    tc.constraint_name AS "constraint_name",
    ccu.column_name AS "column_name",
    tc.constraint_type AS "constraint_type",
    CAST(ccu.ordinal_position AS INTEGER) AS "column_position"
FROM {{catalog}}.information_schema.table_constraints tc
JOIN {{catalog}}.information_schema.key_column_usage ccu
    ON tc.constraint_catalog = ccu.constraint_catalog
    AND tc.constraint_schema = ccu.constraint_schema
    AND tc.constraint_name = ccu.constraint_name
WHERE tc.table_schema = ?
    AND tc.table_name = ?
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY tc.constraint_name, ccu.ordinal_position
