SELECT
    tc.table_schema AS "schema",
    tc.table_name AS "table",
    tc.constraint_name AS "constraint_name",
    kcu.column_name AS "column_name",
    tc.constraint_type AS "constraint_type",
    kcu.ordinal_position AS "column_position"
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.constraint_schema = kcu.constraint_schema
    AND tc.constraint_catalog = kcu.constraint_catalog
WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
