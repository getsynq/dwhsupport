SELECT
    tc.table_schema AS "schema",
    tc.table_name AS "table",
    tc.constraint_name AS "constraint_name",
    ccu.column_name AS "column_name",
    tc.constraint_type AS "constraint_type",
    ccu.ordinal_position AS "column_position"
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.constraint_schema = ccu.constraint_schema
    AND tc.constraint_catalog = ccu.constraint_catalog
WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, ccu.ordinal_position
