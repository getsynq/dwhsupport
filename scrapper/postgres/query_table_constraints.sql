SELECT
    c.table_schema AS "schema",
    c.table_name AS "table",
    tc.constraint_name AS "constraint_name",
    c.column_name AS "column_name",
    tc.constraint_type AS "constraint_type",
    c.ordinal_position::int AS "column_position"
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage c
    ON tc.constraint_name = c.constraint_name
    AND tc.constraint_schema = c.constraint_schema
    AND tc.constraint_catalog = c.constraint_catalog
WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY c.table_schema, c.table_name, tc.constraint_name, c.ordinal_position
