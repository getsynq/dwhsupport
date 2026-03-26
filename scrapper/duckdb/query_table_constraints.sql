SELECT "schema", "table", "constraint_name", "column_name", "constraint_type", "column_position", "constraint_expression"
FROM (
    SELECT
        tc.table_schema AS "schema",
        tc.table_name AS "table",
        tc.constraint_name AS "constraint_name",
        kcu.column_name AS "column_name",
        tc.constraint_type AS "constraint_type",
        kcu.ordinal_position AS "column_position",
        '' AS "constraint_expression"
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_catalog = kcu.constraint_catalog
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')

    UNION ALL

    SELECT
        tc.constraint_schema AS "schema",
        tc.table_name AS "table",
        tc.constraint_name AS "constraint_name",
        '' AS "column_name",
        'CHECK' AS "constraint_type",
        0 AS "column_position",
        cc.check_clause AS "constraint_expression"
    FROM information_schema.table_constraints tc
    JOIN information_schema.check_constraints cc
        ON tc.constraint_catalog = cc.constraint_catalog
        AND tc.constraint_schema = cc.constraint_schema
        AND tc.constraint_name = cc.constraint_name
    WHERE tc.constraint_type = 'CHECK'
)
WHERE 1=1
  /* SYNQ_SCOPE_FILTER */
ORDER BY "schema", "table", "constraint_name", "column_position"
