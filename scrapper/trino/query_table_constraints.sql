SELECT "database", "schema", "table", "constraint_name", "column_name", "constraint_type", "column_position", "constraint_expression", "is_enforced"
FROM (
    SELECT
        tc.constraint_catalog AS "database",
        tc.constraint_schema AS "schema",
        tc.table_name AS "table",
        tc.constraint_name AS "constraint_name",
        ccu.column_name AS "column_name",
        tc.constraint_type AS "constraint_type",
        CAST(ccu.ordinal_position AS INTEGER) AS "column_position",
        '' AS "constraint_expression",
        CASE WHEN tc.is_enforced = 'YES' THEN true ELSE false END AS "is_enforced"
    FROM {{catalog}}.information_schema.table_constraints tc
    JOIN {{catalog}}.information_schema.key_column_usage ccu
        ON tc.constraint_catalog = ccu.constraint_catalog
        AND tc.constraint_schema = ccu.constraint_schema
        AND tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')

    UNION ALL

    SELECT
        tc.constraint_catalog AS "database",
        tc.constraint_schema AS "schema",
        tc.table_name AS "table",
        tc.constraint_name AS "constraint_name",
        '' AS "column_name",
        'CHECK' AS "constraint_type",
        0 AS "column_position",
        cc.check_clause AS "constraint_expression",
        CASE WHEN tc.is_enforced = 'YES' THEN true ELSE false END AS "is_enforced"
    FROM {{catalog}}.information_schema.table_constraints tc
    JOIN {{catalog}}.information_schema.check_constraints cc
        ON tc.constraint_catalog = cc.constraint_catalog
        AND tc.constraint_schema = cc.constraint_schema
        AND tc.constraint_name = cc.constraint_name
    WHERE tc.constraint_type = 'CHECK'
)
WHERE 1=1
  /* SYNQ_SCOPE_FILTER */
ORDER BY "schema", "table", "constraint_name", "column_position"
