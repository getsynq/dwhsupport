SELECT "database", "schema", "table", "constraint_name", "column_name", "constraint_type", "column_position", "constraint_expression", "is_enforced"
FROM (
    SELECT
        tc.table_catalog AS "database",
        tc.table_schema AS "schema",
        tc.table_name AS "table",
        tc.constraint_name AS "constraint_name",
        kcu.column_name AS "column_name",
        tc.constraint_type AS "constraint_type",
        kcu.ordinal_position AS "column_position",
        '' AS "constraint_expression",
        -- DuckDB checks a primary key, a unique constraint and a check on every write
        TRUE AS "is_enforced"
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_catalog = kcu.constraint_catalog
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')

    UNION ALL

    SELECT
        tc.table_catalog AS "database",
        tc.constraint_schema AS "schema",
        tc.table_name AS "table",
        tc.constraint_name AS "constraint_name",
        '' AS "column_name",
        'CHECK' AS "constraint_type",
        0 AS "column_position",
        cc.check_clause AS "constraint_expression",
        TRUE AS "is_enforced"
    FROM information_schema.table_constraints tc
    JOIN information_schema.check_constraints cc
        ON tc.constraint_catalog = cc.constraint_catalog
        AND tc.constraint_schema = cc.constraint_schema
        AND tc.constraint_name = cc.constraint_name
    WHERE tc.constraint_type = 'CHECK'
)
-- the databases and schema QueryTables leaves out
WHERE "schema" NOT IN ('information_schema')
  AND "database" NOT IN ('sample_data', 'temp', 'system', 'md_information_schema')
  /* SYNQ_SCOPE_FILTER */
ORDER BY "database", "schema", "table", "constraint_name", "column_position"
