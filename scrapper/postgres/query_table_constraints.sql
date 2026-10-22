WITH all_constraints AS (
-- Constraints from pg_constraint (PRIMARY KEY, UNIQUE, CHECK constraints)
SELECT
    n.nspname AS "schema",
    c.relname AS "table",
    con.conname AS constraint_name,
    a.attname AS column_name,
    CASE con.contype
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'u' THEN 'UNIQUE'
    END AS constraint_type,
    ord.ordinality::int AS column_position,
    '' AS constraint_expression,
    con.convalidated AS is_enforced,
    -- the access method of the index backing the key (btree, or hash, gist, ... for an exclusion-style unique index)
    COALESCE(am.amname, '') AS index_type,
    '' AS referenced_schema,
    '' AS referenced_table,
    '' AS referenced_column_name
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ord(attnum, ordinality)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ord.attnum
LEFT JOIN pg_catalog.pg_class ci ON ci.oid = con.conindid
LEFT JOIN pg_catalog.pg_am am ON am.oid = ci.relam
WHERE con.contype IN ('p', 'u')
  AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')

UNION ALL

-- FOREIGN KEY constraints, one row per column, each with the column it references
-- (conkey and confkey pair up by position)
SELECT
    n.nspname AS "schema",
    c.relname AS "table",
    con.conname AS constraint_name,
    a.attname AS column_name,
    'FOREIGN KEY' AS constraint_type,
    ord.ordinality::int AS column_position,
    '' AS constraint_expression,
    con.convalidated AS is_enforced,
    '' AS index_type,
    rn.nspname AS referenced_schema,
    rc.relname AS referenced_table,
    ra.attname AS referenced_column_name
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid
JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS ord(attnum, refattnum, ordinality)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ord.attnum
JOIN pg_catalog.pg_attribute ra ON ra.attrelid = rc.oid AND ra.attnum = ord.refattnum
WHERE con.contype = 'f'
  -- A key into a partitioned table is stored again for each partition, on the
  -- same table and with conparentid naming the key. A partition's key inherited
  -- from its partitioned table is on the partition itself, so it stays.
  AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_constraint parent
      WHERE parent.oid = con.conparentid AND parent.conrelid = con.conrelid
  )
  AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')

UNION ALL

-- CHECK constraints from pg_constraint (table-level, no specific column)
SELECT
    n.nspname AS "schema",
    c.relname AS "table",
    con.conname AS constraint_name,
    '' AS column_name,
    'CHECK' AS constraint_type,
    0 AS column_position,
    -- the expression alone, as the other platforms give it; pg_get_constraintdef wraps it in CHECK (...)
    pg_get_expr(con.conbin, con.conrelid) AS constraint_expression,
    con.convalidated AS is_enforced,
    '' AS index_type,
    '' AS referenced_schema,
    '' AS referenced_table,
    '' AS referenced_column_name
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE con.contype = 'c'
  AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')

UNION ALL

-- Indexes from pg_index that are NOT backing a constraint (standalone CREATE INDEX / CREATE UNIQUE INDEX)
SELECT
    n.nspname AS "schema",
    t.relname AS "table",
    ic.relname AS constraint_name,
    a.attname AS column_name,
    CASE WHEN ix.indisunique THEN 'UNIQUE INDEX' ELSE 'INDEX' END AS constraint_type,
    key_ord.ordinality::int AS column_position,
    '' AS constraint_expression,
    ix.indisvalid AS is_enforced,
    am.amname AS index_type,
    '' AS referenced_schema,
    '' AS referenced_table,
    '' AS referenced_column_name
FROM pg_catalog.pg_index ix
JOIN pg_catalog.pg_class ic ON ic.oid = ix.indexrelid
JOIN pg_catalog.pg_am am ON am.oid = ic.relam
JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS key_ord(attnum, ordinality)
JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = key_ord.attnum
WHERE NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_constraint con
    WHERE con.conindid = ix.indexrelid
)
  AND ix.indisvalid
  AND key_ord.attnum > 0
  AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
)
SELECT "schema", "table", constraint_name, column_name, constraint_type, column_position, constraint_expression, is_enforced,
    index_type, referenced_schema, referenced_table, referenced_column_name
FROM all_constraints
WHERE 1=1
  /* SYNQ_SCOPE_FILTER */
ORDER BY "schema", "table", constraint_name, column_position
