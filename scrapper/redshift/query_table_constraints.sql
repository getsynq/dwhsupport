SELECT
    n.nspname AS "schema",
    c.relname AS "table",
    con.conname AS "constraint_name",
    a.attname AS "column_name",
    CASE con.contype
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'u' THEN 'UNIQUE INDEX'
    END AS "constraint_type",
    idx AS "column_position"
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN generate_series(1, 1600) idx ON idx <= array_upper(con.conkey, 1)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = con.conkey[idx]
WHERE con.contype IN ('p', 'u')
  /* SYNQ_SCOPE_FILTER */
ORDER BY n.nspname, c.relname, con.conname, idx
