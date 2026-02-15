SELECT
    n.nspname AS "schema",
    c.relname AS "table",
    con.conname AS "constraint_name",
    a.attname AS "column_name",
    CASE con.contype
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'u' THEN 'UNIQUE INDEX'
    END AS "constraint_type",
    ord.position AS "column_position"
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ord(attnum, position)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ord.attnum
WHERE con.contype IN ('p', 'u')
ORDER BY n.nspname, c.relname, con.conname, ord.position
