SELECT
    n.nspname AS "schema",
    c.relname AS "table",
    con.conname AS "constraint_name",
    a.attname AS "column_name",
    CASE con.contype
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'u' THEN 'UNIQUE INDEX'
        WHEN 'f' THEN 'FOREIGN KEY'
    END AS "constraint_type",
    idx AS "column_position",
    -- Redshift records primary, unique and foreign keys for the planner and never checks them
    FALSE AS "is_enforced",
    -- a foreign key's column at the same position of the referenced table
    COALESCE(rn.nspname, '') AS "referenced_schema",
    COALESCE(rc.relname, '') AS "referenced_table",
    COALESCE(ra.attname, '') AS "referenced_column_name"
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN generate_series(1, 32) idx ON idx <= array_upper(con.conkey, 1)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = con.conkey[idx]
LEFT JOIN pg_catalog.pg_class rc ON con.contype = 'f' AND rc.oid = con.confrelid
LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
LEFT JOIN pg_catalog.pg_attribute ra ON ra.attrelid = rc.oid AND ra.attnum = con.confkey[idx]
WHERE con.contype IN ('p', 'u', 'f')
  AND n.nspname NOT IN ({{system_schemas}})
  -- a materialized view keeps its rows in mv_tbl__<view>__<n> in its own schema, owned by rdsdb (usesysid 1)
  AND NOT (c.relowner = 1 AND left(c.relname, 8) = 'mv_tbl__')
  /* SYNQ_SCOPE_FILTER */
ORDER BY n.nspname, c.relname, con.conname, idx
