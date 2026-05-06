SELECT
    t.table_catalog                       AS database,
    t.table_schema                        AS schema,
    t.table_name                          AS "table",
    t.table_type                          AS "table_type",
    ''                                    AS description,
    (t.table_type = 'BASE TABLE')         AS is_table,
    (t.table_type = 'VIEW')               AS is_view
FROM information_schema.tables t
WHERE t.table_schema NOT IN ('information_schema')
  /* SYNQ_SCOPE_FILTER */
