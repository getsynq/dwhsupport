SELECT
    t.table_catalog                       AS database,
    t.table_schema                        AS schema,
    t.table_name                          AS "table",
    (t.table_type = 'VIEW')               AS is_view,
    false                                 AS is_materialized_view,
    coalesce(v.view_definition, '')       AS sql
FROM information_schema.tables t
LEFT JOIN information_schema.views v
    ON  v.table_catalog = t.table_catalog
    AND v.table_schema  = t.table_schema
    AND v.table_name    = t.table_name
WHERE t.table_schema NOT IN ('information_schema')
  /* SYNQ_SCOPE_FILTER */
