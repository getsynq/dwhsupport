SELECT
    v.table_catalog                       AS database,
    v.table_schema                        AS schema,
    v.table_name                          AS "table",
    true                                  AS is_view,
    false                                 AS is_materialized_view,
    coalesce(v.view_definition, '')       AS sql
FROM information_schema.views v
WHERE v.table_schema NOT IN ('information_schema')
  /* SYNQ_SCOPE_FILTER */
