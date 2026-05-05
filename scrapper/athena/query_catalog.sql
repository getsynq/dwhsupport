SELECT
    c.table_catalog                       AS database,
    c.table_schema                        AS schema,
    c.table_name                          AS "table",
    c.column_name                         AS column,
    c.ordinal_position                    AS position,
    c.data_type                           AS type,
    CAST(NULL AS VARCHAR)                 AS comment,
    CAST(NULL AS VARCHAR)                 AS table_comment
FROM information_schema.columns c
WHERE c.table_schema NOT IN ('information_schema')
  /* SYNQ_SCOPE_FILTER */
