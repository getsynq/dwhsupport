SELECT database_name as "database",
       schema_name   as "schema",
       comment       as "description"
FROM duckdb_schemas()
WHERE not internal
  AND schema_name NOT IN ('information_schema')
  AND database_name NOT IN ('sample_data', 'temp', 'system', 'md_information_schema')
  /* SYNQ_SCOPE_FILTER */
ORDER BY database_name, schema_name
