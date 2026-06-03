SELECT DISTINCT
    catalog_name AS database,
    schema_name  AS schema
FROM information_schema.schemata
WHERE schema_name NOT IN ('information_schema')
  /* SYNQ_SCOPE_FILTER */
