SELECT
    catalog_name as database,
    schema_name  as schema
FROM {{catalog}}.information_schema.schemata
WHERE schema_name NOT IN ('information_schema')
  /* SYNQ_SCOPE_FILTER */
