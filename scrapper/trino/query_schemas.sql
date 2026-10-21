SELECT
    catalog_name as database,
    schema_name  as schema
FROM {{catalog}}.information_schema.schemata
WHERE schema_name NOT IN ({{excluded_schemas}})
  /* SYNQ_SCOPE_FILTER */
