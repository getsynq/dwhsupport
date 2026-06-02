SELECT name    as schema,
       comment as description,
       engine  as schema_type
FROM clusterAllReplicas(default, system.databases)
WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
  /* SYNQ_SCOPE_FILTER */
LIMIT 1 by schema
