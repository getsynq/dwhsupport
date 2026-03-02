SELECT tbls.database as schema,
       tbls.name     as table,
       tbls.engine   as table_type,
       tbls.comment  as description
FROM clusterAllReplicas(default, system.tables) tbls
WHERE schema NOT IN ('system', 'information_schema')
  /* SYNQ_SCOPE_FILTER */
LIMIT 1 by schema, table