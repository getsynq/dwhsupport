SELECT ''            as _database,
       tbls.database as schema,
       tbls.name     as table,
       tbls.engine   as table_type,
       tbls.comment  as description
FROM clusterAllReplicas(default, system.tables) tbls
LIMIT 1 by schema, table