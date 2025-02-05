SELECT tbls.database                                               as schema,
       tbls.name                                                   as table,
       toBool(tbls.engine = 'View' or engine = 'MaterializedView') as is_view,
       tbls.create_table_query                                     as sql
FROM clusterAllReplicas(default, system.tables) tbls
WHERE length(sql) > 0
LIMIT 1 by schema, table