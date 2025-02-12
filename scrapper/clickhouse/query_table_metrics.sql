with parts as (select database               as schema,
                      table,
                      max(modification_time) as updated_at
               from clusterAllReplicas(default, system.parts) prts
               group by database,
                        table
               )

select database            AS schema,
       name                AS table,
       toInt64(total_rows) AS row_count,
       parts.updated_at    as updated_at
from clusterAllReplicas(default, system.tables) tbls
         left join parts
                   ON tbls.database = parts.schema
                       AND tbls.name = parts.table
where has_own_data = 1 AND schema NOT IN ('system', 'information_schema')
settings join_use_nulls=1