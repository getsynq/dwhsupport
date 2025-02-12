with cols as (select database          as schema,
                     table,
                     name              as column,
                     type,
                     toInt32(position) as position,
                     comment
              from
                  clusterAllReplicas(default, system.columns)
              WHERE schema NOT IN ('system', 'information_schema')
              limit 1 by schema, table, column),
     table_comments as (SELECT database as schema,
                               table,
                               comment  as table_comment
                        FROM clusterAllReplicas(default, system.tables)
                        WHERE schema NOT IN ('system', 'information_schema')
                        limit 1 by schema, table)
SELECT cols.schema,
       cols.table,
       cols.column,
       cols.type,
       cols.position,
       cols.comment,
       table_comments.table_comment
FROM cols
         LEFT JOIN table_comments USING (schema, table)