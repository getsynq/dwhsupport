with tables as (select null         as "table_database",
                       table_schema as "table_schema",
                       table_name   as "table_name",
                       -- MySQL and MariaDB report the comment of every view as 'VIEW'
                       case
                           when table_type = 'VIEW' then null
                           else nullif(table_comment, '')
                           end      as "table_comment",
                       case
                           when table_type = 'BASE TABLE' then 'table'
                           when table_type = 'VIEW' then 'view'
                           else table_type
                           end      as "table_type"

                from information_schema.tables)

select ''                         as "database",
       table_schema       as "schema",
       table_name         as "table",
       table_type = 'view' as "is_view",
       table_comment      as "description"
from tables
where table_schema not in ('information_schema', 'performance_schema', 'mysql', 'sys')
  /* SYNQ_SCOPE_FILTER */
order by
    "database", "schema", "table"