with tables as (select null         as "table_database",
                       table_schema as "table_schema",
                       table_name   as "table_name",
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
       '' AS "description"
from tables
where table_schema not in ('information_schema', 'performance_schema', 'mysql', 'sys')
order by
    "database", "schema", "table"