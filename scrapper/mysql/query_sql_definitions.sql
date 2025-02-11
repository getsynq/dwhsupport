with tables as (select null         as "table_database",
                       table_schema as "table_schema",
                       table_name   as "table_name",
                       case
                           when table_type = 'BASE TABLE' then 'table'
                           when table_type = 'VIEW' then 'view'
                           else table_type
                           end      as "table_type"

                from information_schema.tables)

select ''                  as "database",
       table_schema        as "schema",
       table_name          as "table",
       table_type = 'view' as "is_view",
       view_definition     as "sql"
from tables
         LEFT JOIN information_schema.VIEWS
                   USING (table_schema, table_name)
where table_schema not in ('information_schema', 'performance_schema', 'mysql', 'sys')
  AND (table_type = 'view' or VIEW_DEFINITION is not null)
order by "database", "schema", "table"