with tables as (select null          as "table_database",
                       table_schema  as "table_schema",
                       table_name    as "table_name",
                       table_comment as "table_comment",
                       case
                           when table_type = 'BASE TABLE' then 'table'
                           when table_type = 'VIEW' then 'view'
                           else table_type
                           end       as "table_type"

                from information_schema.tables),

     columns as (select null             as "table_database",
                        table_schema     as "table_schema",
                        table_name       as "table_name",

                        column_name      as "column_name",
                        ordinal_position as "column_index",
                        data_type        as "column_type",
                        column_comment   as "comment"


                 from information_schema.columns)

select ''                         as "database",
       columns.table_schema       as "schema",
       columns.table_name         as "table",
       tables.table_type = 'view' as "is_view",
       tables.table_comment       as "table_comment",
       columns.column_name        as "column",
       columns.column_index       as "position",
       columns.column_type        as "type",
       columns.comment            as "comment"
from tables
         join columns using (table_schema, table_name)
where table_schema not in ('information_schema', 'performance_schema', 'mysql', 'sys')
order by "database", "schema", "table", "position"