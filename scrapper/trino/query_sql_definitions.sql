with tables as (
    select 
        table_catalog as database,
        table_schema as schema,
        table_name,
        table_type
    from {{catalog}}.information_schema.tables
    where table_schema not in ('information_schema', 'sys')
)
select 
    t.database,
    t.schema,
    t.table_name as "table",
    t.table_type = 'VIEW' as is_view,
    v.view_definition as sql
from tables t
left join {{catalog}}.information_schema.views v
    on t.schema = v.table_schema and t.table_name = v.table_name
where t.table_type = 'VIEW' or v.view_definition is not null
order by t.database, t.schema, t.table_name 