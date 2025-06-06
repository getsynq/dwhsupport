with tables as (
    select
        table_catalog as database,
        table_schema as schema,
        table_name,
        table_type
    from {{catalog}}.information_schema.tables
    where table_schema not in ('information_schema')
)
select
    t.database,
    t.schema,
    t.table_name as "table",
    (t.table_type = 'VIEW' AND v.view_definition IS NOT NULL) as is_view,
    mv.name is not null as is_materialized_view,
    coalesce(mv.definition, v.view_definition, '') as sql
from tables t
left join {{catalog}}.information_schema.views v
    on t.schema = v.table_schema and t.table_name = v.table_name
LEFT JOIN system.metadata.materialized_views mv
    ON t.database = mv.catalog_name
    AND t.schema = mv.schema_name
    AND t.table_name = mv.name
order by t.database, t.schema, t.table_name 