-- pg_namespace and pg_description hold only the connected database's schemas, so a
-- schema svv_all_schemas lists from another database or a datashare has no comment.
select distinct
    s.schema_name as "schema",
    s.schema_type as "schema_type",
    d.description as "description"
from svv_all_schemas s
         left join pg_namespace n
                   on n.nspname = s.schema_name and s.database_name = current_database()
         left join pg_description d
                   on d.objoid = n.oid and d.objsubid = 0
where s.database_name = $1
  and s.schema_name not in ({{system_schemas}})
  /* SYNQ_SCOPE_FILTER */
order by "schema"
