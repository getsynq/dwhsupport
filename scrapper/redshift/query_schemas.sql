select distinct
    schema_name as "schema",
    schema_type as "schema_type"
from svv_all_schemas
where database_name = $1
  and schema_name not in ('pg_catalog', 'information_schema', 'pg_automv', 'catalog_history', 'pg_toast', 'pg_internal')
  /* SYNQ_SCOPE_FILTER */
order by "schema"
