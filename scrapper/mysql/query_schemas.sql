select
    schema_name as "schema"
from information_schema.schemata
where schema_name not in ('information_schema', 'performance_schema', 'mysql', 'sys')
  /* SYNQ_SCOPE_FILTER */
order by "schema"
