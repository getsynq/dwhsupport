SELECT current_database()    AS database,
       n.nspname             AS schema,
       c.relname             AS table,
       c.relkind = 'v'       AS is_view,
       c.relkind = 'm'       AS is_materialized_view,
       pg_get_viewdef(c.oid) AS sql
FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('v', 'm')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND current_database() = $1
  /* SYNQ_SCOPE_FILTER */
