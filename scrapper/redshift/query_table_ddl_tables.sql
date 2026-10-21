-- The tables whose DDL QuerySqlDefinitions reads with SHOW TABLE. SHOW TABLE
-- answers a view too, with a CREATE TABLE of its columns, so only ordinary
-- tables are listed.
select n.nspname as schema,
       c.relname as table
from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
where c.relkind = 'r'
  and n.nspname not in ({{system_schemas}})
  and n.nspname not like 'pg!_temp!_%' escape '!'
  -- a materialized view keeps its rows in mv_tbl__<view>__<n> in its own schema, owned by rdsdb (usesysid 1)
  and not (c.relowner = 1 and left(c.relname, 8) = 'mv_tbl__')
  /* SYNQ_SCOPE_FILTER */
order by schema, "table"
