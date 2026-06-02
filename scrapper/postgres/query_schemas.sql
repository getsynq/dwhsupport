select
    sch.nspname                              as "schema",
    sch_desc.description                     as "description",
    pg_catalog.pg_get_userbyid(sch.nspowner) as "schema_owner"
from pg_catalog.pg_namespace sch
         left outer join pg_catalog.pg_description sch_desc
                         on (sch_desc.objoid = sch.oid and sch_desc.objsubid = 0)
where
    not pg_is_other_temp_schema(sch.oid)
  and sch.nspname not in ('pg_catalog', 'information_schema')
  and sch.nspname not like 'pg_temp_%'
  and sch.nspname not like 'pg_toast_temp_%'
  and sch.nspname <> 'pg_toast'
  /* SYNQ_SCOPE_FILTER */
order by "schema"
