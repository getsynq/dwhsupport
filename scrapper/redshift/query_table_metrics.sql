select "database",
       "schema",
       "table",
       estimated_visible_rows as "row_count",
       -- size is in 1 MB blocks
       size::bigint * 1048576 as "size_bytes",
       null::timestamp        as "updated_at"
from pg_catalog.svv_table_info
where estimated_visible_rows is not null
  AND schema <> 'catalog_history'::name
  AND schema <> 'pg_toast'::name
  AND schema <> 'pg_internal'::name
  and "database" = $1
  /* SYNQ_SCOPE_FILTER */