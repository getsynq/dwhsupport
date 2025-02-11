select "database",
       "schema",
       "table",
       estimated_visible_rows as "row_count",
       null::timestamp        as "updated_at"
from pg_catalog.svv_table_info
where estimated_visible_rows is not null
  AND schema <> 'catalog_history'::name
  AND schema <> 'pg_toast'::name
  AND schema <> 'pg_internal'::name
  and "database" = $1