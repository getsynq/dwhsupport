select t.table_name    as "table"
     , t.database_name as "database"
     , t.schema_name   as "schema"
     , estimated_size  as "row_count"
from duckdb_tables() t
WHERE not temporary
  and not internal
  AND schema_name NOT IN ('information_schema')
  AND database_name NOT IN ('sample_data', 'temp', 'system', 'md_information_schema')