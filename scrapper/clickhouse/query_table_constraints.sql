-- Primary keys parsed from system.tables.primary_key with correct key column ordering
SELECT
    t.database AS schema,
    t.name AS table,
    'primary_key' AS constraint_name,
    col AS column_name,
    'PRIMARY KEY' AS constraint_type,
    toInt32(col_idx) AS column_position
FROM clusterAllReplicas(default, system.tables) t
ARRAY JOIN
    splitByString(', ', t.primary_key) AS col,
    arrayEnumerate(splitByString(', ', t.primary_key)) AS col_idx
WHERE t.primary_key != ''
  AND t.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
LIMIT 1 BY schema, table, constraint_name, column_name

UNION ALL

-- Sorting keys parsed from system.tables.sorting_key with correct key column ordering
SELECT
    t.database AS schema,
    t.name AS table,
    'sorting_key' AS constraint_name,
    col AS column_name,
    'SORTING KEY' AS constraint_type,
    toInt32(col_idx) AS column_position
FROM clusterAllReplicas(default, system.tables) t
ARRAY JOIN
    splitByString(', ', t.sorting_key) AS col,
    arrayEnumerate(splitByString(', ', t.sorting_key)) AS col_idx
WHERE t.sorting_key != ''
  AND t.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
LIMIT 1 BY schema, table, constraint_name, column_name

UNION ALL

-- Data skipping indexes (bloom_filter, minmax, set, etc.)
SELECT
    dsi.database AS schema,
    dsi.table AS table,
    dsi.name AS constraint_name,
    dsi.expr AS column_name,
    'INDEX' AS constraint_type,
    toInt32(1) AS column_position
FROM clusterAllReplicas(default, system.data_skipping_indices) dsi
WHERE dsi.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
LIMIT 1 BY schema, table, constraint_name

UNION ALL

-- Partition keys from system.tables
SELECT
    t.database AS schema,
    t.name AS table,
    'partition_key' AS constraint_name,
    t.partition_key AS column_name,
    'PARTITION BY' AS constraint_type,
    toInt32(1) AS column_position
FROM clusterAllReplicas(default, system.tables) t
WHERE t.partition_key != ''
  AND t.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')

-- TODO: Add projections when system.projections table becomes available in ClickHouse.
-- Currently projections can only be extracted by parsing create_table_query DDL which is fragile.

ORDER BY schema, table, constraint_type, constraint_name, column_position
