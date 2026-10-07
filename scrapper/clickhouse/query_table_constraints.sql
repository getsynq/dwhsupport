SELECT schema, table, constraint_name, column_name, constraint_type, column_position, constraint_expression
FROM (
-- Primary keys parsed from system.tables.primary_key with correct key column ordering
SELECT
    t.database AS schema,
    t.name AS table,
    'primary_key' AS constraint_name,
    col AS column_name,
    'PRIMARY KEY' AS constraint_type,
    toInt32(col_idx) AS column_position,
    '' AS constraint_expression
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
    toInt32(col_idx) AS column_position,
    '' AS constraint_expression
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
    toInt32(1) AS column_position,
    '' AS constraint_expression
FROM clusterAllReplicas(default, system.data_skipping_indices) dsi
WHERE dsi.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
LIMIT 1 BY schema, table, constraint_name

UNION ALL

-- Partition keys: the key is an expression (toYYYYMM(created_at), (workspace, toDate(at))),
-- so it is listed as the columns it reads, in table order, each row carrying the whole
-- expression. PARTITION BY tuple() reads no column and is one partition, so it lists nothing.
SELECT
    c.database AS schema,
    c.table AS table,
    'partition_key' AS constraint_name,
    c.name AS column_name,
    'PARTITION BY' AS constraint_type,
    toInt32(c.position) AS column_position,
    t.partition_key AS constraint_expression
FROM (
    SELECT database, table, name, position
    FROM clusterAllReplicas(default, system.columns)
    WHERE is_in_partition_key
      AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
    LIMIT 1 BY database, table, name
) c
INNER JOIN (
    SELECT database, name, partition_key
    FROM clusterAllReplicas(default, system.tables)
    WHERE partition_key != ''
      AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
    LIMIT 1 BY database, name
) t ON t.database = c.database AND t.name = c.table

-- TODO: Add projections when system.projections table becomes available in ClickHouse.
-- Currently projections can only be extracted by parsing create_table_query DDL which is fragile.
)
WHERE 1=1
  /* SYNQ_SCOPE_FILTER */
ORDER BY schema, table, constraint_type, constraint_name, column_position
