SELECT
    c.database AS schema,
    c.table AS table,
    CASE
        WHEN c.is_in_primary_key = 1 THEN 'primary_key'
        WHEN c.is_in_sorting_key = 1 THEN 'sorting_key'
    END AS constraint_name,
    c.name AS column_name,
    CASE
        WHEN c.is_in_primary_key = 1 THEN 'PRIMARY KEY'
        WHEN c.is_in_sorting_key = 1 THEN 'SORTING KEY'
    END AS constraint_type,
    toInt32(c.position) AS column_position
FROM system.columns c
WHERE c.database = ?
    AND c.table = ?
    AND (c.is_in_primary_key = 1 OR c.is_in_sorting_key = 1)
ORDER BY constraint_name, c.position
