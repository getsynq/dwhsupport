SELECT
    DB_NAME()                               AS [database],
    s.name                                  AS [schema],
    t.name                                  AS [table],
    p.row_count                             AS row_count,
    STATS_DATE(t.object_id, si.index_id)    AS updated_at,
    p.total_bytes                           AS size_bytes
FROM
    sys.tables t
    INNER JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    LEFT JOIN (
        SELECT
            object_id,
            MIN(index_id) AS index_id
        FROM sys.indexes
        WHERE index_id <= 1
        GROUP BY object_id
    ) si ON t.object_id = si.object_id
    LEFT JOIN (
        SELECT
            p.object_id,
            SUM(p.rows) AS row_count,
            SUM(a.total_pages) * 8 * 1024 AS total_bytes
        FROM sys.partitions p
        INNER JOIN sys.allocation_units a
            ON p.partition_id = a.container_id
        WHERE p.index_id IN (0, 1)
        GROUP BY p.object_id
    ) p ON t.object_id = p.object_id
WHERE
    t.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
    AND t.type = 'U'
ORDER BY
    s.name, t.name
