-- Table & view listing for Fabric Warehouse.
--
-- Descriptions are always NULL: Fabric Warehouse exposes no extended properties
-- for object comments on the SQL endpoint.
SELECT
    DB_NAME()                       AS [database],
    s.name                          AS [schema],
    o.name                          AS [table],
    CASE o.type
        WHEN 'U' THEN 'BASE TABLE'
        WHEN 'V' THEN 'VIEW'
    END                             AS table_type,
    CAST(NULL AS VARCHAR(MAX))      AS description,
    CASE WHEN o.type = 'V' THEN 1 ELSE 0 END AS is_view,
    CASE WHEN o.type = 'U' THEN 1 ELSE 0 END AS is_table
FROM
    sys.objects o
    INNER JOIN sys.schemas s
        ON o.schema_id = s.schema_id
WHERE
    o.type IN ('U', 'V')
    AND o.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name, o.name
