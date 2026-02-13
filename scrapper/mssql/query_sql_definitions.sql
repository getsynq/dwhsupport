SELECT
    DB_NAME()                       AS [database],
    s.name                          AS [schema],
    v.name                          AS [table],
    1                               AS is_view,
    0                               AS is_materialized_view,
    m.definition                    AS [sql]
FROM
    sys.views v
    INNER JOIN sys.schemas s
        ON v.schema_id = s.schema_id
    INNER JOIN sys.sql_modules m
        ON v.object_id = m.object_id
WHERE
    v.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
ORDER BY
    s.name, v.name
