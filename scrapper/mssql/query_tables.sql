SELECT
    DB_NAME()                       AS [database],
    s.name                          AS [schema],
    o.name                          AS [table],
    CASE o.type
        WHEN 'U' THEN 'BASE TABLE'
        WHEN 'V' THEN 'VIEW'
    END                             AS table_type,
    ep.value                        AS description,
    CASE WHEN o.type = 'V' THEN 1 ELSE 0 END AS is_view,
    CASE WHEN o.type = 'U' THEN 1 ELSE 0 END AS is_table
FROM
    sys.objects o
    INNER JOIN sys.schemas s
        ON o.schema_id = s.schema_id
    LEFT JOIN sys.extended_properties ep
        ON ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
WHERE
    o.type IN ('U', 'V')
    AND o.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
ORDER BY
    s.name, o.name
