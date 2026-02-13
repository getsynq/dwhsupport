SELECT
    DB_NAME()                       AS [database],
    s.name                          AS [schema],
    t.name                          AS [table],
    CASE
        WHEN t.type IN ('V')
        THEN 1 ELSE 0
    END                             AS is_view,
    tp.value                        AS table_comment,
    c.name                          AS [column],
    c.column_id                     AS [position],
    TYPE_NAME(c.user_type_id) +
        CASE
            WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary')
                THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END + ')'
            WHEN TYPE_NAME(c.user_type_id) IN ('decimal', 'numeric')
                THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
            WHEN TYPE_NAME(c.user_type_id) IN ('datetime2', 'datetimeoffset', 'time')
                THEN '(' + CAST(c.scale AS VARCHAR) + ')'
            ELSE ''
        END                         AS [type],
    cp.value                        AS comment
FROM
    sys.columns c
    INNER JOIN sys.objects t
        ON c.object_id = t.object_id
    INNER JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    LEFT JOIN sys.extended_properties tp
        ON tp.major_id = t.object_id AND tp.minor_id = 0 AND tp.name = 'MS_Description'
    LEFT JOIN sys.extended_properties cp
        ON cp.major_id = c.object_id AND cp.minor_id = c.column_id AND cp.name = 'MS_Description'
WHERE
    t.type IN ('U', 'V')
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
    AND t.is_ms_shipped = 0
ORDER BY
    s.name, t.name, c.column_id
