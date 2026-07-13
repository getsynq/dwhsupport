-- Column-level catalog for Fabric Warehouse.
--
-- Fabric Warehouse has no extended properties (sp_addextendedproperty is
-- rejected), so column/table comments are always NULL — there is no metadata
-- source for them on the SQL endpoint. National-character types (NVARCHAR/NCHAR)
-- do not exist, so they are absent from the type-formatting CASE. All CAST
-- targets carry an explicit length: Fabric rejects bare-length character casts.
SELECT
    DB_NAME()                       AS [database],
    s.name                          AS [schema],
    t.name                          AS [table],
    CASE WHEN t.type = 'V' THEN 1 ELSE 0 END AS is_view,
    CAST(NULL AS VARCHAR(MAX))      AS table_comment,
    c.name                          AS [column],
    c.column_id                     AS [position],
    TYPE_NAME(c.user_type_id) +
        CASE
            WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'char', 'varbinary')
                THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(20)) END + ')'
            WHEN TYPE_NAME(c.user_type_id) IN ('decimal', 'numeric')
                THEN '(' + CAST(c.precision AS VARCHAR(20)) + ',' + CAST(c.scale AS VARCHAR(20)) + ')'
            WHEN TYPE_NAME(c.user_type_id) IN ('datetime2', 'time')
                THEN '(' + CAST(c.scale AS VARCHAR(20)) + ')'
            ELSE ''
        END                         AS [type],
    CAST(NULL AS VARCHAR(MAX))      AS comment
FROM
    sys.columns c
    INNER JOIN sys.objects t
        ON c.object_id = t.object_id
    INNER JOIN sys.schemas s
        ON t.schema_id = s.schema_id
WHERE
    t.type IN ('U', 'V')
    AND t.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name, t.name, c.column_id
