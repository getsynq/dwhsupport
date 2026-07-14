-- Column-level catalog for Fabric Warehouse.
--
-- Comments come from sys.extended_properties (MS_Description), exactly as on SQL
-- Server. Fabric currently rejects sp_addextendedproperty, so the view is empty
-- today and comments come back NULL — but the view IS queryable (verified live),
-- so joining it costs nothing and forward-supports Fabric adding write support
-- (and any warehouse that does carry descriptions). National-character types
-- (NVARCHAR/NCHAR) do not exist on Fabric, so they are absent from the
-- type-formatting CASE. All CAST targets carry an explicit length: Fabric
-- rejects bare-length character casts.
SELECT
    s.name                          AS [schema],
    t.name                          AS [table],
    CASE WHEN t.type = 'V' THEN 1 ELSE 0 END AS is_view,
    tp.value                        AS table_comment,
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
    cp.value                        AS comment
FROM
    {{DB}}.sys.columns c
    INNER JOIN {{DB}}.sys.objects t
        ON c.object_id = t.object_id
    INNER JOIN {{DB}}.sys.schemas s
        ON t.schema_id = s.schema_id
    LEFT JOIN {{DB}}.sys.extended_properties tp
        ON tp.major_id = t.object_id AND tp.minor_id = 0 AND tp.name = 'MS_Description'
    LEFT JOIN {{DB}}.sys.extended_properties cp
        ON cp.major_id = c.object_id AND cp.minor_id = c.column_id AND cp.name = 'MS_Description'
WHERE
    t.type IN ('U', 'V')
    AND t.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name, t.name, c.column_id
