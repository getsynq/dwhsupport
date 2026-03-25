SELECT [database], [schema], [table], [constraint_name], [column_name], [constraint_type], [column_position], [constraint_expression], [is_enforced]
FROM (
    SELECT
        DB_NAME() AS [database],
        s.name AS [schema],
        t.name AS [table],
        i.name AS [constraint_name],
        c.name AS [column_name],
        CASE
            WHEN i.is_primary_key = 1 THEN 'PRIMARY KEY'
            WHEN i.is_unique = 1 THEN 'UNIQUE INDEX'
            ELSE 'INDEX'
        END AS [constraint_type],
        ic.key_ordinal AS [column_position],
        '' AS [constraint_expression],
        CAST(CASE WHEN i.is_disabled = 0 THEN 1 ELSE 0 END AS BIT) AS [is_enforced]
    FROM sys.indexes i
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    JOIN sys.tables t ON i.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE ic.is_included_column = 0

    UNION ALL

    SELECT
        DB_NAME() AS [database],
        s.name AS [schema],
        t.name AS [table],
        cc.name AS [constraint_name],
        '' AS [column_name],
        'CHECK' AS [constraint_type],
        0 AS [column_position],
        cc.definition AS [constraint_expression],
        CAST(CASE WHEN cc.is_disabled = 0 THEN 1 ELSE 0 END AS BIT) AS [is_enforced]
    FROM sys.check_constraints cc
    JOIN sys.tables t ON cc.parent_object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
) sub
WHERE 1=1
    /* SYNQ_SCOPE_FILTER */
ORDER BY [schema], [table], [constraint_name], [column_position]
