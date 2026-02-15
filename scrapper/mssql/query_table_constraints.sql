SELECT
    s.name AS [schema],
    t.name AS [table],
    i.name AS [constraint_name],
    c.name AS [column_name],
    CASE
        WHEN i.is_primary_key = 1 THEN 'PRIMARY KEY'
        WHEN i.is_unique = 1 THEN 'UNIQUE INDEX'
        ELSE 'INDEX'
    END AS [constraint_type],
    ic.key_ordinal AS [column_position]
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN sys.tables t ON i.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @p1
    AND t.name = @p2
    AND ic.is_included_column = 0
ORDER BY i.name, ic.key_ordinal
