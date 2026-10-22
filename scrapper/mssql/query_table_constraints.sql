SELECT [database], [schema], [table], [constraint_name], [column_name], [constraint_type], [column_position], [constraint_expression], [is_enforced],
    [index_type], [referenced_database], [referenced_schema], [referenced_table], [referenced_column_name]
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
        CAST(CASE WHEN i.is_disabled = 0 THEN 1 ELSE 0 END AS BIT) AS [is_enforced],
        -- CLUSTERED, NONCLUSTERED, CLUSTERED COLUMNSTORE, NONCLUSTERED COLUMNSTORE, XML, SPATIAL, ...
        i.type_desc AS [index_type],
        '' AS [referenced_database],
        '' AS [referenced_schema],
        '' AS [referenced_table],
        '' AS [referenced_column_name]
    FROM sys.indexes i
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    JOIN sys.tables t ON i.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE ic.is_included_column = 0

    UNION ALL

    -- foreign keys, one row per column, each with the column it references
    -- (a foreign key cannot reference another database)
    SELECT
        DB_NAME() AS [database],
        s.name AS [schema],
        t.name AS [table],
        fk.name AS [constraint_name],
        c.name AS [column_name],
        'FOREIGN KEY' AS [constraint_type],
        fkc.constraint_column_id AS [column_position],
        '' AS [constraint_expression],
        CAST(CASE WHEN fk.is_disabled = 0 THEN 1 ELSE 0 END AS BIT) AS [is_enforced],
        '' AS [index_type],
        DB_NAME() AS [referenced_database],
        rs.name AS [referenced_schema],
        rt.name AS [referenced_table],
        rc.name AS [referenced_column_name]
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
    JOIN sys.tables t ON t.object_id = fk.parent_object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN sys.columns c ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
    JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
    JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
    JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id

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
        CAST(CASE WHEN cc.is_disabled = 0 THEN 1 ELSE 0 END AS BIT) AS [is_enforced],
        '' AS [index_type],
        '' AS [referenced_database],
        '' AS [referenced_schema],
        '' AS [referenced_table],
        '' AS [referenced_column_name]
    FROM sys.check_constraints cc
    JOIN sys.tables t ON cc.parent_object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
) sub
WHERE 1=1
    /* SYNQ_SCOPE_FILTER */
ORDER BY [schema], [table], [constraint_name], [column_position]
