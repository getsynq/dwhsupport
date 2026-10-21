-- Constraints for Fabric Warehouse.
--
-- Fabric Warehouse supports PRIMARY KEY / UNIQUE / FOREIGN KEY only as
-- NONCLUSTERED NOT ENFORCED constraints — they are informational (never
-- enforced by the engine) but do appear in metadata. There are no physical
-- indexes (so sys.indexes is not a reliable source) and CHECK / DEFAULT
-- constraints are rejected at DDL time, so neither is emitted here.
--
-- INFORMATION_SCHEMA is the portable, Fabric-supported source for keys. It
-- does not pair a foreign key's columns with the ones they reference, so
-- foreign keys come from sys.foreign_key_columns. Every constraint is reported
-- with is_enforced = 0 because Fabric never enforces them.
SELECT [schema], [table], [constraint_name], [column_name], [constraint_type], [column_position], [constraint_expression], [is_enforced],
    [referenced_schema], [referenced_table], [referenced_column_name]
FROM (
    SELECT
        tc.TABLE_SCHEMA                 AS [schema],
        tc.TABLE_NAME                   AS [table],
        tc.CONSTRAINT_NAME              AS [constraint_name],
        kcu.COLUMN_NAME                 AS [column_name],
        CASE tc.CONSTRAINT_TYPE
            WHEN 'PRIMARY KEY' THEN 'PRIMARY KEY'
            WHEN 'UNIQUE'      THEN 'UNIQUE INDEX'
            ELSE tc.CONSTRAINT_TYPE
        END                             AS [constraint_type],
        kcu.ORDINAL_POSITION            AS [column_position],
        CAST('' AS VARCHAR(MAX))        AS [constraint_expression],
        CAST(0 AS BIT)                  AS [is_enforced],
        CAST('' AS VARCHAR(128))        AS [referenced_schema],
        CAST('' AS VARCHAR(128))        AS [referenced_table],
        CAST('' AS VARCHAR(128))        AS [referenced_column_name]
    FROM
        {{DB}}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN {{DB}}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
    WHERE
        tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')

    UNION ALL

    -- foreign keys, one row per column, each with the column it references
    -- (a foreign key cannot reference another database)
    SELECT
        s.name                          AS [schema],
        t.name                          AS [table],
        fk.name                         AS [constraint_name],
        c.name                          AS [column_name],
        'FOREIGN KEY'                   AS [constraint_type],
        fkc.constraint_column_id        AS [column_position],
        CAST('' AS VARCHAR(MAX))        AS [constraint_expression],
        CAST(0 AS BIT)                  AS [is_enforced],
        rs.name                         AS [referenced_schema],
        rt.name                         AS [referenced_table],
        rc.name                         AS [referenced_column_name]
    FROM
        {{DB}}.sys.foreign_keys fk
        INNER JOIN {{DB}}.sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        INNER JOIN {{DB}}.sys.tables t ON t.object_id = fk.parent_object_id
        INNER JOIN {{DB}}.sys.schemas s ON s.schema_id = t.schema_id
        INNER JOIN {{DB}}.sys.columns c ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
        INNER JOIN {{DB}}.sys.tables rt ON rt.object_id = fk.referenced_object_id
        INNER JOIN {{DB}}.sys.schemas rs ON rs.schema_id = rt.schema_id
        INNER JOIN {{DB}}.sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
) sub
WHERE
    [schema] NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    [schema], [table], [constraint_name], [column_position]
