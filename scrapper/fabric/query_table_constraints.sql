-- Constraints for Fabric Warehouse.
--
-- Fabric Warehouse supports PRIMARY KEY / UNIQUE / FOREIGN KEY only as
-- NONCLUSTERED NOT ENFORCED constraints — they are informational (never
-- enforced by the engine) but do appear in metadata. There are no physical
-- indexes (so sys.indexes is not a reliable source) and CHECK / DEFAULT
-- constraints are rejected at DDL time, so neither is emitted here.
--
-- INFORMATION_SCHEMA is the portable, Fabric-supported source for these. Every
-- constraint is reported with is_enforced = 0 because Fabric never enforces them.
SELECT
    tc.TABLE_SCHEMA                 AS [schema],
    tc.TABLE_NAME                   AS [table],
    tc.CONSTRAINT_NAME              AS [constraint_name],
    kcu.COLUMN_NAME                 AS [column_name],
    CASE tc.CONSTRAINT_TYPE
        WHEN 'PRIMARY KEY' THEN 'PRIMARY KEY'
        WHEN 'UNIQUE'      THEN 'UNIQUE INDEX'
        WHEN 'FOREIGN KEY' THEN 'FOREIGN KEY'
        ELSE tc.CONSTRAINT_TYPE
    END                             AS [constraint_type],
    kcu.ORDINAL_POSITION            AS [column_position],
    CAST('' AS VARCHAR(MAX))        AS [constraint_expression],
    CAST(0 AS BIT)                  AS [is_enforced]
FROM
    {{DB}}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    INNER JOIN {{DB}}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
        AND tc.TABLE_NAME = kcu.TABLE_NAME
WHERE
    tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
    AND tc.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
