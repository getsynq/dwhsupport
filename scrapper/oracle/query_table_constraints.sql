SELECT
    cc.owner AS "schema",
    cc.table_name AS "table",
    cc.constraint_name AS "constraint_name",
    cc.column_name AS "column_name",
    CASE c.constraint_type
        WHEN 'P' THEN 'PRIMARY KEY'
        WHEN 'U' THEN 'UNIQUE INDEX'
    END AS "constraint_type",
    cc.position AS "column_position"
FROM all_constraints c
JOIN all_cons_columns cc
    ON c.constraint_name = cc.constraint_name
    AND c.owner = cc.owner
WHERE c.constraint_type IN ('P', 'U')
ORDER BY cc.owner, cc.table_name, cc.constraint_name, cc.position
