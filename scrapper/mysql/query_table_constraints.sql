SELECT `schema`, `table`, `constraint_name`, `column_name`, `constraint_type`, `column_position`, `constraint_expression`, `is_enforced`
FROM (
    SELECT
        s.TABLE_SCHEMA AS `schema`,
        s.TABLE_NAME AS `table`,
        s.INDEX_NAME AS `constraint_name`,
        s.COLUMN_NAME AS `column_name`,
        CASE
            WHEN s.INDEX_NAME = 'PRIMARY' THEN 'PRIMARY KEY'
            WHEN s.NON_UNIQUE = 0 THEN 'UNIQUE INDEX'
            ELSE 'INDEX'
        END AS `constraint_type`,
        s.SEQ_IN_INDEX AS `column_position`,
        '' AS `constraint_expression`,
        NULL AS `is_enforced`
    FROM information_schema.STATISTICS s

    UNION ALL

    SELECT
        tc.CONSTRAINT_SCHEMA AS `schema`,
        tc.TABLE_NAME AS `table`,
        cc.CONSTRAINT_NAME AS `constraint_name`,
        '' AS `column_name`,
        'CHECK' AS `constraint_type`,
        0 AS `column_position`,
        cc.CHECK_CLAUSE AS `constraint_expression`,
        TRUE AS `is_enforced`
    FROM information_schema.CHECK_CONSTRAINTS cc
    JOIN information_schema.TABLE_CONSTRAINTS tc
        ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
        AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'CHECK'
) sub
WHERE 1=1
  /* SYNQ_SCOPE_FILTER */
ORDER BY `schema`, `table`, `constraint_name`, `column_position`
