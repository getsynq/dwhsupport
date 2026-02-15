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
    s.SEQ_IN_INDEX AS `column_position`
FROM information_schema.STATISTICS s
WHERE s.TABLE_SCHEMA = ?
    AND s.TABLE_NAME = ?
ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX
