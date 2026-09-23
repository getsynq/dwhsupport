-- SYSCAT.VIEWS holds both views and materialized query tables, each with the
-- full statement that created it (CREATE VIEW ... / CREATE TABLE ... AS).
SELECT
    CURRENT SERVER                                   AS "database",
    v.VIEWSCHEMA                                     AS "schema",
    v.VIEWNAME                                       AS "table",
    CASE WHEN t.TYPE = 'S' THEN 0 ELSE 1 END         AS "is_view",
    CASE WHEN t.TYPE = 'S' THEN 1 ELSE 0 END         AS "is_materialized_view",
    v.TEXT                                           AS "sql"
FROM
    SYSCAT.VIEWS v
    INNER JOIN SYSCAT.TABLES t
        ON t.TABSCHEMA = v.VIEWSCHEMA AND t.TABNAME = v.VIEWNAME
WHERE
    v.VIEWSCHEMA NOT LIKE 'SYS%'
    AND v.VIEWSCHEMA NOT IN ('NULLID', 'SQLJ')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    v.VIEWSCHEMA, v.VIEWNAME
