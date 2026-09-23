-- SYSCAT.TABLES types: T table, U typed table, G created temporary table,
-- V view, W typed view, S materialized query table, N nickname. Aliases (A),
-- hierarchy tables (H) and detached partitions (L) are left out.
SELECT
    CURRENT SERVER                  AS "database",
    t.TABSCHEMA                     AS "schema",
    t.TABNAME                       AS "table",
    CASE t.TYPE
        WHEN 'T' THEN 'TABLE'
        WHEN 'U' THEN 'TABLE'
        WHEN 'G' THEN 'TEMPORARY TABLE'
        WHEN 'V' THEN 'VIEW'
        WHEN 'W' THEN 'VIEW'
        WHEN 'S' THEN 'MATERIALIZED VIEW'
        WHEN 'N' THEN 'NICKNAME'
    END                             AS "table_type",
    t.REMARKS                       AS "description",
    CASE WHEN t.TYPE IN ('V', 'W') THEN 1 ELSE 0 END      AS "is_view",
    CASE WHEN t.TYPE IN ('T', 'U', 'G') THEN 1 ELSE 0 END AS "is_table",
    CASE WHEN t.TYPE = 'S' THEN 1 ELSE 0 END              AS "is_materialized_view"
FROM
    SYSCAT.TABLES t
WHERE
    t.TYPE IN ('T', 'U', 'G', 'V', 'W', 'S', 'N')
    AND t.TABSCHEMA NOT LIKE 'SYS%'
    AND t.TABSCHEMA NOT IN ('NULLID', 'SQLJ')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    t.TABSCHEMA, t.TABNAME
