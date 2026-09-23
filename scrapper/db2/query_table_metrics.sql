-- Row counts and sizes come from the statistics RUNSTATS (or automatic
-- statistics collection) leaves in the catalog: CARD and NPAGES are -1 until
-- statistics were collected, and are reported as unknown then. STATS_TIME is
-- when they were collected; Db2 keeps no time of the last data change.
SELECT
    CURRENT SERVER                                                   AS "database",
    t.TABSCHEMA                                                      AS "schema",
    t.TABNAME                                                        AS "table",
    CASE WHEN t.CARD >= 0 THEN t.CARD END                            AS "row_count",
    t.STATS_TIME                                                     AS "updated_at",
    CASE WHEN t.NPAGES >= 0 THEN BIGINT(t.NPAGES) * ts.PAGESIZE END  AS "size_bytes"
FROM
    SYSCAT.TABLES t
    LEFT JOIN SYSCAT.TABLESPACES ts
        ON ts.TBSPACE = t.TBSPACE
WHERE
    t.TYPE IN ('T', 'U', 'S')
    AND t.TABSCHEMA NOT LIKE 'SYS%'
    AND t.TABSCHEMA NOT IN ('NULLID', 'SQLJ')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    t.TABSCHEMA, t.TABNAME
