SELECT
    t.OWNER                     AS "schema",
    t.TABLE_NAME                AS "table",
    t.NUM_ROWS                  AS "row_count",
    t.LAST_ANALYZED             AS "updated_at",
    (t.BLOCKS * p.VALUE)        AS "size_bytes"
FROM
    ALL_TABLES t
    CROSS JOIN (SELECT VALUE FROM V$PARAMETER WHERE NAME = 'db_block_size') p
WHERE
    t.OWNER NOT IN (
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT'
    )
    AND t.NESTED = 'NO'
    AND t.SECONDARY = 'N'
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    t.OWNER, t.TABLE_NAME
