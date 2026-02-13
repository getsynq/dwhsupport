SELECT
    t.OWNER                     AS "schema",
    t.TABLE_NAME                AS "table",
    t.NUM_ROWS                  AS "row_count",
    t.LAST_ANALYZED             AS "updated_at",
    s.BYTES                     AS "size_bytes"
FROM
    ALL_TABLES t
    LEFT JOIN ALL_SEGMENTS s
        ON t.OWNER = s.OWNER AND t.TABLE_NAME = s.SEGMENT_NAME
        AND s.SEGMENT_TYPE IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
WHERE
    t.OWNER NOT IN (
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT'
    )
    AND t.NESTED = 'NO'
    AND t.SECONDARY = 'N'
ORDER BY
    t.OWNER, t.TABLE_NAME
