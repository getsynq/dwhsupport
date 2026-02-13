SELECT
    o.OWNER                     AS "schema",
    o.OBJECT_NAME               AS "table",
    o.OBJECT_TYPE               AS "table_type",
    tc.COMMENTS                 AS "description",
    CASE
        WHEN o.OBJECT_TYPE = 'VIEW' THEN 1
        ELSE 0
    END                         AS "is_view",
    CASE
        WHEN o.OBJECT_TYPE = 'TABLE' THEN 1
        ELSE 0
    END                         AS "is_table"
FROM
    ALL_OBJECTS o
    LEFT JOIN ALL_TAB_COMMENTS tc
        ON o.OWNER = tc.OWNER AND o.OBJECT_NAME = tc.TABLE_NAME
WHERE
    o.OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
    AND o.OWNER NOT IN (
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT'
    )
ORDER BY
    o.OWNER, o.OBJECT_NAME
