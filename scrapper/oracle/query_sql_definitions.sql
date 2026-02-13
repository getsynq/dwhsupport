SELECT
    v.OWNER                     AS "schema",
    v.VIEW_NAME                 AS "table",
    CASE
        WHEN mv.MVIEW_NAME IS NOT NULL THEN 0
        ELSE 1
    END                         AS "is_view",
    CASE
        WHEN mv.MVIEW_NAME IS NOT NULL THEN 1
        ELSE 0
    END                         AS "is_materialized_view",
    CASE
        WHEN mv.MVIEW_NAME IS NOT NULL THEN mv.QUERY
        ELSE v.TEXT
    END                         AS "sql"
FROM
    ALL_VIEWS v
    LEFT JOIN ALL_MVIEWS mv
        ON v.OWNER = mv.OWNER AND v.VIEW_NAME = mv.MVIEW_NAME
WHERE
    v.OWNER NOT IN (
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT'
    )
ORDER BY
    v.OWNER, v.VIEW_NAME
