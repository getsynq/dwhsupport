-- The tables whose DDL QuerySqlDefinitions reads with DBMS_METADATA.GET_DDL.
-- Left out: the container table of a materialized view (same owner and name as
-- the view), a nested table's storage, a secondary table of a domain index, an
-- index-organized table's overflow or mapping segment, and a table in the
-- recycle bin. GET_DDL of those either fails or describes something that is
-- not a table of its own.
SELECT
    t.OWNER       AS "schema",
    t.TABLE_NAME  AS "table"
FROM
    ALL_TABLES t
WHERE
    t.OWNER NOT IN (
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT',
        'VECSYS', 'RASADM', 'GSMCATUSER', 'GSMUSER'
    )
    AND t.NESTED = 'NO'
    AND t.SECONDARY = 'N'
    AND (t.IOT_TYPE IS NULL OR t.IOT_TYPE = 'IOT')
    AND t.DROPPED = 'NO'
    AND NOT EXISTS (
        SELECT 1 FROM ALL_MVIEWS mv WHERE mv.OWNER = t.OWNER AND mv.MVIEW_NAME = t.TABLE_NAME
    )
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    t.OWNER, t.TABLE_NAME
