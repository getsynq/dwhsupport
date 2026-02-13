SELECT
    c.OWNER                                                    AS "schema",
    c.TABLE_NAME                                               AS "table",
    CASE
        WHEN o.OBJECT_TYPE IN ('VIEW', 'MATERIALIZED VIEW') THEN 1
        ELSE 0
    END                                                        AS "is_view",
    tc.COMMENTS                                                AS "table_comment",
    c.COLUMN_NAME                                              AS "column",
    c.COLUMN_ID                                                AS "position",
    c.DATA_TYPE ||
        CASE
            WHEN c.DATA_TYPE IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'RAW')
                THEN '(' || c.DATA_LENGTH || ')'
            WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NOT NULL
                THEN '(' || c.DATA_PRECISION ||
                    CASE WHEN c.DATA_SCALE > 0 THEN ',' || c.DATA_SCALE ELSE '' END
                || ')'
            WHEN c.DATA_TYPE = 'FLOAT'
                THEN '(' || c.DATA_PRECISION || ')'
            ELSE ''
        END                                                    AS "type",
    cc.COMMENTS                                                AS "comment"
FROM
    ALL_TAB_COLUMNS c
    INNER JOIN ALL_OBJECTS o
        ON c.OWNER = o.OWNER AND c.TABLE_NAME = o.OBJECT_NAME
        AND o.OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
    LEFT JOIN ALL_TAB_COMMENTS tc
        ON c.OWNER = tc.OWNER AND c.TABLE_NAME = tc.TABLE_NAME
    LEFT JOIN ALL_COL_COMMENTS cc
        ON c.OWNER = cc.OWNER AND c.TABLE_NAME = cc.TABLE_NAME AND c.COLUMN_NAME = cc.COLUMN_NAME
WHERE
    c.OWNER NOT IN (
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT'
    )
    AND c.COLUMN_ID IS NOT NULL
    AND c.HIDDEN_COLUMN = 'NO'
ORDER BY
    c.OWNER, c.TABLE_NAME, c.COLUMN_ID
