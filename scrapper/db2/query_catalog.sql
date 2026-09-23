-- Schemas starting with SYS are reserved for the system (a user schema cannot
-- be created with that prefix); NULLID and SQLJ hold the packages of the
-- client drivers.
SELECT
    CURRENT SERVER                                             AS "database",
    c.TABSCHEMA                                                AS "schema",
    c.TABNAME                                                  AS "table",
    CASE WHEN t.TYPE IN ('V', 'W') THEN 1 ELSE 0 END           AS "is_view",
    CASE WHEN t.TYPE IN ('T', 'U', 'G') THEN 1 ELSE 0 END      AS "is_table",
    CASE WHEN t.TYPE = 'S' THEN 1 ELSE 0 END                   AS "is_materialized_view",
    t.REMARKS                                                  AS "table_comment",
    c.COLNAME                                                  AS "column",
    c.COLNO + 1                                                AS "position",
    RTRIM(c.TYPENAME) ||
        CASE
            WHEN c.TYPENAME IN ('CHARACTER', 'VARCHAR', 'LONG VARCHAR', 'GRAPHIC', 'VARGRAPHIC',
                                'BINARY', 'VARBINARY', 'CLOB', 'BLOB', 'DBCLOB')
                THEN '(' || VARCHAR(c.LENGTH) || ')'
                    || CASE WHEN c.CODEPAGE = 0 AND c.TYPENAME IN ('CHARACTER', 'VARCHAR') THEN ' FOR BIT DATA' ELSE '' END
            WHEN c.TYPENAME = 'DECIMAL'
                THEN '(' || VARCHAR(c.LENGTH) || ',' || VARCHAR(c.SCALE) || ')'
            WHEN c.TYPENAME = 'DECFLOAT'
                THEN CASE c.LENGTH WHEN 8 THEN '(16)' ELSE '(34)' END
            WHEN c.TYPENAME = 'TIMESTAMP' AND c.SCALE <> 6
                THEN '(' || VARCHAR(c.SCALE) || ')'
            ELSE ''
        END                                                    AS "type",
    c.REMARKS                                                  AS "comment"
FROM
    SYSCAT.COLUMNS c
    INNER JOIN SYSCAT.TABLES t
        ON t.TABSCHEMA = c.TABSCHEMA AND t.TABNAME = c.TABNAME
WHERE
    t.TYPE IN ('T', 'U', 'G', 'V', 'W', 'S', 'N')
    AND c.TABSCHEMA NOT LIKE 'SYS%'
    AND c.TABSCHEMA NOT IN ('NULLID', 'SQLJ')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    c.TABSCHEMA, c.TABNAME, c.COLNO
