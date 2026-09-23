SELECT
    CURRENT SERVER                  AS "database",
    s.SCHEMANAME                    AS "schema",
    s.OWNER                         AS "schema_owner",
    s.REMARKS                       AS "description"
FROM
    SYSCAT.SCHEMATA s
WHERE
    s.SCHEMANAME NOT LIKE 'SYS%'
    AND s.SCHEMANAME NOT IN ('NULLID', 'SQLJ')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.SCHEMANAME
