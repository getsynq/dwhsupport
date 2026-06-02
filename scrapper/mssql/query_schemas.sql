SELECT
    DB_NAME()                       AS [database],
    s.name                          AS [schema],
    dp.name                         AS schema_owner
FROM
    sys.schemas s
    LEFT JOIN sys.database_principals dp
        ON s.principal_id = dp.principal_id
WHERE
    s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc')
    AND s.name NOT LIKE 'db_%'
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name
