-- Schema listing for Fabric Warehouse.
--
-- db[_]% (literal underscore) drops the fixed database-role schemas
-- (db_owner, db_datareader, …) while keeping the default 'dbo' schema, which
-- can legitimately hold user tables.
SELECT
    s.name                          AS [schema],
    CAST(NULL AS VARCHAR(128))      AS schema_owner
FROM
    {{DB}}.sys.schemas s
WHERE
    s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    AND s.name NOT LIKE 'db[_]%'
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name
