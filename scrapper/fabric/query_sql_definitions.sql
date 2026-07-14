-- View definitions for Fabric Warehouse.
--
-- Fabric Warehouse has no materialized views, so is_materialized_view is always
-- 0. sys.sql_modules.definition carries the CREATE VIEW text.
SELECT
    s.name                          AS [schema],
    v.name                          AS [table],
    1                               AS is_view,
    0                               AS is_materialized_view,
    m.definition                    AS [sql]
FROM
    {{DB}}.sys.views v
    INNER JOIN {{DB}}.sys.schemas s
        ON v.schema_id = s.schema_id
    INNER JOIN {{DB}}.sys.sql_modules m
        ON v.object_id = m.object_id
WHERE
    v.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name, v.name
