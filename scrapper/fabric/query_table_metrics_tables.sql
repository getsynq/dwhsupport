-- Base tables to compute row counts for (Fabric Warehouse).
--
-- Only user tables (type 'U'); views are excluded. The row counts themselves
-- are computed by a separate batched COUNT_BIG(*) pass — see
-- query_table_metrics.go for why Fabric can't use page-based storage stats.
SELECT
    s.name                          AS [schema],
    t.name                          AS [table]
FROM
    {{DB}}.sys.tables t
    INNER JOIN {{DB}}.sys.schemas s
        ON t.schema_id = s.schema_id
WHERE
    t.is_ms_shipped = 0
    AND t.type = 'U'
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name, t.name
