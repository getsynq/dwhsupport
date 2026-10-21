-- Stored procedures and SQL functions for Fabric Warehouse, each with the
-- statement that creates it. Fabric has scalar and inline table-valued
-- functions, no multi-statement ones.
--
-- sys.sql_modules.definition is the CREATE statement as it was written, and is
-- NULL when the connecting principal may not see the definition (VIEW
-- DEFINITION). sys.parameters lists a function's return value as parameter 0.
SELECT
    s.name                                                      AS [schema],
    o.name                                                      AS [name],
    CASE WHEN o.type = 'P' THEN 'PROCEDURE' ELSE 'FUNCTION' END AS [kind],
    o.type_desc                                                 AS [routine_type],
    'SQL'                                                       AS [language],
    args.arguments                                              AS [arguments],
    CASE
        WHEN o.type IN ('IF', 'TF') THEN 'TABLE'
        ELSE ret.return_type
    END                                                         AS [return_type],
    m.definition                                                AS [definition],
    CAST(ep.value AS NVARCHAR(MAX))                             AS [description]
FROM
    {{DB}}.sys.objects o
    INNER JOIN {{DB}}.sys.schemas s
        ON o.schema_id = s.schema_id
    LEFT JOIN {{DB}}.sys.sql_modules m
        ON m.object_id = o.object_id
    LEFT JOIN {{DB}}.sys.extended_properties ep
        ON ep.class = 1 AND ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
    OUTER APPLY (
        SELECT STRING_AGG(CAST(p.name + ' ' + TYPE_NAME(p.user_type_id) AS VARCHAR(MAX)), ', ')
                   WITHIN GROUP (ORDER BY p.parameter_id) AS arguments
        FROM {{DB}}.sys.parameters p
        WHERE p.object_id = o.object_id AND p.parameter_id > 0
    ) args
    OUTER APPLY (
        SELECT TYPE_NAME(p.user_type_id) AS return_type
        FROM {{DB}}.sys.parameters p
        WHERE p.object_id = o.object_id AND p.parameter_id = 0
    ) ret
WHERE
    o.type IN ('P', 'FN', 'IF', 'TF')
    AND o.is_ms_shipped = 0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'cdc', 'queryinsights')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.name, o.name
