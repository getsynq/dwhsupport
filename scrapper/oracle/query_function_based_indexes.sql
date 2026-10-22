-- Function-based indexes that back no primary key or unique constraint, one row
-- per index column. A column that is an expression is listed under a hidden
-- SYS_NC name, with the expression in all_ind_expressions. The expression is a
-- LONG, which no UNION, inline view, WHERE or ORDER BY can carry, so this is a
-- query of its own, its rows in no particular order.
SELECT
    i.table_owner AS "schema",
    i.table_name AS "table",
    i.index_name AS "constraint_name",
    ic.column_name AS "column_name",
    ie.column_expression AS "column_expression",
    CASE i.uniqueness WHEN 'UNIQUE' THEN 'UNIQUE INDEX' ELSE 'INDEX' END AS "constraint_type",
    ic.column_position AS "column_position",
    CASE WHEN i.status IN ('VALID', 'N/A') AND i.funcidx_status = 'ENABLED' THEN 1 ELSE 0 END AS "is_enforced",
    i.index_type AS "index_type"
FROM all_indexes i
JOIN all_ind_columns ic
    ON ic.index_owner = i.owner
    AND ic.index_name = i.index_name
LEFT JOIN all_ind_expressions ie
    ON ie.index_owner = ic.index_owner
    AND ie.index_name = ic.index_name
    AND ie.column_position = ic.column_position
WHERE i.index_type LIKE 'FUNCTION-BASED%'
    AND i.table_type = 'TABLE'
    AND i.dropped = 'NO'
    AND NOT EXISTS (
        SELECT 1 FROM all_constraints c
        WHERE c.index_owner = i.owner
            AND c.index_name = i.index_name
            AND c.constraint_type IN ('P', 'U')
    )
    AND i.table_owner NOT IN (
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT',
        'VECSYS', 'RASADM', 'GSMCATUSER', 'GSMUSER'
    )
    /* SYNQ_SCOPE_FILTER */
