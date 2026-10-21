SELECT "schema", "table", "constraint_name", "column_name", "constraint_type", "column_position", "constraint_expression", "is_enforced",
    "referenced_schema", "referenced_table", "referenced_column_name"
FROM (
    SELECT
        cc.owner AS "schema",
        cc.table_name AS "table",
        cc.constraint_name AS "constraint_name",
        cc.column_name AS "column_name",
        CASE c.constraint_type
            WHEN 'P' THEN 'PRIMARY KEY'
            WHEN 'U' THEN 'UNIQUE INDEX'
        END AS "constraint_type",
        cc.position AS "column_position",
        '' AS "constraint_expression",
        CASE WHEN c.status = 'ENABLED' AND c.validated = 'VALIDATED' THEN 1 ELSE 0 END AS "is_enforced",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_schema",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_table",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_column_name"
    FROM all_constraints c
    JOIN all_cons_columns cc
        ON c.constraint_name = cc.constraint_name
        AND c.owner = cc.owner
    WHERE c.constraint_type IN ('P', 'U')

    UNION ALL

    -- foreign keys, one row per column, each with the column at the same
    -- position of the key it references
    SELECT
        cc.owner AS "schema",
        cc.table_name AS "table",
        cc.constraint_name AS "constraint_name",
        cc.column_name AS "column_name",
        'FOREIGN KEY' AS "constraint_type",
        cc.position AS "column_position",
        '' AS "constraint_expression",
        CASE WHEN c.status = 'ENABLED' AND c.validated = 'VALIDATED' THEN 1 ELSE 0 END AS "is_enforced",
        pc.owner AS "referenced_schema",
        pc.table_name AS "referenced_table",
        pc.column_name AS "referenced_column_name"
    FROM all_constraints c
    JOIN all_cons_columns cc
        ON c.constraint_name = cc.constraint_name
        AND c.owner = cc.owner
    JOIN all_cons_columns pc
        ON pc.owner = c.r_owner
        AND pc.constraint_name = c.r_constraint_name
        AND pc.position = cc.position
    WHERE c.constraint_type = 'R'

    UNION ALL

    SELECT
        c.owner AS "schema",
        c.table_name AS "table",
        c.constraint_name AS "constraint_name",
        '' AS "column_name",
        'CHECK' AS "constraint_type",
        0 AS "column_position",
        c.search_condition_vc AS "constraint_expression",
        CASE WHEN c.status = 'ENABLED' AND c.validated = 'VALIDATED' THEN 1 ELSE 0 END AS "is_enforced",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_schema",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_table",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_column_name"
    FROM all_constraints c
    WHERE c.constraint_type = 'C'
        AND c.constraint_name NOT LIKE 'SYS_%'

    UNION ALL

    -- indexes that back no primary key or unique constraint, one row per column.
    -- A function-based index lists a hidden SYS_NC column for each expression,
    -- and the expressions are LONG, which a UNION cannot carry, so it is left
    -- out, as are LOB, IOT, domain and cluster indexes, which are Oracle's own.
    SELECT
        i.table_owner AS "schema",
        i.table_name AS "table",
        i.index_name AS "constraint_name",
        ic.column_name AS "column_name",
        CASE i.uniqueness WHEN 'UNIQUE' THEN 'UNIQUE INDEX' ELSE 'INDEX' END AS "constraint_type",
        ic.column_position AS "column_position",
        '' AS "constraint_expression",
        CASE WHEN i.status IN ('VALID', 'N/A') THEN 1 ELSE 0 END AS "is_enforced",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_schema",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_table",
        CAST(NULL AS VARCHAR2(128)) AS "referenced_column_name"
    FROM all_indexes i
    JOIN all_ind_columns ic
        ON ic.index_owner = i.owner
        AND ic.index_name = i.index_name
    WHERE i.index_type IN ('NORMAL', 'NORMAL/REV', 'BITMAP')
        AND i.table_type = 'TABLE'
        AND i.dropped = 'NO'
        AND NOT EXISTS (
            SELECT 1 FROM all_constraints c
            WHERE c.index_owner = i.owner
                AND c.index_name = i.index_name
                AND c.constraint_type IN ('P', 'U')
        )
)
WHERE "schema" NOT IN (
    'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
    'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
    'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
    'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT',
    'VECSYS', 'RASADM', 'GSMCATUSER', 'GSMUSER'
)
    /* SYNQ_SCOPE_FILTER */
ORDER BY "schema", "table", "constraint_name", "column_position"
