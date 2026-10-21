SELECT "schema", "table", "constraint_name", "column_name", "constraint_type", "column_position", "constraint_expression", "is_enforced",
    "referenced_schema", "referenced_table", "referenced_column_name"
FROM (
    -- primary keys, unique and foreign key constraints, one row per column; a
    -- foreign key column names the column at the same position of the parent key
    SELECT
        c.TABSCHEMA AS "schema",
        c.TABNAME AS "table",
        c.CONSTNAME AS "constraint_name",
        k.COLNAME AS "column_name",
        CASE c.TYPE
            WHEN 'P' THEN 'PRIMARY KEY'
            WHEN 'U' THEN 'UNIQUE INDEX'
            WHEN 'F' THEN 'FOREIGN KEY'
        END AS "constraint_type",
        k.COLSEQ AS "column_position",
        CAST('' AS VARCHAR(1)) AS "constraint_expression",
        CASE WHEN c.ENFORCED = 'Y' THEN 1 ELSE 0 END AS "is_enforced",
        r.REFTABSCHEMA AS "referenced_schema",
        r.REFTABNAME AS "referenced_table",
        rk.COLNAME AS "referenced_column_name"
    FROM SYSCAT.TABCONST c
    JOIN SYSCAT.KEYCOLUSE k
        ON k.CONSTNAME = c.CONSTNAME AND k.TABSCHEMA = c.TABSCHEMA AND k.TABNAME = c.TABNAME
    LEFT JOIN SYSCAT.REFERENCES r
        ON c.TYPE = 'F' AND r.CONSTNAME = c.CONSTNAME AND r.TABSCHEMA = c.TABSCHEMA AND r.TABNAME = c.TABNAME
    LEFT JOIN SYSCAT.KEYCOLUSE rk
        ON rk.CONSTNAME = r.REFKEYNAME AND rk.TABSCHEMA = r.REFTABSCHEMA AND rk.TABNAME = r.REFTABNAME AND rk.COLSEQ = k.COLSEQ
    WHERE c.TYPE IN ('P', 'U', 'F')

    UNION ALL

    -- check constraints
    SELECT
        c.TABSCHEMA,
        c.TABNAME,
        c.CONSTNAME,
        CAST('' AS VARCHAR(1)),
        'CHECK',
        0,
        CAST(ch.TEXT AS VARCHAR(32000)),
        CASE WHEN c.ENFORCED = 'Y' THEN 1 ELSE 0 END,
        CAST(NULL AS VARCHAR(128)),
        CAST(NULL AS VARCHAR(128)),
        CAST(NULL AS VARCHAR(128))
    FROM SYSCAT.TABCONST c
    JOIN SYSCAT.CHECKS ch
        ON ch.CONSTNAME = c.CONSTNAME AND ch.TABSCHEMA = c.TABSCHEMA AND ch.TABNAME = c.TABNAME
    WHERE c.TYPE = 'K' AND ch.TYPE = 'C'

    UNION ALL

    -- indexes that do not back a constraint (UNIQUERULE P backs a primary key)
    SELECT
        i.TABSCHEMA,
        i.TABNAME,
        i.INDNAME,
        ic.COLNAME,
        CASE i.UNIQUERULE WHEN 'U' THEN 'UNIQUE INDEX' ELSE 'INDEX' END,
        ic.COLSEQ,
        CAST('' AS VARCHAR(1)),
        1,
        CAST(NULL AS VARCHAR(128)),
        CAST(NULL AS VARCHAR(128)),
        CAST(NULL AS VARCHAR(128))
    FROM SYSCAT.INDEXES i
    JOIN SYSCAT.INDEXCOLUSE ic
        ON ic.INDSCHEMA = i.INDSCHEMA AND ic.INDNAME = i.INDNAME
    WHERE i.UNIQUERULE IN ('D', 'U')
        AND i.SYSTEM_REQUIRED = 0
        AND NOT EXISTS (
            SELECT 1 FROM SYSCAT.CONSTDEP d
            WHERE d.BSCHEMA = i.INDSCHEMA AND d.BNAME = i.INDNAME AND d.BTYPE = 'I'
        )

    UNION ALL

    -- range partitioning keys
    SELECT
        p.TABSCHEMA,
        p.TABNAME,
        CAST('PARTITION BY' AS VARCHAR(128)),
        CAST(p.DATAPARTITIONEXPRESSION AS VARCHAR(128)),
        'PARTITION BY',
        p.DATAPARTITIONKEYSEQ,
        CAST('' AS VARCHAR(1)),
        1,
        CAST(NULL AS VARCHAR(128)),
        CAST(NULL AS VARCHAR(128)),
        CAST(NULL AS VARCHAR(128))
    FROM SYSCAT.DATAPARTITIONEXPRESSION p
) x
WHERE "schema" NOT LIKE 'SYS%'
    AND "schema" NOT IN ('NULLID', 'SQLJ')
    /* SYNQ_SCOPE_FILTER */
ORDER BY "schema", "table", "constraint_name", "column_position"
