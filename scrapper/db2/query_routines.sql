-- Stored procedures and user-defined functions. SYSCAT.ROUTINES.TEXT is the
-- full CREATE statement of an SQL routine, and NULL for an external or sourced
-- one, which has no body here. Built-in and system-generated routines
-- (ORIGIN 'B' and 'S') and methods are left out.
--
-- A routine name can be overloaded, one row per SPECIFICNAME. Parameters are
-- in SYSCAT.ROUTINEPARMS: P, O and B rows are the arguments, the C row is a
-- scalar function's result.
SELECT
    CURRENT SERVER                                                    AS "database",
    r.ROUTINESCHEMA                                                   AS "schema",
    r.ROUTINENAME                                                     AS "name",
    CASE WHEN r.ROUTINETYPE = 'P' THEN 'PROCEDURE' ELSE 'FUNCTION' END AS "kind",
    CASE
        WHEN r.ROUTINETYPE = 'P' THEN 'PROCEDURE'
        WHEN r.FUNCTIONTYPE = 'T' THEN 'TABLE FUNCTION'
        WHEN r.FUNCTIONTYPE = 'R' THEN 'ROW FUNCTION'
        WHEN r.FUNCTIONTYPE = 'C' THEN 'AGGREGATE FUNCTION'
        ELSE 'SCALAR FUNCTION'
    END                                                               AS "routine_type",
    NULLIF(TRIM(r.LANGUAGE), '')                                      AS "language",
    (
        SELECT LISTAGG(COALESCE(p.PARMNAME || ' ', '') || p.TYPENAME, ', ') WITHIN GROUP (ORDER BY p.ORDINAL)
        FROM SYSCAT.ROUTINEPARMS p
        WHERE p.ROUTINESCHEMA = r.ROUTINESCHEMA
          AND p.SPECIFICNAME = r.SPECIFICNAME
          AND p.ROWTYPE IN ('P', 'O', 'B')
    )                                                                 AS "arguments",
    CASE
        WHEN r.ROUTINETYPE = 'P' THEN NULL
        WHEN r.FUNCTIONTYPE = 'T' THEN 'TABLE'
        ELSE (
            SELECT MIN(p.TYPENAME)
            FROM SYSCAT.ROUTINEPARMS p
            WHERE p.ROUTINESCHEMA = r.ROUTINESCHEMA
              AND p.SPECIFICNAME = r.SPECIFICNAME
              AND p.ROWTYPE = 'C'
        )
    END                                                               AS "return_type",
    r.TEXT                                                            AS "definition",
    r.REMARKS                                                         AS "description"
FROM
    SYSCAT.ROUTINES r
WHERE
    r.ROUTINETYPE IN ('F', 'P')
    AND r.ORIGIN NOT IN ('B', 'S')
    AND r.ROUTINESCHEMA NOT LIKE 'SYS%'
    AND r.ROUTINESCHEMA NOT IN ('NULLID', 'SQLJ')
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    r.ROUTINESCHEMA, r.ROUTINENAME, r.SPECIFICNAME
