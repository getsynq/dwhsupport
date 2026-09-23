-- The package cache: one row per cached statement and member, with metrics
-- accumulated over every execution since it entered the cache, as in Oracle's
-- V$SQL. A statement ages out of the cache under memory pressure.
-- Timestamps are the server's local time; subtracting CURRENT TIMEZONE turns
-- them into UTC, which the bounds (?, ?) are in.
SELECT
    HEX(EXECUTABLE_ID)                              AS EXECUTABLE_ID,
    STMTID                                          AS STMTID,
    PLANID                                          AS PLANID,
    MEMBER                                          AS MEMBER,
    SECTION_TYPE                                    AS SECTION_TYPE,
    RTRIM(PACKAGE_SCHEMA)                           AS PACKAGE_SCHEMA,
    RTRIM(PACKAGE_NAME)                             AS PACKAGE_NAME,
    STMT_TYPE_ID                                    AS STMT_TYPE_ID,
    INSERT_TIMESTAMP - CURRENT TIMEZONE             AS INSERT_TIMESTAMP,
    LAST_METRICS_UPDATE - CURRENT TIMEZONE          AS LAST_METRICS_UPDATE,
    NUM_EXECUTIONS                                  AS NUM_EXECUTIONS,
    NUM_EXEC_WITH_METRICS                           AS NUM_EXEC_WITH_METRICS,
    TOTAL_ACT_TIME                                  AS TOTAL_ACT_TIME,
    TOTAL_ACT_WAIT_TIME                             AS TOTAL_ACT_WAIT_TIME,
    TOTAL_CPU_TIME                                  AS TOTAL_CPU_TIME,
    ROWS_READ                                       AS ROWS_READ,
    ROWS_RETURNED                                   AS ROWS_RETURNED,
    ROWS_MODIFIED                                   AS ROWS_MODIFIED,
    TOTAL_SORTS                                     AS TOTAL_SORTS,
    POOL_DATA_L_READS + POOL_INDEX_L_READS          AS LOGICAL_READS,
    POOL_DATA_P_READS + POOL_INDEX_P_READS          AS PHYSICAL_READS,
    QUERY_COST_ESTIMATE                             AS QUERY_COST_ESTIMATE,
    CURRENT SERVER                                  AS DATABASE_NAME,
    STMT_TEXT                                       AS STMT_TEXT
FROM
    TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2))
WHERE
    NUM_EXECUTIONS > 0
    AND LAST_METRICS_UPDATE - CURRENT TIMEZONE >= TIMESTAMP(CAST(? AS VARCHAR(26)))
    AND LAST_METRICS_UPDATE - CURRENT TIMEZONE < TIMESTAMP(CAST(? AS VARCHAR(26)))
    AND COALESCE(PACKAGE_SCHEMA, '') NOT LIKE 'SYS%'
ORDER BY
    LAST_METRICS_UPDATE
