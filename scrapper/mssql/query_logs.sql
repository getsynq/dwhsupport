SELECT
    DB_NAME()                                    AS [database],
    q.query_id,
    CAST(q.query_hash AS BIGINT)            AS query_hash,
    qt.query_sql_text,
    rs.execution_type,
    rs.count_executions,
    rs.first_execution_time,
    rs.last_execution_time,
    rs.avg_duration                          AS avg_duration_us,
    rs.last_duration                         AS last_duration_us,
    rs.avg_cpu_time                          AS avg_cpu_time_us,
    rs.avg_logical_io_reads,
    rs.avg_logical_io_writes,
    rs.avg_physical_io_reads,
    rs.avg_rowcount,
    rs.avg_query_max_used_memory             AS avg_query_max_used_memory_kb,
    p.plan_id,
    rsi.start_time                           AS interval_start,
    rsi.end_time                             AS interval_end
FROM sys.query_store_runtime_stats rs
JOIN sys.query_store_plan p
    ON rs.plan_id = p.plan_id
JOIN sys.query_store_query q
    ON p.query_id = q.query_id
JOIN sys.query_store_query_text qt
    ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_runtime_stats_interval rsi
    ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
WHERE rs.last_execution_time >= @p1
  AND rs.last_execution_time < @p2
ORDER BY rs.last_execution_time
