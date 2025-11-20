SELECT
    query_id,
    state,
    "user",
    source,
    query,
    resource_group_id,
    queued_time_ms,
    analysis_time_ms,
    planning_time_ms,
    created,
    started,
    last_heartbeat,
    "end",
    error_type,
    error_code
FROM system.runtime.queries
WHERE "end" BETWEEN ? AND ?
