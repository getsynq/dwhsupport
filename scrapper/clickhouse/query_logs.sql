SELECT type,
       event_time_microseconds,
       query_start_time_microseconds,
       read_rows,
       read_bytes,
       written_rows,
       written_bytes,
       result_rows,
       result_bytes,
       memory_usage,
       current_database,
       query_kind,
       databases,
       tables,
       columns,
       projections,
       views,
       exception_code,
       exception,
       stack_trace,
       initial_user,
       initial_query_id,
       initial_address,
       initial_port,
       initial_query_start_time_microseconds,
       os_user,
       client_hostname,
       client_name,
       client_revision,
       client_version_major,
       client_version_minor,
       client_version_patch,
       distributed_depth,
       normalizeQuery(query) as normalized_query

FROM
    clusterAllReplicas(default, system.query_log)

WHERE type in ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
  AND is_initial_query = true
  AND event_time between ? and ?
  AND notEmpty(tables)
