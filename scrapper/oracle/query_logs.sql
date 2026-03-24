SELECT
    sql_id,
    sql_fulltext,
    parsing_schema_name,
    last_active_time,
    executions,
    elapsed_time,
    cpu_time,
    disk_reads,
    buffer_gets,
    rows_processed,
    command_type,
    module,
    action,
    optimizer_cost,
    fetches,
    sorts
FROM V$SQL
WHERE last_active_time >= TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')
  AND last_active_time < TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS')
  AND parsing_schema_name NOT IN (
      'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
      'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
      'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
      'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT'
  )
  AND executions > 0
ORDER BY last_active_time
