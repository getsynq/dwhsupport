SELECT
    s.sql_id,
    t.sql_text                              AS sql_fulltext,
    s.parsing_schema_name,
    sn.end_interval_time                    AS last_active_time,
    sn.begin_interval_time                  AS interval_start,
    s.executions_delta                      AS executions,
    s.elapsed_time_delta                    AS elapsed_time,
    s.cpu_time_delta                        AS cpu_time,
    s.disk_reads_delta                      AS disk_reads,
    s.buffer_gets_delta                     AS buffer_gets,
    s.rows_processed_delta                  AS rows_processed,
    s.module
FROM DBA_HIST_SQLSTAT s
JOIN DBA_HIST_SQLTEXT t
    ON s.sql_id = t.sql_id AND s.dbid = t.dbid
JOIN DBA_HIST_SNAPSHOT sn
    ON s.snap_id = sn.snap_id
    AND s.dbid = sn.dbid
    AND s.instance_number = sn.instance_number
WHERE sn.end_interval_time >= TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')
  AND sn.end_interval_time < TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS')
  AND s.parsing_schema_name NOT IN (
      'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
      'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
      'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
      'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT',
      'VECSYS', 'RASADM', 'GSMCATUSER', 'GSMUSER'
  )
  AND s.executions_delta > 0
ORDER BY sn.end_interval_time
