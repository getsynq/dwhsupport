SELECT
    q.end_time  AS timestamp,
    q.query_id  AS version,
    q.query_type AS operation
FROM SYS_QUERY_HISTORY q
WHERE q.status = 'success'
  AND q.query_type IN ('INSERT', 'UPDATE', 'DELETE', 'COPY', 'MERGE', 'TRUNCATE', 'DDL')
  AND q.end_time BETWEEN $1 AND $2
  AND q.query_id IN (
      SELECT DISTINCT query_id
      FROM SYS_QUERY_DETAIL
      WHERE LOWER(object_schema) = LOWER($3)
        AND LOWER(object_name) = LOWER($4)
  )
ORDER BY q.end_time DESC
LIMIT $5
