WITH history AS (
/* SYS_QUERY_HISTORY */
)
SELECT h.*,
       u.usename  AS user_name,
       t.sequence AS text_sequence,
       t.text     AS text_chunk
FROM history h
LEFT JOIN pg_user u ON u.usesysid = h.user_id
-- query_text is only the first chunk of the statement; one row per chunk follows for any statement
-- whose first chunk is long enough that it may have been cut.
LEFT JOIN SYS_QUERY_TEXT t ON OCTET_LENGTH(h.query_text) >= $3 AND t.query_id = h.query_id
ORDER BY h.end_time, h.query_id, t.sequence
