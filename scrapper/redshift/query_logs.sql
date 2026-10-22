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
-- SYS_QUERY_TEXT holds a second chunk of. Its length cannot tell: Redshift counts each pipe twice
-- towards a chunk's 4000 bytes, so a chunk can hold far fewer bytes than that.
LEFT JOIN (SELECT DISTINCT query_id FROM SYS_QUERY_TEXT WHERE sequence = 1) multi ON multi.query_id = h.query_id
LEFT JOIN SYS_QUERY_TEXT t ON t.query_id = multi.query_id
ORDER BY h.end_time, h.query_id, t.sequence
