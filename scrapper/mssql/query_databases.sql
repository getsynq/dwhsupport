SELECT
    d.name                              AS [database],
    sp.name                             AS database_owner
FROM
    sys.databases d
    LEFT JOIN sys.server_principals sp
        ON d.owner_sid = sp.sid
WHERE
    d.database_id > 4
    AND d.state_desc = 'ONLINE'
    AND d.name NOT IN ('master', 'tempdb', 'model', 'msdb')
ORDER BY
    d.name
