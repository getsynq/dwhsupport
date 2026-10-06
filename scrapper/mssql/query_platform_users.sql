-- Server logins, plus the users of the connected database that authenticate
-- without one (contained users, Entra users of Azure SQL Database). A user
-- mapped to a login is that login and is not listed twice. Roles are not
-- logins, and the ##...## logins are the server's own certificate and policy
-- logins.
--
-- create_date is the server's local time; it is shifted to UTC by the
-- server's current offset.
SELECT
    sp.name                                                                  AS login,
    CONVERT(varchar(200), sp.sid, 1)                                         AS platform_id,
    sp.type_desc                                                             AS type,
    sp.is_disabled                                                           AS disabled,
    DATEADD(MINUTE, DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()), sp.create_date) AS created_at
FROM sys.server_principals sp
WHERE sp.type IN ('S', 'U', 'G', 'E', 'X')
  AND sp.name NOT LIKE '##%##'
UNION ALL
SELECT
    dp.name,
    CONVERT(varchar(200), dp.sid, 1),
    dp.type_desc,
    NULL,
    DATEADD(MINUTE, DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()), dp.create_date)
FROM sys.database_principals dp
WHERE dp.type IN ('S', 'U', 'G', 'E', 'X')
  AND dp.authentication_type IN (2, 3, 4)
  AND dp.sid IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM sys.server_principals sp WHERE sp.sid = dp.sid)
