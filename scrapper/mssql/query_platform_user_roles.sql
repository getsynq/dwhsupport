-- Server roles, and the roles of the connected database prefixed with its
-- name (a login holds different roles in each database). A database user is
-- reported under the login it maps to.
SELECT m.name AS login, r.name AS role
FROM sys.server_role_members rm
JOIN sys.server_principals r ON r.principal_id = rm.role_principal_id
JOIN sys.server_principals m ON m.principal_id = rm.member_principal_id
UNION ALL
SELECT COALESCE(sp.name, u.name), DB_NAME() + '.' + r.name
FROM sys.database_role_members drm
JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
JOIN sys.database_principals u ON u.principal_id = drm.member_principal_id
LEFT JOIN sys.server_principals sp ON sp.sid = u.sid
