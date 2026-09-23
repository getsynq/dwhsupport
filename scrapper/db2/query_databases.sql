-- A connection is bound to one database, so this is the one it is connected to.
SELECT
    CURRENT SERVER                  AS "database",
    'DB2'                           AS "database_type"
FROM
    SYSIBM.SYSDUMMY1
