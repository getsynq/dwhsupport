-- Database listing for Fabric Warehouse.
--
-- A Fabric connection is bound to a single Warehouse/Lakehouse item and Fabric
-- does not support cross-database queries, so the only database this connection
-- can address is the one it is attached to. Returning DB_NAME() keeps the
-- (database, schema, table) hierarchy internally consistent with the catalog
-- queries, which all key off DB_NAME().
SELECT
    DB_NAME()                       AS [database],
    CAST(NULL AS VARCHAR(128))      AS database_owner
