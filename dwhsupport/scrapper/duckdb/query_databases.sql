SELECT database_name as "database", comment as "description", type as "database_type", path as "database_owner"
FROM duckdb_databases()
where not internal