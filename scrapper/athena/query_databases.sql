SELECT DISTINCT
    schema_name AS database
FROM information_schema.schemata
WHERE schema_name NOT IN ('information_schema')
