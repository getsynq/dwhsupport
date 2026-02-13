package sqldialect

type TestedDialect struct {
	Name    string
	Dialect Dialect
}

func DialectsToTest() []*TestedDialect {
	return []*TestedDialect{
		{"clickhouse", NewClickHouseDialect()},
		{"snowflake", NewSnowflakeDialect()},
		{"redshift", NewRedshiftDialect()},
		{"bigquery", NewBigQueryDialect()},
		{"postgres", NewPostgresDialect()},
		{"mysql", NewMySQLDialect()},
		{"databricks", NewDatabricksDialect()},
		{"duckdb", NewDuckDBDialect()},
		{"trino", NewTrinoDialect()},
		{"oracle", NewOracleDialect()},
		{"mssql", NewMSSQLDialect()},
	}
}
