package metrics

import dwhsql "github.com/getsynq/dwhsupport/sqldialect"

type TestedDialect struct {
	Name    string
	Dialect dwhsql.Dialect
}

func DialectsToTest() []*TestedDialect {
	return []*TestedDialect{
		{"clickhouse", dwhsql.NewClickHouseDialect()},
		{"snowflake", dwhsql.NewSnowflakeDialect()},
		{"redshift", dwhsql.NewRedshiftDialect()},
		{"bigquery", dwhsql.NewBigQueryDialect()},
		{"postgres", dwhsql.NewPostgresDialect()},
		{"mysql", dwhsql.NewMySQLDialect()},
		{"databricks", dwhsql.NewDatabricksDialect()},
		{"duckdb", dwhsql.NewDuckDBDialect()},
	}
}
