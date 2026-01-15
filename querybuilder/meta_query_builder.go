package querybuilder

import (
	"fmt"

	. "github.com/getsynq/dwhsupport/sqldialect"
)

// MetaQueryBuilder provides dialect specific ways to query table metadata
type MetaQueryBuilder struct {
	table TableExpr
}

func NewMetaQueryBuilder(table TableExpr) *MetaQueryBuilder {
	return &MetaQueryBuilder{
		table: table,
	}
}

func (m *MetaQueryBuilder) ToSql(dialect Dialect) (string, error) {
	switch dialect.(type) {
	case *PostgresDialect:
		return postgres(m.table)
	case *TrinoDialect:
		return trino(m.table)
	case *BigQueryDialect:
		return bigquery(m.table)
	case *RedshiftDialect:
		return redshift(m.table)
	case *SnowflakeDialect:
		return snowflake(m.table)
	case *ClickHouseDialect:
		return clickhouse(m.table)
	case *DuckDBDialect:
		return duckdb(m.table)
	case *MySQLDialect:
		return mysql(m.table)

	default:
		return "", fmt.Errorf("dialect not supported: %v", dialect)
	}
}

func mysql(table TableExpr) (string, error) {
	// Based on scrapper/mysql/query_metrics.sql
	// Returns: database, schema, table, row_count, updated_at
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for mysql")
	}

	query := `SELECT
    TABLE_ROWS as "num_rows",
    UPDATE_TIME as "last_loaded_at"
FROM information_schema.tables
WHERE TABLE_SCHEMA NOT IN ('sys', 'information_schema', 'mysql', 'performance_schema')
  AND (UPDATE_TIME IS NOT NULL OR TABLE_ROWS IS NOT NULL)
  AND TABLE_SCHEMA = '%s'
  AND TABLE_NAME = '%s'`

	return fmt.Sprintf(query, tableFqn.DatasetId(), tableFqn.TableId()), nil
}

func duckdb(table TableExpr) (string, error) {
	// Based on scrapper/duckdb/query_table_metrics.sql
	// Returns: table, database, schema, row_count
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for duckdb")
	}

	query := `SELECT
    estimated_size as "num_rows"
FROM duckdb_tables() t
WHERE NOT temporary
  AND NOT internal
  AND schema_name NOT IN ('information_schema')
  AND database_name NOT IN ('sample_data', 'temp', 'system', 'md_information_schema')
  AND database_name = '%s'
  AND schema_name = '%s'
  AND table_name = '%s'`

	return fmt.Sprintf(query, tableFqn.ProjectId(), tableFqn.DatasetId(), tableFqn.TableId()), nil
}

func clickhouse(table TableExpr) (string, error) {
	// Based on scrapper/clickhouse/query_table_metrics.sql
	// Returns: schema, table, row_count, updated_at
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for clickhouse")
	}

	query := `WITH parts AS (
    SELECT
        database AS schema,
        table,
        max(modification_time) AS updated_at
    FROM clusterAllReplicas(default, system.parts) prts
    GROUP BY database, table
)
SELECT
    toInt64(max(total_rows)) AS num_rows,
    max(parts.updated_at) as last_loaded_at
FROM clusterAllReplicas(default, system.tables) tbls
LEFT JOIN parts
    ON tbls.database = parts.schema
    AND tbls.name = parts.table
WHERE has_own_data = 1
  AND schema NOT IN ('system', 'information_schema')
  AND database = '%s'
  AND name = '%s'
SETTINGS join_use_nulls=1`

	return fmt.Sprintf(query, tableFqn.DatasetId(), tableFqn.TableId()), nil
}

func snowflake(table TableExpr) (string, error) {
	// Based on scrapper/snowflake/query_table_metrics.go
	// Returns: database, schema, table, row_count, size_bytes, updated_at
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for snowflake")
	}

	query := `SELECT
    row_count as "num_rows",
    bytes as "size_bytes",
    last_altered as "last_loaded_at"
FROM %s.information_schema.tables
WHERE row_count IS NOT NULL
  AND table_schema NOT IN ('INFORMATION_SCHEMA')
  AND table_schema = '%s'
  AND table_name = '%s'`

	return fmt.Sprintf(query, tableFqn.ProjectId(), tableFqn.DatasetId(), tableFqn.TableId()), nil
}

func redshift(table TableExpr) (string, error) {
	// Based on scrapper/redshift/query_table_metrics.sql
	// Returns: database, schema, table, row_count, updated_at
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for redshift")
	}

	query := `SELECT
    estimated_visible_rows as "num_rows",
    null::timestamp as "last_loaded_at"
FROM pg_catalog.svv_table_info
WHERE estimated_visible_rows IS NOT NULL
  AND schema <> 'catalog_history'::name
  AND schema <> 'pg_toast'::name
  AND schema <> 'pg_internal'::name
  AND "database" = '%s'
  AND "schema" = '%s'
  AND "table" = '%s'`

	return fmt.Sprintf(query, tableFqn.ProjectId(), tableFqn.DatasetId(), tableFqn.TableId()), nil
}

func bigquery(table TableExpr) (string, error) {
	// Based on scrapper/bigquery/query_table_metrics.go
	// Returns: database, schema, table, row_count, size_bytes, updated_at
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for bigquery")
	}

	query := `SELECT
    row_count AS num_rows,
    size_bytes AS size_bytes,
    TIMESTAMP_MILLIS(last_modified_time) AS last_loaded_at
FROM ` + "`%s`.`%s`.__TABLES__" + `
WHERE table_id = '%s'`

	return fmt.Sprintf(query, tableFqn.ProjectId(), tableFqn.DatasetId(), tableFqn.TableId()), nil
}

func trino(table TableExpr) (string, error) {
	// Based on scrapper/trino/query_table_metrics.go
	// Trino uses SHOW STATS which requires the full table name
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for trino")
	}

	// SHOW STATS returns statistics for a specific table
	query := fmt.Sprintf("SHOW STATS FOR %s.%s.%s", tableFqn.ProjectId(), tableFqn.DatasetId(), tableFqn.TableId())
	return query, nil
}

func postgres(table TableExpr) (string, error) {
	// Based on scrapper/postgres/query_table_metrics.sql
	// Returns: database, schema, table, row_count, updated_at
	tableFqn, ok := table.(*TableFqnExpr)
	if !ok {
		return "", fmt.Errorf("expected TableFqnExpr for postgres")
	}

	query := `SELECT
    COALESCE(
        (SELECT n_live_tup::bigint
         FROM pg_stat_user_tables
         WHERE relid = tbl.oid),
        CASE
            WHEN tbl.relpages = 0 THEN 0
            ELSE (tbl.reltuples *
                  (pg_catalog.pg_relation_size(tbl.oid) / pg_catalog.current_setting('block_size')::int))::bigint
        END
    ) as "num_rows"
FROM pg_catalog.pg_namespace sch
JOIN pg_catalog.pg_class tbl ON tbl.relnamespace = sch.oid
WHERE NOT pg_is_other_temp_schema(sch.oid)
  AND tbl.relpersistence IN ('p', 'u')
  AND tbl.relkind IN ('r', 'f', 'p', 'm')
  AND sch.nspname NOT IN ('pg_catalog', 'information_schema')
  AND tbl.reltuples >= 0
  AND sch.nspname = '%s'
  AND tbl.relname = '%s'`

	return fmt.Sprintf(query, tableFqn.DatasetId(), tableFqn.TableId()), nil
}
