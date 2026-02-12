package sqldialect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcatWithSeparator(t *testing.T) {
	t.Run("postgres uses concat_ws", func(t *testing.T) {
		dialect := NewPostgresDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"), Sql("col3"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2, col3)", sql)
	})

	t.Run("mysql uses concat_ws", func(t *testing.T) {
		dialect := NewMySQLDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2)", sql)
	})

	t.Run("snowflake uses concat_ws", func(t *testing.T) {
		dialect := NewSnowflakeDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"), Sql("col3"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2, col3)", sql)
	})

	t.Run("duckdb uses concat_ws", func(t *testing.T) {
		dialect := NewDuckDBDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2)", sql)
	})

	t.Run("clickhouse uses concat_ws", func(t *testing.T) {
		dialect := NewClickHouseDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2)", sql)
	})

	t.Run("databricks uses concat_ws", func(t *testing.T) {
		dialect := NewDatabricksDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2)", sql)
	})

	t.Run("redshift uses concat_ws", func(t *testing.T) {
		dialect := NewRedshiftDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2)", sql)
	})

	t.Run("trino uses concat_ws", func(t *testing.T) {
		dialect := NewTrinoDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2)", sql)
	})

	t.Run("bigquery uses CONCAT without concat_ws", func(t *testing.T) {
		dialect := NewBigQueryDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"), Sql("col2"), Sql("col3"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		// BigQuery: CONCAT(col1, '|', col2, '|', col3)
		assert.Contains(t, sql, "CONCAT(")
		assert.Contains(t, sql, "col1")
		assert.Contains(t, sql, "'|'")
		assert.Contains(t, sql, "col2")
		assert.Contains(t, sql, "col3")
		assert.NotContains(t, sql, "concat_ws")
	})

	t.Run("bigquery with single column returns the column", func(t *testing.T) {
		dialect := NewBigQueryDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "col1", sql)
	})

	t.Run("bigquery with no columns returns empty string", func(t *testing.T) {
		dialect := NewBigQueryDialect()
		expr := dialect.ConcatWithSeparator("|")
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Equal(t, "''", sql)
	})
}

func TestConcatWsHelper(t *testing.T) {
	t.Run("ConcatWs delegates to dialect", func(t *testing.T) {
		expr := ConcatWs("|", Sql("col1"), Sql("col2"), Sql("col3"))

		// Test with Postgres
		pgSql, err := expr.ToSql(NewPostgresDialect())
		require.NoError(t, err)
		assert.Equal(t, "concat_ws('|', col1, col2, col3)", pgSql)

		// Test with BigQuery
		bqSql, err := expr.ToSql(NewBigQueryDialect())
		require.NoError(t, err)
		assert.Contains(t, bqSql, "CONCAT(")
		assert.Contains(t, bqSql, "'|'")
	})

	t.Run("ConcatWs with COALESCE for NULL handling", func(t *testing.T) {
		// Common pattern: concat_ws('|', COALESCE(col1, 'NULL'), COALESCE(col2, 'NULL'))
		expr := ConcatWs("|",
			Coalesce(Sql("col1"), String("<NULL>")),
			Coalesce(Sql("col2"), String("<NULL>")),
		)

		dialect := NewPostgresDialect()
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Contains(t, sql, "concat_ws")
		assert.Contains(t, sql, "COALESCE")
		assert.Contains(t, sql, "<NULL>")
	})

	t.Run("ConcatWs implements TextExpr", func(t *testing.T) {
		expr := ConcatWs("|", Sql("col1"), Sql("col2"))

		// Should be usable where TextExpr is expected
		asExpr := As(expr, Sql("concatenated"))
		sql, err := asExpr.ToSql(NewPostgresDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "as concatenated")
	})
}

func TestConcatWithSeparatorWithToString(t *testing.T) {
	t.Run("postgres with ToString", func(t *testing.T) {
		dialect := NewPostgresDialect()
		// Common pattern: concat_ws('|', CAST(id AS VARCHAR), CAST(amount AS VARCHAR))
		expr := dialect.ConcatWithSeparator("|",
			dialect.ToString(Sql("id")),
			dialect.ToString(Sql("amount")),
		)
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Contains(t, sql, "concat_ws")
		assert.Contains(t, sql, "CAST")
		assert.Contains(t, sql, "VARCHAR")
	})

	t.Run("bigquery with ToString", func(t *testing.T) {
		dialect := NewBigQueryDialect()
		expr := dialect.ConcatWithSeparator("|",
			dialect.ToString(Sql("id")),
			dialect.ToString(Sql("amount")),
		)
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Contains(t, sql, "CONCAT(")
		assert.Contains(t, sql, "SAFE_CAST")
		assert.Contains(t, sql, "STRING")
	})

	t.Run("clickhouse with ToString", func(t *testing.T) {
		dialect := NewClickHouseDialect()
		expr := dialect.ConcatWithSeparator("|",
			dialect.ToString(Sql("id")),
			dialect.ToString(Sql("name")),
		)
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Contains(t, sql, "concat_ws")
		assert.Contains(t, sql, "toString")
	})
}

func TestConcatWithSeparatorEdgeCases(t *testing.T) {
	t.Run("empty separator", func(t *testing.T) {
		dialect := NewPostgresDialect()
		expr := dialect.ConcatWithSeparator("", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Contains(t, sql, "concat_ws('', col1, col2)")
	})

	t.Run("multi-character separator", func(t *testing.T) {
		dialect := NewPostgresDialect()
		expr := dialect.ConcatWithSeparator(" | ", Sql("col1"), Sql("col2"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		assert.Contains(t, sql, "concat_ws(' | ', col1, col2)")
	})

	t.Run("single column", func(t *testing.T) {
		dialect := NewPostgresDialect()
		expr := dialect.ConcatWithSeparator("|", Sql("col1"))
		sql, err := expr.ToSql(dialect)
		require.NoError(t, err)
		// Most dialects will still use concat_ws with single column
		assert.Contains(t, sql, "concat_ws")
	})
}
