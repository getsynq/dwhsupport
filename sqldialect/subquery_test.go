package sqldialect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create CondExpr from raw SQL for tests
func condSql(sql string) CondExpr {
	s := Sql(sql)
	return s
}

// Helper to create TableExpr from raw SQL for tests
func tableSql(sql string) TableExpr {
	s := Sql(sql)
	return s
}

func TestSubqueryTable(t *testing.T) {
	dialects := map[string]Dialect{
		"postgres":   NewPostgresDialect(),
		"mysql":      NewMySQLDialect(),
		"snowflake":  NewSnowflakeDialect(),
		"bigquery":   NewBigQueryDialect(),
		"duckdb":     NewDuckDBDialect(),
		"clickhouse": NewClickHouseDialect(),
		"databricks": NewDatabricksDialect(),
		"redshift":   NewRedshiftDialect(),
		"trino":      NewTrinoDialect(),
	}

	t.Run("simple subquery", func(t *testing.T) {
		subquery := "SELECT * FROM users WHERE active = true"
		subqTable := SubqueryTable(subquery, "active_users")

		for name, dialect := range dialects {
			t.Run(name, func(t *testing.T) {
				sql, err := subqTable.ToSql(dialect)
				require.NoError(t, err)
				assert.Contains(t, sql, "(")
				assert.Contains(t, sql, "SELECT * FROM users WHERE active = true")
				assert.Contains(t, sql, ") AS active_users")
			})
		}
	})

	t.Run("subquery with newlines", func(t *testing.T) {
		subquery := `SELECT id, name
FROM users
WHERE created_at > '2024-01-01'`
		subqTable := SubqueryTable(subquery, "recent_users")

		sql, err := subqTable.ToSql(NewPostgresDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "(\n")
		assert.Contains(t, sql, "\n) AS recent_users")
		assert.Contains(t, sql, "FROM users")
	})

	t.Run("implements TableExpr", func(t *testing.T) {
		subqTable := SubqueryTable("SELECT 1", "dummy")
		var _ TableExpr = subqTable
	})

	t.Run("can be used in SELECT FROM", func(t *testing.T) {
		subquery := "SELECT id, amount FROM orders"
		subqTable := SubqueryTable(subquery, "order_data")

		sel := NewSelect().
			From(subqTable).
			Cols(
				As(Fn("COUNT", Sql("*")), Sql("cnt")),
				As(Fn("SUM", Sql("amount")), Sql("total")),
			)

		sql, err := sel.ToSql(NewPostgresDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "select")
		assert.Contains(t, sql, "COUNT(*) as cnt")
		assert.Contains(t, sql, "SUM(amount) as total")
		assert.Contains(t, sql, "from (\n")
		assert.Contains(t, sql, "SELECT id, amount FROM orders")
		assert.Contains(t, sql, ") AS order_data")
	})

	t.Run("nested subqueries", func(t *testing.T) {
		// Create nested query as string (simulating what would be done in practice)
		middleStr := "SELECT id FROM (\nSELECT * FROM raw_data\n) AS filtered WHERE id > 100"
		outerSubq := SubqueryTable(middleStr, "final")

		sql, err := outerSubq.ToSql(NewDuckDBDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "AS final")
		assert.Contains(t, sql, "AS filtered")
	})

	t.Run("subquery with SQL comments", func(t *testing.T) {
		// SQL comments should be preserved in subquery
		subquery := `SELECT id, name FROM users -- active users only
WHERE active = true`
		subqTable := SubqueryTable(subquery, "users_active")

		sql, err := subqTable.ToSql(NewPostgresDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "-- active users only")
		assert.Contains(t, sql, "AS users_active")
	})
}

func TestSubqueryTableWithWhereAndGroupBy(t *testing.T) {
	t.Run("subquery with WHERE clause", func(t *testing.T) {
		subquery := "SELECT id, amount FROM transactions"
		subqTable := SubqueryTable(subquery, "txn")

		sel := NewSelect().
			From(subqTable).
			Cols(Sql("id"), Sql("amount")).
			Where(condSql("amount > 100"))

		sql, err := sel.ToSql(NewPostgresDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "from (\n")
		assert.Contains(t, sql, "SELECT id, amount FROM transactions")
		assert.Contains(t, sql, ") AS txn")
		assert.Contains(t, sql, "where amount > 100")
	})

	t.Run("subquery with GROUP BY", func(t *testing.T) {
		subquery := "SELECT id, name, value FROM data"
		subqTable := SubqueryTable(subquery, "_recon_base")

		caseExpr := Case().
			When(condSql("id < 100"), Int64(0)).
			Else(Int64(1))

		sel := NewSelect().
			From(subqTable).
			Cols(
				As(caseExpr, Sql("bucket")),
				As(Fn("COUNT", Sql("*")), Sql("cnt")),
			).
			GroupBy(Sql("bucket")).
			OrderBy(Asc(Sql("bucket")))

		sql, err := sel.ToSql(NewDuckDBDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "from (\n")
		assert.Contains(t, sql, "SELECT id, name, value FROM data")
		assert.Contains(t, sql, ") AS _recon_base")
		assert.Contains(t, sql, "CASE")
		assert.Contains(t, sql, "group by")
		assert.Contains(t, sql, "order by")
	})
}

func TestSumHelper(t *testing.T) {
	t.Run("Sum creates SUM function", func(t *testing.T) {
		sumExpr := Sum(Sql("amount"))

		dialects := []Dialect{
			NewPostgresDialect(),
			NewBigQueryDialect(),
			NewDuckDBDialect(),
		}

		for _, dialect := range dialects {
			sql, err := sumExpr.ToSql(dialect)
			require.NoError(t, err)
			assert.Equal(t, "SUM(amount)", sql)
		}
	})

	t.Run("Sum with CASE expression", func(t *testing.T) {
		caseExpr := Case().
			When(condSql("status = 'active'"), Sql("amount")).
			Else(Int64(0))

		sumExpr := Sum(caseExpr)

		sql, err := sumExpr.ToSql(NewPostgresDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "SUM(CASE")
		assert.Contains(t, sql, "WHEN status = 'active' THEN amount")
		assert.Contains(t, sql, "ELSE 0")
		assert.Contains(t, sql, "END)")
	})

	t.Run("Sum in SELECT with alias", func(t *testing.T) {
		sel := NewSelect().
			From(tableSql("orders")).
			Cols(
				As(Sum(Sql("amount")), Sql("total_amount")),
			)

		sql, err := sel.ToSql(NewSnowflakeDialect())
		require.NoError(t, err)
		assert.Contains(t, sql, "SUM(amount) as total_amount")
	})
}

func TestIntegrationSubqueryWithCaseAndConcat(t *testing.T) {
	t.Run("realistic reconciliation query pattern", func(t *testing.T) {
		// This mimics the pattern used in synq-recon:
		// SELECT bucket, COUNT(*), SUM(checksum) FROM (baseQuery) AS _recon_base WHERE ... GROUP BY bucket

		baseQuery := "SELECT id, name, value FROM test_data"
		subqTable := SubqueryTable(baseQuery, "_recon_base")

		// Bucket assignment using CASE
		bucketExpr := Case().
			When(condSql("id >= 1 AND id < 50"), Int64(0)).
			When(condSql("id >= 50 AND id < 100"), Int64(1)).
			Else(Int64(2))

		// Row checksum using concat_ws
		dialect := NewDuckDBDialect()
		checksumInner := dialect.ConcatWithSeparator("|",
			Coalesce(dialect.ToString(Sql("id")), String("<NULL>")),
			Coalesce(dialect.ToString(Sql("name")), String("<NULL>")),
			Coalesce(dialect.ToString(Sql("value")), String("<NULL>")),
		)
		checksumInnerSql, _ := checksumInner.ToSql(dialect)

		// Wrap in MD5 (simplified - real version has offset subtraction)
		checksumExpr := Sql("MD5(" + checksumInnerSql + ")")

		sel := NewSelect().
			From(subqTable).
			Cols(
				As(bucketExpr, Sql("bucket")),
				As(Fn("COUNT", Sql("*")), Sql("cnt")),
				As(Sum(checksumExpr), Sql("checksum")),
			).
			Where(condSql("id >= 1 AND id < 100")).
			GroupBy(Sql("bucket")).
			OrderBy(Asc(Sql("bucket")))

		sql, err := sel.ToSql(dialect)
		require.NoError(t, err)

		// Verify all components are present
		assert.Contains(t, sql, "CASE")
		assert.Contains(t, sql, "WHEN id >= 1 AND id < 50 THEN 0")
		assert.Contains(t, sql, "concat_ws")
		assert.Contains(t, sql, "COALESCE")
		assert.Contains(t, sql, "CAST")
		assert.Contains(t, sql, "VARCHAR")
		assert.Contains(t, sql, "SUM(MD5")
		assert.Contains(t, sql, "from (\n")
		assert.Contains(t, sql, "SELECT id, name, value FROM test_data")
		assert.Contains(t, sql, ") AS _recon_base")
		assert.Contains(t, sql, "where id >= 1 AND id < 100")
		assert.Contains(t, sql, "group by")
		assert.Contains(t, sql, "order by")
	})
}
