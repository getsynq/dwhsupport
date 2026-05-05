package athena

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/require"
)

// TestAthenaProbe asserts the scrapper actually returns the seeded fixtures —
// catches "all methods returned zero rows" failure modes that the generic
// compliance suite cannot detect.
func TestAthenaProbe(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skip in CI")
	}
	accessKeyID := os.Getenv("ATHENA_ACCESS_KEY_ID")
	secret := os.Getenv("ATHENA_SECRET_ACCESS_KEY")
	if accessKeyID == "" || secret == "" {
		t.Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY not set")
	}

	region := envOrDefault("ATHENA_REGION", "eu-central-1")
	wg := envOrDefault("ATHENA_WORKGROUP", "synq-dwhtesting-wg")

	sc, err := NewAthenaScrapper(context.Background(), &AthenaScrapperConf{
		AthenaConf: &dwhexecathena.AthenaConf{
			Region: region, Workgroup: wg,
			AccessKeyID: accessKeyID, SecretAccessKey: secret,
		},
	})
	require.NoError(t, err)
	defer sc.Close()

	ctx := context.Background()

	// QueryDatabases — must include the seeded Glue database.
	dbs, err := sc.QueryDatabases(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, dbs, "QueryDatabases returned no rows")
	var dbNames []string
	for _, d := range dbs {
		dbNames = append(dbNames, d.Database)
	}
	require.Contains(t, dbNames, "synq_dwhtesting")

	// QueryTables — must include every fixture name we seeded.
	tables, err := sc.QueryTables(ctx)
	require.NoError(t, err)
	tableNames := map[string]string{} // table -> table_type
	for _, r := range tables {
		if r.Schema == "synq_dwhtesting" {
			tableNames[r.Table] = r.TableType
		}
	}
	for _, want := range []string{
		"products", "order_items", "customers", "all_types", "daily_revenue",
		"elb_logs_parquet", "imports_csv", "logs_json", "metrics_projection",
		"active_products", "tools_only", "order_summary",
	} {
		require.Contains(t, tableNames, want, "QueryTables missing %q", want)
	}
	// Type sanity: the three seeded views surface as VIEW.
	for _, v := range []string{"active_products", "tools_only", "order_summary"} {
		require.Equal(t, "VIEW", tableNames[v], "%s not classified as VIEW", v)
	}

	// QueryCatalog — must return columns for the all_types fixture, including
	// every seeded column.
	cat, err := sc.QueryCatalog(ctx)
	require.NoError(t, err)
	allTypesCols := map[string]string{} // column -> data_type
	for _, c := range cat {
		if c.Schema == "synq_dwhtesting" && c.Table == "all_types" {
			allTypesCols[c.Column] = c.Type
		}
	}
	for _, want := range []string{
		"col_boolean", "col_int", "col_bigint", "col_decimal",
		"col_string", "col_binary", "col_date", "col_timestamp",
		"col_array_str", "col_array_int", "col_array_struct",
		"col_map", "col_map_struct", "col_struct",
	} {
		require.Contains(t, allTypesCols, want, "all_types missing column %q", want)
	}
	// Type-mapping spot checks (Glue lowercases types).
	require.Equal(t, "boolean", allTypesCols["col_boolean"])
	require.Contains(t, allTypesCols["col_decimal"], "decimal(38")
	require.Contains(t, allTypesCols["col_array_struct"], "array(row(")
	require.Contains(t, allTypesCols["col_struct"], "row(")

	// QuerySqlDefinitions — default mode: view bodies (information_schema.views).
	defs, err := sc.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	defByName := map[string]string{}
	for _, d := range defs {
		if d.Schema == "synq_dwhtesting" {
			defByName[d.Table] = d.Sql
		}
	}
	for _, v := range []string{"active_products", "tools_only", "order_summary"} {
		require.NotEmpty(t, defByName[v], "QuerySqlDefinitions[%q] empty", v)
	}
	// In default mode tables have no DDL — they should be filtered out.
	require.NotContains(t, defByName, "products")
	require.NotContains(t, defByName, "elb_logs_parquet")

	// SHOW CREATE flags — opt-in DDL fetch. Build a fresh scrapper with both flags on.
	scDDL, err := NewAthenaScrapper(ctx, &AthenaScrapperConf{
		AthenaConf: &dwhexecathena.AthenaConf{
			Region: region, Workgroup: wg,
			AccessKeyID: accessKeyID, SecretAccessKey: secret,
		},
		UseShowCreateView:  true,
		UseShowCreateTable: true,
	})
	require.NoError(t, err)
	defer scDDL.Close()

	ddls, err := scDDL.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	ddlByName := map[string]string{}
	for _, d := range ddls {
		if d.Schema == "synq_dwhtesting" {
			ddlByName[d.Table] = d.Sql
		}
	}
	// Tables now have DDL — assert SHOW CREATE TABLE actually returned a CREATE statement.
	require.Contains(t, ddlByName, "products", "products DDL missing under UseShowCreateTable")
	require.Contains(t, strings.ToUpper(ddlByName["products"]), "CREATE TABLE")
	require.Contains(t, ddlByName, "elb_logs_parquet")
	require.Contains(t, strings.ToUpper(ddlByName["elb_logs_parquet"]), "CREATE EXTERNAL TABLE")
	// Iceberg properties leaked through SHOW CREATE TABLE (proves we got more than information_schema gives).
	require.Contains(t, ddlByName["products"], "table_type",
		"products DDL should mention table_type (Iceberg TBLPROPERTIES)")
	// Views now have full CREATE OR REPLACE VIEW DDL.
	require.Contains(t, strings.ToUpper(ddlByName["active_products"]), "CREATE")
	require.Contains(t, strings.ToUpper(ddlByName["active_products"]), "VIEW")

	// Scope filter — restrict to a single table and verify both QueryTables
	// and QueryCatalog narrow accordingly.
	scopedCtx := scope.WithScope(ctx, &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Schema: "synq_dwhtesting", Table: "products"}},
	})
	scopedTables, err := sc.QueryTables(scopedCtx)
	require.NoError(t, err)
	for _, r := range scopedTables {
		require.Equal(t, "products", r.Table, "scope filter leaked: got %s.%s", r.Schema, r.Table)
	}
	require.NotEmpty(t, scopedTables, "scope filter dropped everything")

	scopedCols, err := sc.QueryCatalog(scopedCtx)
	require.NoError(t, err)
	for _, c := range scopedCols {
		require.Equal(t, "products", c.Table, "scope filter leaked into catalog: %s.%s", c.Schema, c.Table)
	}
	require.NotEmpty(t, scopedCols)

	// FetchQueryLogs — pages through ListQueryExecutions+BatchGetQueryExecution
	// for the workgroup. The seed itself ran ~30 queries via reseed.sh, so the
	// 24h window must contain at least the SELECT we just sent.
	from := time.Now().Add(-24 * time.Hour)
	to := time.Now().Add(time.Minute)
	obf, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	iter, err := sc.FetchQueryLogs(ctx, from, to, obf)
	require.NoError(t, err)
	defer iter.Close()

	var (
		count          int
		sawShowCreate  bool
		sawSelect      bool
		anyHasContext  bool
		anyHasFinished bool
	)
	for {
		ql, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
		require.NotEmpty(t, ql.QueryID)
		require.NotEmpty(t, ql.Status)
		require.False(t, ql.CreatedAt.IsZero())
		require.NotNil(t, ql.StartedAt)
		require.False(t, ql.CreatedAt.Before(from))
		require.False(t, ql.CreatedAt.After(to))

		upper := strings.ToUpper(ql.SQL)
		if strings.HasPrefix(upper, "SHOW CREATE") {
			sawShowCreate = true
		}
		if strings.HasPrefix(upper, "SELECT") {
			sawSelect = true
		}
		if ql.DwhContext != nil && ql.DwhContext.Schema != "" {
			anyHasContext = true
		}
		if ql.FinishedAt != nil {
			anyHasFinished = true
		}
		if count > 200 {
			break // sanity cap; we only need a representative sample
		}
	}
	require.NotZero(t, count, "FetchQueryLogs returned no rows in 24h window")
	require.True(t, sawSelect, "expected at least one SELECT (we ran several earlier in this test)")
	require.True(t, sawShowCreate, "expected at least one SHOW CREATE from the SHOW CREATE flag run")
	require.True(t, anyHasContext, "expected at least one row with QueryExecutionContext.Database populated")
	require.True(t, anyHasFinished, "expected at least one row with FinishedAt set")
}

func envOrDefault(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
