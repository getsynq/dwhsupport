package duckdb

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/getsynq/dwhsupport/scrapper"
	scrapperstdsql "github.com/getsynq/dwhsupport/scrapper/stdsql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

type DuckDBScrapperSuite struct {
	suite.Suite
}

func TestNewDuckDBScrapperSuite(t *testing.T) {
	suite.Run(t, new(DuckDBScrapperSuite))
}

func (s *DuckDBScrapperSuite) TestNewDuckDBScrapper() {
	if len(os.Getenv("CI")) > 0 {
		s.T().SkipNow()
	}

	ctx := context.TODO()

	scrapper, err := NewDuckDBScrapper(ctx, &DuckDBScapperConf{
		MotherduckAccount: "synq-pristine",
		MotherduckToken:   "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Imx1a2FzekBzeW5xLmlvIiwic2Vzc2lvbiI6Imx1a2Fzei5zeW5xLmlvIiwicGF0IjoiMWxEVWYzWDd5c2I0N0Fua1ItYjE4eVhuRmlQRHpxeHc5R0dCZzZFMTlGWSIsInVzZXJJZCI6IjI2YTU0NWE3LWU2YmEtNGNjYy04ODE2LTVjYTU5OTFhN2Q4ZCIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5Ijp0cnVlLCJ0b2tlblR5cGUiOiJyZWFkX3NjYWxpbmciLCJpYXQiOjE3Mzc2NDUyMjh9.e_C7UOusETMriyHtaifUyqoCVscq0yYQ09oa5bwzxNo",
	})
	s.Require().NoError(err)
	s.Require().NotNil(scrapper)

	databases, err := scrapper.QueryDatabases(ctx)
	s.Require().NoError(err)
	s.NotEmpty(databases)

	tables, err := scrapper.QueryTables(ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables)

	catalog, err := scrapper.QueryCatalog(ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog)

	sqlDefinitions, err := scrapper.QuerySqlDefinitions(ctx)
	s.Require().NoError(err)
	s.NotEmpty(sqlDefinitions)

	tableMetrics, err := scrapper.QueryTableMetrics(ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(tableMetrics)

}

func (s *DuckDBScrapperSuite) TestQueryCustomMetrics_HugeIntWithinInt64Range() {
	ctx := context.TODO()

	// Use in-memory DuckDB for testing hugeint support
	db, err := sqlx.Open("duckdb", "")
	s.Require().NoError(err)
	defer db.Close()

	// Test hugeint that fits within int64 range - should be converted to IntValue
	sql := `SELECT 12345::hugeint as small_huge_value`

	result, err := scrapperstdsql.QueryCustomMetrics(ctx, db, sql)
	s.Require().NoError(err, "QueryCustomMetrics should handle hugeint that fits in int64")
	s.Require().Len(result, 1)

	row := result[0]
	s.Require().Len(row.ColumnValues, 1)

	// Hugeint within int64 range should be converted to IntValue
	s.Equal("small_huge_value", row.ColumnValues[0].Name)
	s.False(row.ColumnValues[0].IsNull)
	s.IsType(scrapper.IntValue(0), row.ColumnValues[0].Value)
	s.Equal(scrapper.IntValue(12345), row.ColumnValues[0].Value)
}
