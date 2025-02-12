package duckdb

import (
	"context"
	"os"
	"testing"
	"time"

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
