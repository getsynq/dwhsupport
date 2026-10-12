package duckdb

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// DuckDBCatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports against in-memory DuckDB, so it runs in CI.
type DuckDBCatalogNumericSuite struct {
	scrappertest.CatalogNumericSuite
}

func TestDuckDBCatalogNumericSuite(t *testing.T) {
	suite.Run(t, new(DuckDBCatalogNumericSuite))
}

func (s *DuckDBCatalogNumericSuite) SetupSuite() {
	sc, err := NewLocalDuckDBScrapper(context.Background(), "", "test_instance")
	if err != nil {
		s.T().Skipf("Could not create DuckDB scrapper: %v", err)
	}
	s.Scrapper = sc

	db := sc.executor.GetDb()
	_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS numerics`)
	s.Require().NoError(err)
	_, err = db.Exec(`CREATE TABLE numerics.amounts (
		id BIGINT, amount DECIMAL(20,6), whole DECIMAL(10,0), bare NUMERIC, ratio DOUBLE, single FLOAT, label VARCHAR
	)`)
	s.Require().NoError(err)

	amountP, amountS := scrappertest.Sized(20, 6)
	wholeP, wholeS := scrappertest.Sized(10, 0)
	// DuckDB gives a NUMERIC declared without a size its default, DECIMAL(18,3),
	// and the catalog names it so.
	bareP, bareS := scrappertest.Sized(18, 3)
	s.Database, s.Schema, s.Table = "", "numerics", "amounts"
	s.Want = []scrappertest.CatalogNumericColumn{
		{Column: "id", Category: scrapper.NumericCategoryInteger},
		{Column: "amount", Category: scrapper.NumericCategoryDecimal, Precision: amountP, Scale: amountS},
		{Column: "whole", Category: scrapper.NumericCategoryInteger, Precision: wholeP, Scale: wholeS},
		{Column: "bare", Category: scrapper.NumericCategoryDecimal, Precision: bareP, Scale: bareS},
		{Column: "ratio", Category: scrapper.NumericCategoryFloat},
		{Column: "single", Category: scrapper.NumericCategorySinglePrecisionFloat},
		{Column: "label", Category: scrapper.NumericCategoryNone},
	}
}

func (s *DuckDBCatalogNumericSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
