package duckdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/suite"
)

// DuckDBSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against in-memory DuckDB. Unlike the other warehouses this one needs no
// credentials, so it is the copy of these checks that runs in CI.
type DuckDBSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestDuckDBSqlDialectExecutionSuite(t *testing.T) {
	suite.Run(t, new(DuckDBSqlDialectExecutionSuite))
}

func (s *DuckDBSqlDialectExecutionSuite) SetupSuite() {
	ctx := context.Background()

	sc, err := NewLocalDuckDBScrapper(ctx, "", "test_instance")
	if err != nil {
		s.T().Skipf("Could not create DuckDB scrapper: %v", err)
	}
	s.Scrapper = sc

	db := sc.executor.GetDb()
	_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS schema_a`)
	s.Require().NoError(err)
	_, err = db.Exec(`CREATE TABLE schema_a.products (id INTEGER PRIMARY KEY, name VARCHAR, category VARCHAR, price DOUBLE)`)
	s.Require().NoError(err)

	categories := []string{"Electronics", "Home", "Garden"}
	for i := 1; i <= 24; i++ {
		_, err = db.Exec(fmt.Sprintf(
			`INSERT INTO schema_a.products VALUES (%d, 'product %d', '%s', %d.50)`,
			i, i, categories[i%len(categories)], i,
		))
		s.Require().NoError(err)
	}

	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn("", "schema_a", "products"),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *DuckDBSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
