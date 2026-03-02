package duckdb

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// DuckDBScopeComplianceSuite runs scope filtering compliance checks against in-memory DuckDB.
type DuckDBScopeComplianceSuite struct {
	scrappertest.ScopeComplianceSuite
}

func TestDuckDBScopeComplianceSuite(t *testing.T) {
	suite.Run(t, new(DuckDBScopeComplianceSuite))
}

func (s *DuckDBScopeComplianceSuite) SetupSuite() {
	ctx := context.Background()

	sc, err := NewLocalDuckDBScrapper(ctx, "", "test_instance")
	if err != nil {
		s.T().Skipf("Could not create DuckDB scrapper: %v", err)
	}
	s.Scrapper = sc

	// Create test fixtures in two schemas so scope tests can distinguish them.
	db := sc.executor.GetDb()

	_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS schema_a`)
	s.Require().NoError(err)
	_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS schema_b`)
	s.Require().NoError(err)

	_, err = db.Exec(`CREATE TABLE schema_a.t1 (id INTEGER PRIMARY KEY, name VARCHAR)`)
	s.Require().NoError(err)
	_, err = db.Exec(`CREATE VIEW schema_a.v1 AS SELECT id, name FROM schema_a.t1`)
	s.Require().NoError(err)

	_, err = db.Exec(`CREATE TABLE schema_b.t2 (id INTEGER PRIMARY KEY, value DOUBLE)`)
	s.Require().NoError(err)
}

func (s *DuckDBScopeComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
