package mysql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

var productsTable = scrappertest.FixtureTable{Table: "schema_a_products"}

var productsConstraints = []scrappertest.Constraint{
	{Table: productsTable, Type: scrapper.ConstraintTypePrimaryKey, Columns: []string{"id"}, IsEnforced: lo.ToPtr(true), IndexType: "BTREE"},
	{Table: productsTable, Type: scrapper.ConstraintTypeUniqueIndex, Columns: []string{"sku"}, IsEnforced: lo.ToPtr(true), IndexType: "BTREE"},
	// a plain index constrains nothing, so it says nothing about enforcement
	{Table: productsTable, Type: scrapper.ConstraintTypeIndex, Columns: []string{"category"}, UnsetEnforced: true, IndexType: "BTREE"},
}

// MySQLTableConstraintsSuite checks the keys of the products fixture, and that
// no constraint of a system schema (performance_schema, mysql, sys) is read.
type MySQLTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestMySQLTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL constraint tests in CI")
	}
	suite.Run(t, new(MySQLTableConstraintsSuite))
}

func (s *MySQLTableConstraintsSuite) SetupSuite() {
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
	s.Expect = productsConstraints
}

func (s *MySQLTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MariaDBTableConstraintsSuite checks the same on MariaDB.
type MariaDBTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestMariaDBTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB constraint tests in CI")
	}
	suite.Run(t, new(MariaDBTableConstraintsSuite))
}

func (s *MariaDBTableConstraintsSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
	s.Expect = productsConstraints
}

func (s *MariaDBTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
