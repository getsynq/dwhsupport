package mysql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

var orderItemsForeignKey = scrappertest.ForeignKey{
	Table:             scrappertest.FixtureTable{Table: "schema_a_order_items"},
	Columns:           []string{"product_id"},
	Referenced:        scrappertest.FixtureTable{Table: "schema_a_products"},
	ReferencedColumns: []string{"id"},
}

// MySQLForeignKeysSuite checks the foreign key of the order_items fixture.
type MySQLForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestMySQLForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL constraint tests in CI")
	}
	suite.Run(t, new(MySQLForeignKeysSuite))
}

func (s *MySQLForeignKeysSuite) SetupSuite() {
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
	s.ForeignKeys = []scrappertest.ForeignKey{orderItemsForeignKey}
}

func (s *MySQLForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MariaDBForeignKeysSuite checks the same foreign key on MariaDB.
type MariaDBForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestMariaDBForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB constraint tests in CI")
	}
	suite.Run(t, new(MariaDBForeignKeysSuite))
}

func (s *MariaDBForeignKeysSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
	s.ForeignKeys = []scrappertest.ForeignKey{orderItemsForeignKey}
}

func (s *MariaDBForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
