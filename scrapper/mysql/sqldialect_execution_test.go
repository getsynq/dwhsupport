package mysql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// MariaDBSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against MariaDB.
type MariaDBSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestMariaDBSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB sqldialect execution tests in CI")
	}
	suite.Run(t, new(MariaDBSqlDialectExecutionSuite))
}

func (s *MariaDBSqlDialectExecutionSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn("", testenv.EnvOrDefault("MARIADB_DATABASE", "synq_test"), "schema_a_products"),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *MariaDBSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MySQLSqlDialectExecutionSuite runs the same checks against real MySQL. The
// two engines are one dialect here but not one parser — MySQL gained window
// functions in 8.0 and MariaDB in 10.2, on separate implementations.
type MySQLSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestMySQLSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL sqldialect execution tests in CI")
	}
	suite.Run(t, new(MySQLSqlDialectExecutionSuite))
}

func (s *MySQLSqlDialectExecutionSuite) SetupSuite() {
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn:     sqldialect.TableFqn("", testenv.EnvOrDefault("MYSQL_DATABASE", "synq_test"), "schema_a_products"),
		KeyField:     "id",
		SegmentField: "category",
	}
}

func (s *MySQLSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
