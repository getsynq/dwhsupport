package mysql

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// MariaDBValueRoundTripSuite checks that values read from MariaDB decode to
// what they stand for and can be written back as literals that compare equal.
type MariaDBValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestMariaDBValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB value round-trip tests in CI")
	}
	suite.Run(t, new(MariaDBValueRoundTripSuite))
}

func (s *MariaDBValueRoundTripSuite) SetupSuite() {
	if testenv.EnvOrDefault("MARIADB_HOST", "") == "" {
		s.T().Skip("MARIADB_HOST env var not set")
	}
	sc, err := newMariaDBScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MariaDB: %v", err)
	}
	s.Scrapper = sc
}

func (s *MariaDBValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// MySQLValueRoundTripSuite checks that values read from MySQL decode to
// what they stand for and can be written back as literals that compare equal.
type MySQLValueRoundTripSuite struct {
	scrappertest.ValueRoundTripSuite
}

func TestMySQLValueRoundTripSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL value round-trip tests in CI")
	}
	suite.Run(t, new(MySQLValueRoundTripSuite))
}

func (s *MySQLValueRoundTripSuite) SetupSuite() {
	if testenv.EnvOrDefault("MYSQL_HOST", "") == "" {
		s.T().Skip("MYSQL_HOST env var not set")
	}
	sc, err := newMySQLScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to MySQL: %v", err)
	}
	s.Scrapper = sc
}

func (s *MySQLValueRoundTripSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
