package redshift

import (
	"os"
	"strconv"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// RedshiftSqlDialectExecutionSuite runs the sqldialect builder execution checks
// against Redshift.
//
// Redshift is the one engine here that folds a *delimited* identifier to lower
// case as well as an unquoted one, unless the cluster sets
// enable_case_sensitive_identifier — so quoting buys reserved words and
// punctuation but not case, and that is worth seeing on the engine rather than
// taking from the documentation.
type RedshiftSqlDialectExecutionSuite struct {
	scrappertest.SqlDialectExecutionSuite
}

func TestRedshiftSqlDialectExecutionSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift sqldialect execution tests in CI")
	}
	suite.Run(t, new(RedshiftSqlDialectExecutionSuite))
}

func (s *RedshiftSqlDialectExecutionSuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)

	database := testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev")
	sc, err := NewRedshiftScrapper(s.Ctx(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: database,
		},
	})
	if err != nil {
		s.T().Skipf("Could not connect to Redshift: %v", err)
	}
	s.Scrapper = sc
	s.Config = scrappertest.SqlDialectExecutionConfig{
		TableFqn: sqldialect.TableFqn(
			database,
			testenv.EnvOrDefault("REDSHIFT_SCHEMA", "schema_a"),
			testenv.EnvOrDefault("REDSHIFT_TABLE", "products"),
		),
		KeyField:     testenv.EnvOrDefault("REDSHIFT_KEY_FIELD", "id"),
		SegmentField: testenv.EnvOrDefault("REDSHIFT_SEGMENT_FIELD", "category"),
	}
}

func (s *RedshiftSqlDialectExecutionSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
