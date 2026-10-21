package redshift

import (
	"os"
	"strconv"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// RedshiftForeignKeysSuite checks the informational foreign key of the
// order_items fixture.
type RedshiftForeignKeysSuite struct {
	scrappertest.ForeignKeysSuite
}

func TestRedshiftForeignKeysSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift constraint tests in CI")
	}
	suite.Run(t, new(RedshiftForeignKeysSuite))
}

func (s *RedshiftForeignKeysSuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)
	sc, err := NewRedshiftScrapper(s.Ctx(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev"),
		},
	})
	if err != nil {
		s.T().Skipf("Could not connect to Redshift: %v", err)
	}
	s.Scrapper = sc
	s.ForeignKeys = []scrappertest.ForeignKey{{
		Table:             scrappertest.FixtureTable{Schema: "schema_a", Table: "order_items"},
		Columns:           []string{"product_id"},
		Referenced:        scrappertest.FixtureTable{Schema: "schema_a", Table: "products"},
		ReferencedColumns: []string{"id"},
	}}
}

func (s *RedshiftForeignKeysSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
