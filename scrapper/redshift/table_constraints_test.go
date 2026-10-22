package redshift

import (
	"os"
	"strconv"
	"testing"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// RedshiftTableConstraintsSuite checks the sort and distribution keys of the
// seed, a compound and an interleaved sort key among them, and that the
// indexes backing its keys, which carry the same sort and distribution
// columns in pg_attribute, are not reported as tables with keys.
type RedshiftTableConstraintsSuite struct {
	scrappertest.TableConstraintsSuite
}

func TestRedshiftTableConstraintsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift constraint tests in CI")
	}
	suite.Run(t, new(RedshiftTableConstraintsSuite))
}

func (s *RedshiftTableConstraintsSuite) SetupSuite() {
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
	products := scrappertest.FixtureTable{Schema: "schema_a", Table: "products"}
	orderItems := scrappertest.FixtureTable{Schema: "schema_a", Table: "order_items"}
	s.Expect = []scrappertest.Constraint{
		{Table: products, Type: scrapper.ConstraintTypePrimaryKey, Columns: []string{"id"}, IsEnforced: lo.ToPtr(false)},
		{Table: products, Type: scrapper.ConstraintTypeSortingKey, Columns: []string{"category", "id"}},
		{Table: products, Type: scrapper.ConstraintTypeDistributionKey, Columns: []string{"id"}},
		{Table: orderItems, Type: scrapper.ConstraintTypeSortingKey, Columns: []string{"order_id", "product_id"}, Expression: "INTERLEAVED"},
		{Table: orderItems, Type: scrapper.ConstraintTypeDistributionKey, Columns: []string{"order_id"}},
	}
}

func (s *RedshiftTableConstraintsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
