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

// RedshiftCatalogFactsSuite checks column nullability, owner, creation time
// and size on a products fixture, and the comment on its schema.
type RedshiftCatalogFactsSuite struct {
	scrappertest.CatalogFactsSuite
}

func TestRedshiftCatalogFactsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift catalog tests in CI")
	}
	suite.Run(t, new(RedshiftCatalogFactsSuite))
}

func (s *RedshiftCatalogFactsSuite) SetupSuite() {
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

	s.ColumnsTable = scrappertest.FixtureTable{Schema: "schema_a", Table: "products"}
	s.NotNullColumn, s.NullableColumn = "id", "description"
	s.Table = s.ColumnsTable
	s.ExpectOwner, s.ExpectCreatedAt, s.ExpectSizeBytes = true, true, true

	s.DescribedSchema = scrappertest.FixtureSchema{Schema: "schema_a"}
	s.SchemaDescription = "Primary test schema: products, orders, constraints"
}

func (s *RedshiftCatalogFactsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
