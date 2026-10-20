package databricks

import (
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// DatabricksCatalogFactsSuite checks owner and creation time on the samples
// catalog every Databricks workspace has. No fixture with a known NOT NULL
// column exists there, so nullability is covered by TestColumnNullable on
// decoded API responses instead.
type DatabricksCatalogFactsSuite struct {
	scrappertest.CatalogFactsSuite
}

func TestDatabricksCatalogFactsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Databricks catalog tests in CI")
	}
	suite.Run(t, new(DatabricksCatalogFactsSuite))
}

func (s *DatabricksCatalogFactsSuite) SetupSuite() {
	s.Scrapper = newIntegrationScrapper(s.T(), s.Ctx())

	s.Table = scrappertest.FixtureTable{Database: "samples", Schema: "tpch", Table: "orders"}
	s.ExpectOwner, s.ExpectCreatedAt = true, true
}
