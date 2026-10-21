package trino

import (
	"os"
	"testing"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// TrinoMySQLCommentsSuite reads the comments of the MySQL seed through the
// self-hosted Trino's mysql catalog. Trino's MySQL connector lists a view as a
// BASE TABLE and passes on the comment "VIEW" MySQL reports for it.
type TrinoMySQLCommentsSuite struct {
	scrappertest.CommentsSuite
}

func TestTrinoMySQLCommentsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Trino comment tests in CI")
	}
	suite.Run(t, new(TrinoMySQLCommentsSuite))
}

func (s *TrinoMySQLCommentsSuite) SetupSuite() {
	if testenv.EnvOrDefault("TRINO_HOST", "") == "" {
		s.T().Skip("TRINO_HOST env var not set")
	}
	catalog := testenv.EnvOrDefault("TRINO_MYSQL_CATALOG", "mysql")
	sc, err := NewTrinoScrapper(s.Ctx(), &TrinoScrapperConf{
		TrinoConf: &dwhexectrino.TrinoConf{
			Host:      testenv.EnvOrDefault("TRINO_HOST", ""),
			Port:      testenv.EnvOrDefaultInt("TRINO_PORT", 8080),
			User:      testenv.EnvOrDefault("TRINO_USER", "trino"),
			Password:  os.Getenv("TRINO_PASSWORD"),
			Plaintext: true,
		},
		Catalogs:           []string{catalog},
		FetchTableComments: true,
	})
	if err != nil {
		s.T().Skipf("Could not connect to Trino: %v", err)
	}
	s.Scrapper = sc

	s.Table = scrappertest.FixtureTable{Database: catalog, Table: "schema_a_products"}
	s.TableComment = "Product catalog with inventory tracking"
	s.ColumnComments = map[string]string{
		"id":   "Unique product identifier",
		"name": "Product display name",
	}
	s.UncommentedColumn = "price"
	s.View = scrappertest.FixtureTable{Database: catalog, Table: "schema_a_active_products"}
	s.CatalogTableComments = true
}

func (s *TrinoMySQLCommentsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
