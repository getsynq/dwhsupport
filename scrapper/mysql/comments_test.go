package mysql

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// commentsFixture is the same on the MySQL and the MariaDB seed:
// schema_a_products has a table comment and two commented columns, and
// schema_a_active_products is a view, whose TABLE_COMMENT both report as
// "VIEW".
func commentsFixture(s *scrappertest.CommentsSuite) {
	s.Table = scrappertest.FixtureTable{Table: "schema_a_products"}
	s.TableComment = "Product catalog with inventory tracking"
	s.ColumnComments = map[string]string{
		"id":   "Unique product identifier",
		"name": "Product display name",
	}
	s.UncommentedColumn = "price"
	s.View = scrappertest.FixtureTable{Table: "schema_a_active_products"}
	s.CatalogTableComments = true
}

type MySQLCommentsSuite struct {
	scrappertest.CommentsSuite
}

func TestMySQLCommentsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MySQL comment tests in CI")
	}
	suite.Run(t, new(MySQLCommentsSuite))
}

func (s *MySQLCommentsSuite) SetupSuite() {
	s.Scrapper = connectForComments(s.T(), "MYSQL_HOST", newMySQLScrapperFromEnv)
	commentsFixture(&s.CommentsSuite)
}

func (s *MySQLCommentsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

type MariaDBCommentsSuite struct {
	scrappertest.CommentsSuite
}

func TestMariaDBCommentsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MariaDB comment tests in CI")
	}
	suite.Run(t, new(MariaDBCommentsSuite))
}

func (s *MariaDBCommentsSuite) SetupSuite() {
	s.Scrapper = connectForComments(s.T(), "MARIADB_HOST", newMariaDBScrapperFromEnv)
	commentsFixture(&s.CommentsSuite)
}

func (s *MariaDBCommentsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

func connectForComments(t *testing.T, hostVar string, connect func(context.Context) (*MySQLScrapper, error)) *MySQLScrapper {
	t.Helper()
	if testenv.EnvOrDefault(hostVar, "") == "" {
		t.Skipf("%s env var not set", hostVar)
	}
	sc, err := connect(context.Background())
	if err != nil {
		t.Skipf("Could not connect: %v", err)
	}
	return sc
}
