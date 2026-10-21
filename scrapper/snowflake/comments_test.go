package snowflake

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// SnowflakeCommentsSuite reads the comments of the dwhtesting seed's
// SCHEMA_A.PRODUCTS, whose table and three columns are commented. Point
// SNOWFLAKE_DATABASE at the seeded database to run it. Every view of the seed
// has a comment, and Snowflake has no comment of its own for a view, so View
// is left empty.
type SnowflakeCommentsSuite struct {
	scrappertest.CommentsSuite
}

func TestSnowflakeCommentsSuite(t *testing.T) {
	skipWithoutSnowflake(t)
	suite.Run(t, new(SnowflakeCommentsSuite))
}

func (s *SnowflakeCommentsSuite) SetupSuite() {
	conf := snowflakeTestConf()
	sc, err := NewSnowflakeScrapper(s.Ctx(), &SnowflakeScrapperConf{SnowflakeConf: conf})
	if err != nil {
		s.T().Skipf("Could not connect to Snowflake: %v", err)
	}
	s.Scrapper = sc

	database := conf.Databases[0]
	s.Table = scrappertest.FixtureTable{Database: database, Schema: "SCHEMA_A", Table: "PRODUCTS"}
	s.TableComment = "Product catalog with inventory tracking"
	s.ColumnComments = map[string]string{
		"ID":       "Unique product identifier",
		"NAME":     "Product display name",
		"METADATA": "Free-form product metadata (VARIANT)",
	}
	s.UncommentedColumn = "PRICE"
}

func (s *SnowflakeCommentsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
