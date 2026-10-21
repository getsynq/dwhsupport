package snowflake

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

// TagsIntegrationSuite reads the tags of the dwhtesting seed's fixtures: the
// table SCHEMA_A.PRODUCTS and the view SCHEMA_A.ACTIVE_PRODUCTS each carry a
// table tag and a tag on their NAME column, the view a tag holding JSON too,
// and SCHEMA_B.CUSTOMERS a tag with a quoted lower-case name and tags on two
// columns. Point SNOWFLAKE_* at the seeded database and its role to run it.
// TAG_REFERENCES lags behind by up to two hours, so a reseed shows here only
// after that.
type TagsIntegrationSuite struct {
	suite.Suite
	database string
}

func TestTagsIntegrationSuite(t *testing.T) {
	skipWithoutSnowflake(t)
	suite.Run(t, new(TagsIntegrationSuite))
}

func (s *TagsIntegrationSuite) newScrapper(accountUsageDb *string) *SnowflakeScrapper {
	sc, err := NewSnowflakeScrapper(context.Background(), &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf(), AccountUsageDb: accountUsageDb})
	if err != nil {
		s.T().Skipf("Could not connect to Snowflake: %v", err)
	}
	s.T().Cleanup(func() { _ = sc.Close() })
	return sc
}

func (s *TagsIntegrationSuite) SetupSuite() {
	s.database = snowflakeTestConf().Databases[0]
}

// fixtureScope narrows a read to the three fixtures.
func (s *TagsIntegrationSuite) fixtureScope() context.Context {
	return scope.WithScope(context.Background(), &scope.ScopeFilter{Include: []scope.ScopeRule{
		{Database: s.database, Schema: "SCHEMA_A", Table: "PRODUCTS"},
		{Database: s.database, Schema: "SCHEMA_A", Table: "ACTIVE_PRODUCTS"},
		{Database: s.database, Schema: "SCHEMA_B", Table: "CUSTOMERS"},
	}})
}

func (s *TagsIntegrationSuite) catalog(sc *SnowflakeScrapper) map[string]*scrapper.CatalogColumnRow {
	rows, err := sc.QueryCatalog(s.fixtureScope())
	s.Require().NoError(err)
	byColumn := lo.KeyBy(rows, func(r *scrapper.CatalogColumnRow) string { return r.Schema + "." + r.Table + "." + r.Column })
	if _, ok := byColumn["SCHEMA_A.PRODUCTS.NAME"]; !ok {
		s.T().Skipf("the dwhtesting tag fixtures are not in database %s", s.database)
	}
	return byColumn
}

func (s *TagsIntegrationSuite) tag(name, value string) *scrapper.Tag {
	return &scrapper.Tag{TagName: s.database + ".GOVERNANCE." + name, TagValue: value, Kind: scrapper.TagKindGovernedTag}
}

func (s *TagsIntegrationSuite) TestQueryCatalogReadsTableAndColumnTagsWithValues() {
	byColumn := s.catalog(s.newScrapper(nil))

	costCenter := s.tag("COST_CENTER", "SALES")
	s.Equal([]*scrapper.Tag{costCenter}, byColumn["SCHEMA_A.PRODUCTS.ID"].TableTags)
	s.Nil(byColumn["SCHEMA_A.PRODUCTS.ID"].ColumnTags)
	s.Equal([]*scrapper.Tag{s.tag("PII", "low")}, byColumn["SCHEMA_A.PRODUCTS.NAME"].ColumnTags)

	s.ElementsMatch(
		[]*scrapper.Tag{costCenter, s.tag("META", `{"owner":"team_a","tier":"gold"}`)},
		byColumn["SCHEMA_A.ACTIVE_PRODUCTS.NAME"].TableTags,
	)
	s.Equal([]*scrapper.Tag{s.tag("PII", "low")}, byColumn["SCHEMA_A.ACTIVE_PRODUCTS.NAME"].ColumnTags)

	s.Equal([]*scrapper.Tag{s.tag("data_domain", "customers")}, byColumn["SCHEMA_B.CUSTOMERS.ID"].TableTags)
	s.Equal([]*scrapper.Tag{s.tag("PII", "high")}, byColumn["SCHEMA_B.CUSTOMERS.EMAIL"].ColumnTags)
	s.Equal([]*scrapper.Tag{s.tag("PII", "high")}, byColumn["SCHEMA_B.CUSTOMERS.SSN"].ColumnTags)
	s.Nil(byColumn["SCHEMA_B.CUSTOMERS.FULL_NAME"].ColumnTags)
}

// Without TAG_REFERENCES the catalog is read all the same, and the tags come
// from GET_DDL in QuerySqlDefinitions. The role holds APPLY TAG, so GET_DDL
// shows the values.
func (s *TagsIntegrationSuite) TestWithoutTagReferencesTheTagsComeFromTheDdl() {
	sc := s.newScrapper(lo.ToPtr("ACCOUNT_USAGE_DB_THAT_DOES_NOT_EXIST"))
	byColumn := s.catalog(sc)
	for key, row := range byColumn {
		s.Nil(row.TableTags, key)
		s.Nil(row.ColumnTags, key)
	}

	definitions, err := sc.QuerySqlDefinitions(s.fixtureScope())
	s.Require().NoError(err)
	byTable := lo.KeyBy(definitions, func(r *scrapper.SqlDefinitionRow) string { return r.Schema + "." + r.Table })

	view := byTable["SCHEMA_A.ACTIVE_PRODUCTS"]
	s.Require().NotNil(view)
	s.NotEmpty(view.Sql)
	s.ElementsMatch([]*scrapper.Tag{s.tag("COST_CENTER", "SALES"), s.tag("META", `{"owner":"team_a","tier":"gold"}`)}, view.Tags)
	s.Equal(map[string][]*scrapper.Tag{"NAME": {s.tag("PII", "low")}}, view.ColumnTags)

	customers := byTable["SCHEMA_B.CUSTOMERS"]
	s.Require().NotNil(customers)
	s.Equal([]*scrapper.Tag{s.tag("data_domain", "customers")}, customers.Tags)
	s.Equal(map[string][]*scrapper.Tag{
		"EMAIL": {s.tag("PII", "high")},
		"SSN":   {s.tag("PII", "high")},
	}, customers.ColumnTags)
}
