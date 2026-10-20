package snowflake

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/suite"
)

var tagReferenceColumns = []string{
	"database", "schema", "table", "column", "domain", "tag_database", "tag_schema", "tag_name", "tag_value",
}

// TagReferencesSuite reads the catalog of a database MY_DB with a table
// CUSTOMERS, a view ACTIVE_CUSTOMERS and a lower-case table orders, with the
// tags TAG_REFERENCES lists for them.
type TagReferencesSuite struct {
	suite.Suite
}

func TestTagReferencesSuite(t *testing.T) {
	suite.Run(t, new(TagReferencesSuite))
}

func (s *TagReferencesSuite) catalogRoutes() []fakeRoute {
	return []fakeRoute{
		{
			match:   sqlContains("SHOW DATABASES"),
			columns: []string{"name", "origin", "owner", "comment", "kind"},
			rows:    [][]driver.Value{{"MY_DB", "", "SYSADMIN", "", "STANDARD"}},
		},
		{
			match:   sqlContains("SHOW STREAMS IN DATABASE MY_DB"),
			columns: []string{"created_on", "name", "database_name", "schema_name", "table_name"},
		},
		{
			match: sqlContains("MY_DB.information_schema.columns"),
			columns: []string{
				"instance", "database", "schema", "table", "is_view", "column", "type", "numeric_precision", "numeric_scale", "position",
			},
			rows: [][]driver.Value{
				{"ORG-ACCOUNT", "MY_DB", "CRM", "CUSTOMERS", false, "ID", "NUMBER", int64(38), int64(0), int64(1)},
				{"ORG-ACCOUNT", "MY_DB", "CRM", "CUSTOMERS", false, "EMAIL", "TEXT", nil, nil, int64(2)},
				{"ORG-ACCOUNT", "MY_DB", "CRM", "ACTIVE_CUSTOMERS", true, "EMAIL", "TEXT", nil, nil, int64(1)},
				{"ORG-ACCOUNT", "MY_DB", "CRM", "orders", false, "ID", "NUMBER", int64(38), int64(0), int64(1)},
			},
		},
	}
}

func (s *TagReferencesSuite) scrapper(conf *SnowflakeScrapperConf, tagReferences fakeRoute) (*SnowflakeScrapper, *fakeSnowflake) {
	ctx := context.Background()
	fake := &fakeSnowflake{t: s.T(), routes: append(s.catalogRoutes(), tagReferences)}
	if conf == nil {
		conf = &SnowflakeScrapperConf{}
	}
	conf.Account = "ORG-ACCOUNT"
	conf.Databases = []string{"MY_DB"}
	return newFakeSnowflakeScrapper(ctx, fake, conf), fake
}

func tagReferencesAnswering(rows ...[]driver.Value) fakeRoute {
	return fakeRoute{match: sqlContains("ACCOUNT_USAGE.TAG_REFERENCES"), columns: tagReferenceColumns, rows: rows}
}

func tagReferencesFailing(err error) fakeRoute {
	return fakeRoute{match: sqlContains("ACCOUNT_USAGE.TAG_REFERENCES"), err: err}
}

type columnTags struct {
	table, column []*scrapper.Tag
}

func tagsByColumn(rows []*scrapper.CatalogColumnRow) map[string]columnTags {
	return lo.SliceToMap(rows, func(r *scrapper.CatalogColumnRow) (string, columnTags) {
		return r.Table + "." + r.Column, columnTags{table: r.TableTags, column: r.ColumnTags}
	})
}

func (s *TagReferencesSuite) TestTagsGoOnTheirTableAndColumn() {
	scr, _ := s.scrapper(nil, tagReferencesAnswering(
		[]driver.Value{"MY_DB", "CRM", "CUSTOMERS", "", "TABLE", "MY_DB", "GOVERNANCE", "data_domain", "customers"},
		[]driver.Value{"MY_DB", "CRM", "CUSTOMERS", "EMAIL", "COLUMN", "MY_DB", "GOVERNANCE", "PII", "high"},
		// The same assignment twice is one tag.
		[]driver.Value{"MY_DB", "CRM", "CUSTOMERS", "EMAIL", "COLUMN", "MY_DB", "GOVERNANCE", "PII", "high"},
		[]driver.Value{"MY_DB", "CRM", "ACTIVE_CUSTOMERS", "", "TABLE", "MY_DB", "GOVERNANCE", "META", `{"owner":"team_a"}`},
		[]driver.Value{"MY_DB", "CRM", "ACTIVE_CUSTOMERS", "EMAIL", "COLUMN", "SNOWFLAKE", "CORE", "SEMANTIC_CATEGORY", "EMAIL"},
		// A tag without a value.
		[]driver.Value{"MY_DB", "CRM", "ACTIVE_CUSTOMERS", "", "TABLE", "MY_DB", "GOVERNANCE", "CERTIFIED", ""},
		// Names are exact: ORDERS is another object than orders.
		[]driver.Value{"MY_DB", "CRM", "ORDERS", "", "TABLE", "MY_DB", "GOVERNANCE", "TEAM", "sales"},
		// An object the catalog does not list.
		[]driver.Value{"MY_DB", "CRM", "GONE", "", "TABLE", "MY_DB", "GOVERNANCE", "TEAM", "sales"},
		// A column the catalog does not list.
		[]driver.Value{"MY_DB", "CRM", "CUSTOMERS", "DROPPED", "COLUMN", "MY_DB", "GOVERNANCE", "PII", "high"},
	))

	rows, err := scr.QueryCatalog(context.Background())
	s.Require().NoError(err)

	customers := []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.data_domain", TagValue: "customers"}}
	activeCustomers := []*scrapper.Tag{
		{TagName: "MY_DB.GOVERNANCE.META", TagValue: `{"owner":"team_a"}`},
		{TagName: "MY_DB.GOVERNANCE.CERTIFIED", TagValue: ""},
	}
	s.Equal(map[string]columnTags{
		"CUSTOMERS.ID":           {table: customers},
		"CUSTOMERS.EMAIL":        {table: customers, column: []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high"}}},
		"ACTIVE_CUSTOMERS.EMAIL": {table: activeCustomers, column: []*scrapper.Tag{{TagName: "SNOWFLAKE.CORE.SEMANTIC_CATEGORY", TagValue: "EMAIL"}}},
		"orders.ID":              {},
	}, tagsByColumn(rows))
}

func (s *TagReferencesSuite) TestEveryColumnHasItsOwnTableTags() {
	scr, _ := s.scrapper(nil, tagReferencesAnswering(
		[]driver.Value{"MY_DB", "CRM", "CUSTOMERS", "", "TABLE", "MY_DB", "GOVERNANCE", "TEAM", "crm"},
	))
	rows, err := scr.QueryCatalog(context.Background())
	s.Require().NoError(err)

	byColumn := lo.KeyBy(rows, func(r *scrapper.CatalogColumnRow) string { return r.Table + "." + r.Column })
	id, email := byColumn["CUSTOMERS.ID"].TableTags, byColumn["CUSTOMERS.EMAIL"].TableTags
	s.Require().Len(id, 1)
	s.Require().Len(email, 1)
	s.NotSame(&id[0], &email[0], "a wrapper filtering one row's tags in place must not change another's")
}

func (s *TagReferencesSuite) TestTheQueryReadsOnlyDirectLiveAssignmentsOfTheScannedDatabases() {
	scr, fake := s.scrapper(nil, tagReferencesAnswering())
	_, err := scr.QueryCatalog(context.Background())
	s.Require().NoError(err)

	statements := fake.statements(sqlContains("TAG_REFERENCES"))
	s.Require().Len(statements, 1, "one read per scan")
	s.Contains(statements[0], "FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES")
	s.Contains(statements[0], "object_deleted IS NULL")
	s.Contains(statements[0], "domain IN ('TABLE', 'COLUMN')")
	s.Contains(statements[0], "UPPER(object_database) IN ('MY_DB')")
}

func (s *TagReferencesSuite) TestTheAccountUsageDatabaseCanBeOverridden() {
	scr, fake := s.scrapper(
		&SnowflakeScrapperConf{AccountUsageDb: lo.ToPtr("USAGE_COPY")},
		tagReferencesAnswering(),
	)
	_, err := scr.QueryCatalog(context.Background())
	s.Require().NoError(err)
	s.Len(fake.statements(sqlContains("FROM USAGE_COPY.ACCOUNT_USAGE.TAG_REFERENCES")), 1)
}

func (s *TagReferencesSuite) TestTheScopeIsPushedDown() {
	scr, fake := s.scrapper(nil, tagReferencesAnswering())
	ctx := scope.WithScope(context.Background(), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Database: "MY_DB", Schema: "CRM"}},
	})
	_, err := scr.QueryCatalog(ctx)
	s.Require().NoError(err)

	statements := fake.statements(sqlContains("TAG_REFERENCES"))
	s.Require().Len(statements, 1)
	s.Contains(statements[0], "object_schema")
	s.NotContains(statements[0], "SYNQ_SCOPE_FILTER")
}

func (s *TagReferencesSuite) TestACatalogIsReadWithoutTagsWhenTagReferencesFails() {
	cases := map[string]error{
		"refused": &gosnowflake.SnowflakeError{
			Number: 3001, SQLState: "42501", Message: "SQL access control error: Insufficient privileges to operate on schema 'ACCOUNT_USAGE'",
		},
		"a missing account usage database": &gosnowflake.SnowflakeError{
			Number: 2003, SQLState: "02000", Message: "SQL compilation error:\nDatabase 'USAGE_COPY' does not exist or not authorized.",
		},
		"any other failure": errors.New("connection reset by peer"),
	}
	for name, failure := range cases {
		s.Run(name, func() {
			scr, _ := s.scrapper(nil, tagReferencesFailing(failure))
			rows, err := scr.QueryCatalog(context.Background())
			s.Require().NoError(err)
			s.Len(rows, 4)
			for _, row := range rows {
				s.Nil(row.TableTags)
				s.Nil(row.ColumnTags)
			}
		})
	}
}
