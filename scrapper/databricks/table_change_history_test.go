package databricks

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type TableChangeHistorySuite struct {
	suite.Suite
}

func TestTableChangeHistorySuite(t *testing.T) {
	suite.Run(t, new(TableChangeHistorySuite))
}

func (s *TableChangeHistorySuite) TestBuildTableChangeHistorySQL() {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)

	testCases := []struct {
		name  string
		fqn   scrapper.DwhFqn
		limit int
	}{
		{
			name:  "simple_table",
			fqn:   scrapper.DwhFqn{DatabaseName: "my_catalog", SchemaName: "public", ObjectName: "orders"},
			limit: 100,
		},
		{
			name:  "table_with_special_chars",
			fqn:   scrapper.DwhFqn{DatabaseName: "catalog-with-dashes", SchemaName: "schema_name", ObjectName: "table name"},
			limit: 50,
		},
		{
			name:  "table_with_backtick",
			fqn:   scrapper.DwhFqn{DatabaseName: "catalog`inject", SchemaName: "schema", ObjectName: "table"},
			limit: 10,
		},
	}

	scrpr := &DatabricksScrapper{}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			sql := scrpr.buildTableChangeHistorySQL(tc.fqn, from, to, tc.limit)
			s.NotEmpty(sql)
			snaps.MatchSnapshot(s.T(), sql)
		})
	}
}

func (s *TableChangeHistorySuite) TestBuildTableChangeHistorySQLIdentifierQuoting() {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)
	scrpr := &DatabricksScrapper{}

	fqn := scrapper.DwhFqn{DatabaseName: "my_catalog", SchemaName: "my_schema", ObjectName: "my_table"}
	sql := scrpr.buildTableChangeHistorySQL(fqn, from, to, 100)

	// Table identifiers should be backtick-quoted
	s.Contains(sql, "`my_catalog`")
	s.Contains(sql, "`my_schema`")
	s.Contains(sql, "`my_table`")
}
