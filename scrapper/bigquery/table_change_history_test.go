package bigquery

import (
	"testing"
	"time"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
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
	fqn := scrapper.DwhFqn{DatabaseName: "my_project", SchemaName: "my_dataset", ObjectName: "my_table"}

	testCases := []struct {
		name  string
		conf  *BigQueryScrapperConf
		fqn   scrapper.DwhFqn
		limit int
	}{
		{
			name: "default_region_us",
			conf: &BigQueryScrapperConf{
				BigQueryConf: dwhexecbigquery.BigQueryConf{ProjectId: "my-project"},
			},
			fqn:   fqn,
			limit: 100,
		},
		{
			name: "explicit_eu_region",
			conf: &BigQueryScrapperConf{
				BigQueryConf: dwhexecbigquery.BigQueryConf{ProjectId: "my-project", Region: "EU"},
			},
			fqn:   fqn,
			limit: 50,
		},
		{
			name: "table_name_with_single_quote",
			conf: &BigQueryScrapperConf{
				BigQueryConf: dwhexecbigquery.BigQueryConf{ProjectId: "my-project", Region: "us"},
			},
			fqn:   scrapper.DwhFqn{DatabaseName: "project'name", SchemaName: "data'set", ObjectName: "my'table"},
			limit: 10,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			scrpr := &BigQueryScrapper{conf: tc.conf}
			sql := scrpr.buildTableChangeHistorySQL(tc.fqn, from, to, tc.limit)
			s.NotEmpty(sql)
			snaps.MatchSnapshot(s.T(), sql)
		})
	}
}

func (s *TableChangeHistorySuite) TestBuildTableChangeHistorySQLSafeEscaping() {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)
	scrpr := &BigQueryScrapper{conf: &BigQueryScrapperConf{
		BigQueryConf: dwhexecbigquery.BigQueryConf{ProjectId: "my-project", Region: "us"},
	}}

	// Table names with single quotes should be properly escaped (doubled)
	fqn := scrapper.DwhFqn{DatabaseName: "project'inject", SchemaName: "dataset", ObjectName: "table"}
	sql := scrpr.buildTableChangeHistorySQL(fqn, from, to, 100)
	s.Contains(sql, "project''inject", "single quote should be doubled for safe escaping")
	s.NotContains(sql, "'project'inject'", "unescaped single quote should not appear")
}
