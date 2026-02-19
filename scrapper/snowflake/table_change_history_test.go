package snowflake

import (
	"testing"
	"time"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
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

func (s *TableChangeHistorySuite) TestBuildDMLHistorySQL() {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)
	fqn := scrapper.DwhFqn{DatabaseName: "MY_DB", SchemaName: "PUBLIC", ObjectName: "ORDERS"}

	testCases := []struct {
		name  string
		conf  *SnowflakeScrapperConf
		fqn   scrapper.DwhFqn
		limit int
	}{
		{
			name:  "default_account_usage_db",
			conf:  &SnowflakeScrapperConf{},
			fqn:   fqn,
			limit: 100,
		},
		{
			name: "custom_account_usage_db",
			conf: &SnowflakeScrapperConf{
				SnowflakeConf:  dwhexecsnowflake.SnowflakeConf{Account: "test-account"},
				AccountUsageDb: ptr("CUSTOM_SNOWFLAKE"),
			},
			fqn:   fqn,
			limit: 50,
		},
		{
			name:  "table_name_with_single_quote",
			conf:  &SnowflakeScrapperConf{},
			fqn:   scrapper.DwhFqn{DatabaseName: "DB'WITH'QUOTES", SchemaName: "SCH'EMA", ObjectName: "TABLE'NAME"},
			limit: 10,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			scrpr := &SnowflakeScrapper{conf: tc.conf}
			sql := scrpr.buildDMLHistorySQL(tc.fqn, from, to, tc.limit)
			s.NotEmpty(sql)
			snaps.MatchSnapshot(s.T(), sql)
		})
	}
}

func (s *TableChangeHistorySuite) TestBuildAccessHistorySQL() {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)
	fqn := scrapper.DwhFqn{DatabaseName: "MY_DB", SchemaName: "PUBLIC", ObjectName: "ORDERS"}

	testCases := []struct {
		name  string
		conf  *SnowflakeScrapperConf
		fqn   scrapper.DwhFqn
		limit int
	}{
		{
			name:  "default_account_usage_db",
			conf:  &SnowflakeScrapperConf{},
			fqn:   fqn,
			limit: 100,
		},
		{
			name: "custom_account_usage_db",
			conf: &SnowflakeScrapperConf{
				AccountUsageDb: ptr("CUSTOM_SNOWFLAKE"),
			},
			fqn:   fqn,
			limit: 50,
		},
		{
			name:  "object_name_with_single_quote",
			conf:  &SnowflakeScrapperConf{},
			fqn:   scrapper.DwhFqn{DatabaseName: "DB'TEST", SchemaName: "SCH'EMA", ObjectName: "TABLE'NAME"},
			limit: 10,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			scrpr := &SnowflakeScrapper{conf: tc.conf}
			sql := scrpr.buildAccessHistorySQL(tc.fqn, from, to, tc.limit)
			s.NotEmpty(sql)
			snaps.MatchSnapshot(s.T(), sql)
		})
	}
}

func (s *TableChangeHistorySuite) TestDMLHistorySQLSafeEscaping() {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)
	scrpr := &SnowflakeScrapper{conf: &SnowflakeScrapperConf{}}

	// Table names with single quotes should be properly escaped (doubled)
	fqn := scrapper.DwhFqn{DatabaseName: "DB'INJECT", SchemaName: "SCHEMA", ObjectName: "TABLE"}
	sql := scrpr.buildDMLHistorySQL(fqn, from, to, 100)
	s.Contains(sql, "DB''INJECT", "single quote should be doubled for safe escaping")
	s.NotContains(sql, "'DB'INJECT'", "unescaped single quote should not appear")
}

func (s *TableChangeHistorySuite) TestAccessHistorySQLSafeEscaping() {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)
	scrpr := &SnowflakeScrapper{conf: &SnowflakeScrapperConf{}}

	// Table names with single quotes should be properly escaped (doubled)
	fqn := scrapper.DwhFqn{DatabaseName: "DB'INJECT", SchemaName: "SCHEMA", ObjectName: "TABLE"}
	sql := scrpr.buildAccessHistorySQL(fqn, from, to, 100)
	s.Contains(sql, "DB''INJECT", "single quote should be doubled for safe escaping")
}
