package snowflake

import (
	"context"
	"regexp"
	"testing"
	"time"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type QueryLogsSuite struct {
	suite.Suite
}

func TestQueryLogsSuite(t *testing.T) {
	suite.Run(t, new(QueryLogsSuite))
}

func (s *QueryLogsSuite) TestBuildQueryLogsSql() {
	ctx := context.Background()
	from := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
	to := time.Date(2025, 11, 4, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name string
		conf *SnowflakeScrapperConf
	}{
		{
			name: "no_database_filtering",
			conf: &SnowflakeScrapperConf{
				SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
					Account:   "test-account",
					Databases: []string{},
				},
			},
		},
		{
			name: "single_database",
			conf: &SnowflakeScrapperConf{
				SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
					Account:   "test-account",
					Databases: []string{"NYC_TAXI"},
				},
			},
		},
		{
			name: "multiple_databases",
			conf: &SnowflakeScrapperConf{
				SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
					Account:   "test-account",
					Databases: []string{"NYC_TAXI", "PRODUCTION_DB", "DEV_DB"},
				},
			},
		},
		{
			name: "query_logs_databases_overrides_databases",
			conf: &SnowflakeScrapperConf{
				SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
					Account:   "test-account",
					Databases: []string{"NYC_TAXI", "PRODUCTION_DB"},
				},
				QueryLogsDatabases: []string{"ANALYTICS"},
			},
		},
		{
			name: "custom_account_usage_db",
			conf: &SnowflakeScrapperConf{
				SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
					Account:   "test-account",
					Databases: []string{"ANALYTICS"},
				},
				AccountUsageDb: ptr("CUSTOM_SNOWFLAKE"),
			},
		},
		{
			name: "database_with_special_chars",
			conf: &SnowflakeScrapperConf{
				SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
					Account:   "test-account",
					Databases: []string{"DATA_WAREHOUSE", "DB-WITH-DASH"},
				},
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			scrapper := &SnowflakeScrapper{
				conf: tc.conf,
			}

			// Get columns to select - use a subset for testing
			columnsToSelect := []string{
				"QUERY_ID",
				"QUERY_TEXT",
				"DATABASE_NAME",
				"SCHEMA_NAME",
				"QUERY_TYPE",
				"START_TIME",
				"END_TIME",
				"EXECUTION_STATUS",
			}

			sql, err := scrapper.buildQueryLogsSql(ctx, from, to, columnsToSelect)
			require.NoError(s.T(), err)
			require.NotEmpty(s.T(), sql)

			// Snapshot the generated SQL
			snaps.MatchSnapshot(s.T(), sql)

			// All queries should have time-based filtering
			s.Contains(sql, "END_TIME")
			s.Contains(sql, "to_timestamp_ltz")
			s.Contains(sql, "between")

			// Check database filtering based on config
			if len(tc.conf.Databases) > 0 || len(tc.conf.QueryLogsDatabases) > 0 {
				// Should have database filtering
				s.Contains(sql, "REGEXP_INSTR")
				s.Contains(sql, "DATABASE_NAME")
			}

			// Verify account usage database
			if tc.conf.AccountUsageDb != nil {
				s.Contains(sql, *tc.conf.AccountUsageDb+".ACCOUNT_USAGE.QUERY_HISTORY")
			} else {
				s.Contains(sql, "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY")
			}
		})
	}
}

func (s *QueryLogsSuite) TestBuildQueryLogsSqlRegexPattern() {
	// This test specifically validates the regex pattern for quoted identifiers
	ctx := context.Background()
	from := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
	to := time.Date(2025, 11, 4, 12, 0, 0, 0, time.UTC)

	conf := &SnowflakeScrapperConf{
		SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
			Account:   "test-account",
			Databases: []string{"NYC_TAXI"},
		},
	}

	scrapper := &SnowflakeScrapper{conf: conf}
	columnsToSelect := []string{"QUERY_ID", "QUERY_TEXT"}

	sql, err := scrapper.buildQueryLogsSql(ctx, from, to, columnsToSelect)
	require.NoError(s.T(), err)

	// Verify the regex pattern is correctly formatted
	// Should match both "NYC_TAXI"."PUBLIC" and NYC_TAXI.PUBLIC
	s.Contains(sql, `REGEXP_INSTR(QUERY_TEXT,`)

	// Verify proper escaping of special characters for whitespace
	s.Contains(sql, `\\t\\n\\r`)

	// Verify the pattern has case-insensitive flag
	s.Contains(sql, `, 'i') > 0`)

	// Verify OR condition with DATABASE_NAME
	s.Contains(sql, `DATABASE_NAME in ('NYC_TAXI')`)
	s.Contains(sql, ` or REGEXP_INSTR`)
}

func (s *QueryLogsSuite) TestBuildQueryLogsSqlMultipleDatabases() {
	// Test regex pattern with multiple databases
	ctx := context.Background()
	from := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
	to := time.Date(2025, 11, 4, 12, 0, 0, 0, time.UTC)

	conf := &SnowflakeScrapperConf{
		SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
			Account:   "test-account",
			Databases: []string{"DB1", "DB2", "DB3"},
		},
	}

	scrapper := &SnowflakeScrapper{conf: conf}
	columnsToSelect := []string{"QUERY_ID", "QUERY_TEXT"}

	sql, err := scrapper.buildQueryLogsSql(ctx, from, to, columnsToSelect)
	require.NoError(s.T(), err)

	// Verify all databases are in the IN clause
	s.Contains(sql, `DATABASE_NAME in ('DB1', 'DB2', 'DB3')`)

	// Verify regex pattern has all databases with pipe separator
	s.Contains(sql, `DB1|DB2|DB3`)
}

func (s *QueryLogsSuite) TestBuildQueryLogsSqlAdmitsAppFileFetches() {
	// A Streamlit app or a notebook fetches its own files under the login Snowflake runs it as, and
	// that fetch is the only statement tying the login to the app. The app often lives in a database
	// the filter leaves out, while its queries read the listed ones.
	ctx := context.Background()
	from := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
	to := time.Date(2025, 11, 4, 12, 0, 0, 0, time.UTC)

	scrapper := &SnowflakeScrapper{conf: &SnowflakeScrapperConf{
		SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Account: "test-account", Databases: []string{"ANALYTICS"}},
	}}
	sql, err := scrapper.buildQueryLogsSql(ctx, from, to, []string{"QUERY_ID", "QUERY_TEXT"})
	require.NoError(s.T(), err)

	s.Contains(sql, `REGEXP_LIKE(QUERY_TEXT, '[ \\t\\n\\r]*(GET|LS|LIST)[ \\t\\n\\r]+''snow://(streamlit|notebook)/.*', 'is')`)
	s.Contains(sql, `DATABASE_NAME in ('ANALYTICS') or REGEXP_INSTR`)
}

func (s *QueryLogsSuite) TestAppFileFetchPattern() {
	pattern := regexp.MustCompile(`(?is)^` + appFileFetchPattern + `$`)
	for _, text := range []string{
		`GET 'snow://streamlit/"APPS_DB"."PUBLIC"."MY_APP"/versions/version$1/snowflake.yml' 'file:///opt/streamlit-runtime'`,
		`ls 'snow://streamlit/APPS_DB.PUBLIC.MY_APP/versions/live/'`,
		"\n  GET 'snow://notebook/\"APPS_DB\".\"PUBLIC\".\"MY_NOTEBOOK\"/versions/live/notebook_app.ipynb' 'file:///tmp'",
	} {
		s.True(pattern.MatchString(text), text)
	}
	for _, text := range []string{
		`GET 'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/head/query.sql' 'file:///tmp'`,
		`GET @my_stage/file.csv file:///tmp`,
		`SELECT 'GET ''snow://streamlit/APPS_DB.PUBLIC.MY_APP/'''`,
	} {
		s.False(pattern.MatchString(text), text)
	}
}

func (s *QueryLogsSuite) TestQueryLogsDatabasesPriority() {
	// Test that QueryLogsDatabases takes priority over Databases
	ctx := context.Background()
	from := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
	to := time.Date(2025, 11, 4, 12, 0, 0, 0, time.UTC)

	conf := &SnowflakeScrapperConf{
		SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
			Account:   "test-account",
			Databases: []string{"SHOULD_NOT_BE_USED"},
		},
		QueryLogsDatabases: []string{"ACTUAL_DB"},
	}

	scrapper := &SnowflakeScrapper{conf: conf}
	columnsToSelect := []string{"QUERY_ID", "QUERY_TEXT"}

	sql, err := scrapper.buildQueryLogsSql(ctx, from, to, columnsToSelect)
	require.NoError(s.T(), err)

	// Should use QueryLogsDatabases, not Databases
	s.Contains(sql, `DATABASE_NAME in ('ACTUAL_DB')`)
	s.NotContains(sql, "SHOULD_NOT_BE_USED")

	// Verify regex pattern uses the correct database
	s.Contains(sql, `ACTUAL_DB`)
}

func ptr(s string) *string {
	return &s
}
