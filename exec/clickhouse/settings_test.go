package clickhouse

import (
	"context"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

type SettingsSuite struct {
	suite.Suite
}

func TestSettingsSuite(t *testing.T) {
	suite.Run(t, new(SettingsSuite))
}

func (s *SettingsSuite) TestCoerceSettingValue() {
	testCases := []struct {
		value    string
		expected any
	}{
		{value: "true", expected: 1},
		{value: "TRUE", expected: 1},
		{value: "false", expected: 0},
		{value: "False", expected: 0},
		{value: "0", expected: 0},
		{value: "1", expected: 1},
		{value: "300", expected: 300},
		{value: "-5", expected: -5},
		{value: "1.5", expected: "1.5"},
		{value: "", expected: ""},
		{value: "best_effort", expected: "best_effort"},
		// A DSN lowercases free-text values; we keep them as written.
		{value: "Scrape_Catalog", expected: "Scrape_Catalog"},
	}

	for _, tc := range testCases {
		s.Run(tc.value, func() {
			s.Equal(tc.expected, coerceSettingValue(tc.value))
		})
	}
}

func (s *SettingsSuite) TestBuildSettingsKeepsDefaultsWhenNoneSupplied() {
	s.Equal(defaultSettings(), buildSettings(nil))
	s.Equal(defaultSettings(), buildSettings(map[string]string{}))
}

func (s *SettingsSuite) TestBuildSettingsOverridesDefaults() {
	settings := buildSettings(map[string]string{"max_execution_time": "3600"})

	s.Equal(3600, settings["max_execution_time"])
	s.Equal(defaultSettings()["max_query_size"], settings["max_query_size"])
}

func (s *SettingsSuite) TestBuildSettingsAddsUnknownNames() {
	settings := buildSettings(map[string]string{
		"use_query_cache":                    "true",
		"receive_timeout":                    "120",
		"date_time_input_format":             "best_effort",
		"max_bytes_before_external_group_by": "1000000",
	})

	s.Equal(clickhouse.Settings{
		"max_execution_time":                 defaultSettings()["max_execution_time"],
		"max_query_size":                     defaultSettings()["max_query_size"],
		"use_query_cache":                    1,
		"receive_timeout":                    120,
		"date_time_input_format":             "best_effort",
		"max_bytes_before_external_group_by": 1000000,
	}, settings)
}

func (s *SettingsSuite) TestBuildSettingsDoesNotMutateDefaults() {
	buildSettings(map[string]string{"max_execution_time": "3600"})

	s.Equal(60, defaultSettings()["max_execution_time"])
}

// LocalClickHouseSettingsSuite checks that conf settings actually reach the
// server, against a local ClickHouse (dev-infra/dwhtesting/lib/clickhouse).
type LocalClickHouseSettingsSuite struct {
	suite.Suite
	ctx  context.Context
	conf ClickhouseConf
}

func TestLocalClickHouseSettingsSuite(t *testing.T) {
	suite.Run(t, new(LocalClickHouseSettingsSuite))
}

func (s *LocalClickHouseSettingsSuite) SetupSuite() {
	if os.Getenv("CI") != "" {
		s.T().Skip("Skipping local ClickHouse tests in CI")
	}

	s.ctx = context.TODO()
	s.conf = ClickhouseConf{
		Hostname:        testenv.EnvOrDefault("CLICKHOUSE_HOST", "127.0.0.1"),
		Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
		Username:        os.Getenv("CLICKHOUSE_USER"),
		Password:        testenv.EnvOrDefault("CLICKHOUSE_PASSWORD", "getsynq10"),
		DefaultDatabase: testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "synq_test"),
		NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
	}

	executor, err := NewClickhouseExecutor(s.ctx, &s.conf)
	if err != nil {
		s.T().Skipf("Skipping: could not connect to local ClickHouse: %v", err)
	}
	s.Require().NoError(executor.Close())
}

// settingValue reads back what the server resolved a setting to for this connection.
func (s *LocalClickHouseSettingsSuite) settingValue(conf ClickhouseConf, name string) string {
	executor, err := NewClickhouseExecutor(s.ctx, &conf)
	s.Require().NoError(err)
	defer executor.Close()

	var value string
	s.Require().NoError(
		executor.QueryRow(s.ctx, "SELECT value FROM system.settings WHERE name = ?", name).Scan(&value),
	)
	return value
}

func (s *LocalClickHouseSettingsSuite) TestDefaultsApply() {
	s.Equal("60", s.settingValue(s.conf, "max_execution_time"))
}

func (s *LocalClickHouseSettingsSuite) TestConfSettingOverridesDefault() {
	conf := s.conf
	conf.Settings = map[string]string{"max_execution_time": "137"}

	s.Equal("137", s.settingValue(conf, "max_execution_time"))
}

func (s *LocalClickHouseSettingsSuite) TestConfSettingAppliesToUndefaultedName() {
	conf := s.conf
	conf.Settings = map[string]string{"max_result_rows": "4242"}

	s.Equal("4242", s.settingValue(conf, "max_result_rows"))
	s.Equal("60", s.settingValue(conf, "max_execution_time"), "unrelated defaults survive")
}

func (s *LocalClickHouseSettingsSuite) TestConfSettingCoercesBoolean() {
	conf := s.conf
	conf.Settings = map[string]string{"join_use_nulls": "true"}

	s.Equal("1", s.settingValue(conf, "join_use_nulls"))
}
