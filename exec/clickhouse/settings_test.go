package clickhouse

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

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
