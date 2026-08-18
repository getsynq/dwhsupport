package clickhouse

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

type ClickhouseSuite struct {
	suite.Suite
}

func TestClickhouseSuite(t *testing.T) {
	if len(os.Getenv("CI")) > 0 {
		t.SkipNow()
	}

	suite.Run(t, new(ClickhouseSuite))
}

type res struct {
	TableCatalog string `db:"table_catalog"`
	TableSchema  string `db:"table_schema"`
	TableName    string `db:"table_name"`
	TableType    string `db:"table_type"`
}

// DefaultDatabase reaches the connection, so an unqualified table name in a query
// resolves against it.
func (s *ClickhouseSuite) TestDefaultDatabaseOpensTheConnection() {
	ctx := context.TODO()
	execer, err := NewClickhouseExecutor(ctx, &ClickhouseConf{
		Hostname:        testenv.EnvOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
		Username:        testenv.EnvOrDefault("CLICKHOUSE_USER", "default"),
		Password:        testenv.EnvOrDefault("CLICKHOUSE_PASSWORD", "default"),
		DefaultDatabase: "system",
		NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
	})
	s.Require().NoError(err)
	s.Require().NotNil(execer)
	defer execer.Close()

	var currentDatabase string
	s.Require().NoError(execer.GetDb().GetContext(ctx, &currentDatabase, "SELECT currentDatabase()"))
	s.Equal("system", currentDatabase)
}

// A default database that cannot be opened is a misconfigured convenience, not a
// reason for the whole connection to fail: the scrape reads system tables and
// needs none of it.
func (s *ClickhouseSuite) TestUnopenableDefaultDatabaseStillConnects() {
	ctx := context.TODO()
	execer, err := NewClickhouseExecutor(ctx, &ClickhouseConf{
		Hostname:        testenv.EnvOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
		Username:        testenv.EnvOrDefault("CLICKHOUSE_USER", "default"),
		Password:        testenv.EnvOrDefault("CLICKHOUSE_PASSWORD", "default"),
		DefaultDatabase: "no_such_database_here",
		NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
	})
	s.Require().NoError(err)
	s.Require().NotNil(execer)
	defer execer.Close()

	// The connection fell back to the server default, and system tables — where
	// every scrape query reads from — are reachable.
	var currentDatabase string
	s.Require().NoError(execer.GetDb().GetContext(ctx, &currentDatabase, "SELECT currentDatabase()"))
	s.Equal("default", currentDatabase)
}

func (s *ClickhouseSuite) TestSomething() {
	ctx := context.TODO()
	execer, err := NewClickhouseExecutor(ctx, &ClickhouseConf{
		Hostname:        testenv.EnvOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
		Username:        testenv.EnvOrDefault("CLICKHOUSE_USER", "default"),
		Password:        testenv.EnvOrDefault("CLICKHOUSE_PASSWORD", "default"),
		DefaultDatabase: testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "default"),
		NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
	})
	s.NoError(err)
	s.NotNil(execer)
	defer execer.Close()

	ctx = querystats.WithCallback(ctx, func(stats querystats.QueryStats) {
		jsonBytes, _ := json.Marshal(stats)
		logging.GetLogger(ctx).Printf("Query stats: %s", string(jsonBytes))
	})

	q := NewQuerier[res](execer)
	res, err := q.QueryMany(
		ctx,
		"SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_catalog = ?",
		exec.WithArgs[res]("system"),
	)
	s.Require().NoError(err)
	s.Require().NotEmpty(res)
}
