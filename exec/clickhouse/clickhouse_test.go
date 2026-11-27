package clickhouse

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/google/uuid"
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

func (s *ClickhouseSuite) newExecutor(ctx context.Context) *ClickhouseExecutor {
	execer, err := NewClickhouseExecutor(ctx, &ClickhouseConf{
		Hostname: testenv.EnvOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:     testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
		Username: testenv.EnvOrDefault("CLICKHOUSE_USER", "default"),
		Password: testenv.EnvOrDefault("CLICKHOUSE_PASSWORD", "default"),
		NoSsl:    testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = execer.Close() })
	return execer
}

// A DDL answers with no data block at all. The driver read that as a broken
// connection, so database/sql ran the statement again on new connections and
// still failed with "bad connection".
func (s *ClickhouseSuite) TestQueryMapsRunsADdlOnceAndReturnsNoRows() {
	ctx := context.TODO()
	execer := s.newExecutor(ctx)
	table := "default.qua3277_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	s.Require().NoError(execer.Exec(ctx, "CREATE TABLE "+table+" (id UInt64) ENGINE = MergeTree ORDER BY id"))
	defer func() { _ = execer.Exec(ctx, "DROP TABLE IF EXISTS "+table) }()

	marker := uuid.NewString()
	rows, err := NewQuerier[any](execer).QueryMaps(ctx, "ALTER TABLE "+table+" MODIFY COMMENT '"+marker+"'")
	s.Require().NoError(err)
	s.Empty(rows)

	s.Require().NoError(execer.Exec(ctx, "SYSTEM FLUSH LOGS"))
	var starts uint64
	s.Require().NoError(execer.GetDb().GetContext(ctx, &starts,
		"SELECT count() FROM system.query_log WHERE type = 'QueryStart' AND query_kind = 'Alter' AND query LIKE ?", "%"+marker+"%"))
	if starts == 0 {
		s.T().Skip("the server does not log QueryStart rows (log_queries_min_type above QUERY_START)")
	}
	s.Equal(uint64(1), starts)

	// The connection the DDL ran on goes back to the pool and still answers.
	selected, err := NewQuerier[any](execer).QueryMaps(ctx, "SELECT comment FROM system.tables WHERE database || '.' || name = '"+table+"'")
	s.Require().NoError(err)
	s.Require().Len(selected, 1)
	comment, ok := selected[0].Get("comment")
	s.Require().True(ok)
	s.Equal(marker, comment)
}

// A distributed DDL in the default throw mode answers with status rows, one per
// host, which must still come back. The count of hosts is the cluster's business,
// so only that rows arrive is checked. Needs a server with Keeper and a cluster,
// named by CLICKHOUSE_CLUSTER.
func (s *ClickhouseSuite) TestQueryMapsReturnsTheHostRowsOfAnOnClusterDdl() {
	cluster := os.Getenv("CLICKHOUSE_CLUSTER")
	if cluster == "" {
		s.T().Skip("CLICKHOUSE_CLUSTER is not set")
	}
	ctx := context.TODO()
	execer := s.newExecutor(ctx)
	table := "default.qua3277_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	s.Require().NoError(execer.Exec(ctx, "CREATE TABLE "+table+" ON CLUSTER "+cluster+" (id UInt64) ENGINE = MergeTree ORDER BY id"))
	defer func() { _ = execer.Exec(ctx, "DROP TABLE IF EXISTS "+table+" ON CLUSTER "+cluster) }()

	rows, err := NewQuerier[any](execer).QueryMaps(ctx, "ALTER TABLE "+table+" ON CLUSTER "+cluster+" MODIFY COMMENT 'x'")
	s.Require().NoError(err)
	s.Require().NotEmpty(rows)
	_, ok := rows[0].Get("host")
	s.True(ok)
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
