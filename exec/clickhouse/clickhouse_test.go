package clickhouse

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/logging"
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

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func (s *ClickhouseSuite) TestSomething() {
	ctx := context.TODO()
	execer, err := NewClickhouseExecutor(ctx, &ClickhouseConf{
		Hostname:        envOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:            envOrDefaultInt("CLICKHOUSE_PORT", 9000),
		Username:        envOrDefault("CLICKHOUSE_USER", "default"),
		Password:        envOrDefault("CLICKHOUSE_PASSWORD", "default"),
		DefaultDatabase: envOrDefault("CLICKHOUSE_DATABASE", "default"),
		NoSsl:           true,
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
