package snowflake

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/stretchr/testify/suite"
)

type SnowflakeSuite struct {
	suite.Suite
}

func TestSnowflakeSuite(t *testing.T) {
	if len(os.Getenv("CI")) > 0 {
		t.SkipNow()
	}

	suite.Run(t, new(SnowflakeSuite))
}

type sfRes struct {
	TableCatalog string `db:"TABLE_CATALOG"`
	TableSchema  string `db:"TABLE_SCHEMA"`
	TableName    string `db:"TABLE_NAME"`
	TableType    string `db:"TABLE_TYPE"`
}

func (s *SnowflakeSuite) newExecutor() *SnowflakeExecutor {
	ctx := context.TODO()
	execer, err := NewSnowflakeExecutor(ctx, &SnowflakeConf{
		User:      os.Getenv("SNOWFLAKE_USER"),
		Password:  os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:   os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse: os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases: []string{os.Getenv("SNOWFLAKE_DATABASE")},
		Role:      os.Getenv("SNOWFLAKE_ROLE"),
	})
	s.Require().NoError(err)
	s.Require().NotNil(execer)
	return execer
}

func (s *SnowflakeSuite) TestQueryWithStats() {
	execer := s.newExecutor()
	defer execer.Close()

	var receivedStats *querystats.QueryStats
	ctx := context.TODO()
	ctx = querystats.WithCallback(ctx, func(stats querystats.QueryStats) {
		receivedStats = &stats
		jsonBytes, _ := json.Marshal(stats)
		logging.GetLogger(ctx).Printf("Query stats: %s", string(jsonBytes))
	})

	q := NewQuerier[sfRes](execer)
	results, err := q.QueryMany(
		ctx,
		"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?",
		exec.WithArgs[sfRes]("INFORMATION_SCHEMA"),
	)

	s.Require().NoError(err)
	s.Require().NotEmpty(results)

	s.Require().NotNil(receivedStats, "query stats callback should have been called")
	s.NotEmpty(receivedStats.QueryID, "Snowflake should always provide a query ID")
	// Without WithQueryStatsFetch, BytesRead is not available (requires extra API call)
	s.Nil(receivedStats.BytesRead, "BytesRead should not be set without WithQueryStatsFetch")
}

func (s *SnowflakeSuite) TestQueryWithDetailedStats() {
	execer := s.newExecutor()
	defer execer.Close()

	var receivedStats *querystats.QueryStats
	ctx := querystats.WithQueryStatsFetch(context.TODO())
	ctx = querystats.WithCallback(ctx, func(stats querystats.QueryStats) {
		receivedStats = &stats
		jsonBytes, _ := json.Marshal(stats)
		logging.GetLogger(ctx).Printf("Query stats: %s", string(jsonBytes))
	})

	q := NewQuerier[sfRes](execer)
	results, err := q.QueryMany(
		ctx,
		"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?",
		exec.WithArgs[sfRes]("INFORMATION_SCHEMA"),
	)

	s.Require().NoError(err)
	s.Require().NotEmpty(results)

	s.Require().NotNil(receivedStats, "query stats callback should have been called")
	s.NotEmpty(receivedStats.QueryID, "Snowflake should always provide a query ID")
	// With WithQueryStatsFetch, detailed stats are fetched via GetQueryStatus API
	s.NotNil(receivedStats.BytesRead, "BytesRead should be set with WithQueryStatsFetch")
}
