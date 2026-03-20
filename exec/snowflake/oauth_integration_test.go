package snowflake

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/stretchr/testify/suite"
)

type SnowflakeOAuthSuite struct {
	suite.Suite
}

func TestSnowflakeOAuthSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
	}
	if os.Getenv("SNOWFLAKE_OAUTH_TOKEN") == "" {
		t.Skip("SNOWFLAKE_OAUTH_TOKEN not set")
	}

	suite.Run(t, new(SnowflakeOAuthSuite))
}

func (s *SnowflakeOAuthSuite) TestOAuthConnection() {
	ctx := context.TODO()
	conf := &SnowflakeConf{
		Token:     os.Getenv("SNOWFLAKE_OAUTH_TOKEN"),
		Account:   "cuunsqr-ir70409",
		Warehouse: "SYNQ_WH",
		Databases: []string{"LUKAS"},
		Role:      "PUBLIC",
	}

	execer, err := NewSnowflakeExecutor(ctx, conf)
	s.Require().NoError(err)
	s.Require().NotNil(execer)
	defer execer.Close()

	type result struct {
		TestCol int    `db:"TEST_COL"`
		TextCol string `db:"TEXT_COL"`
	}
	q := NewQuerier[result](execer)
	results, err := q.QueryMany(ctx, "SELECT 1 as TEST_COL, 'hello' as TEXT_COL")
	s.Require().NoError(err)
	s.Require().Len(results, 1)
	s.Equal(1, results[0].TestCol)
	s.Equal("hello", results[0].TextCol)
}

func (s *SnowflakeOAuthSuite) TestOAuthQueryMany() {
	ctx := context.TODO()
	conf := &SnowflakeConf{
		Token:     os.Getenv("SNOWFLAKE_OAUTH_TOKEN"),
		Account:   "cuunsqr-ir70409",
		Warehouse: "SYNQ_WH",
		Databases: []string{"LUKAS"},
		Role:      "PUBLIC",
	}

	execer, err := NewSnowflakeExecutor(ctx, conf)
	s.Require().NoError(err)
	defer execer.Close()

	type sfRes struct {
		TableCatalog string `db:"TABLE_CATALOG"`
		TableSchema  string `db:"TABLE_SCHEMA"`
		TableName    string `db:"TABLE_NAME"`
	}
	q := NewQuerier[sfRes](execer)
	results, err := q.QueryMany(
		ctx,
		"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES LIMIT 5",
		exec.WithArgs[sfRes](),
	)
	s.Require().NoError(err)
	s.Require().NotEmpty(results)
}
