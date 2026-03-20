package bigquery

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/stretchr/testify/suite"
)

type BigQueryOAuthSuite struct {
	suite.Suite
}

func TestBigQueryOAuthSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
	}
	if os.Getenv("BIGQUERY_OAUTH_TOKEN") == "" {
		t.Skip("BIGQUERY_OAUTH_TOKEN not set")
	}

	suite.Run(t, new(BigQueryOAuthSuite))
}

func (s *BigQueryOAuthSuite) TestOAuthConnection() {
	ctx := context.TODO()
	conf := &BigQueryConf{
		ProjectId:   "nifty-motif-341212",
		AccessToken: os.Getenv("BIGQUERY_OAUTH_TOKEN"),
	}

	execer, err := NewBigqueryExecutor(ctx, conf)
	s.Require().NoError(err)
	s.Require().NotNil(execer)
	defer execer.Close()

	type result struct {
		TestCol int64  `bigquery:"test_col"`
		TextCol string `bigquery:"text_col"`
	}
	q := NewQuerier[result](execer)
	results, err := q.QueryMany(ctx, "SELECT 1 as test_col, 'hello' as text_col")
	s.Require().NoError(err)
	s.Require().Len(results, 1)
	s.Equal(int64(1), results[0].TestCol)
	s.Equal("hello", results[0].TextCol)
}

func (s *BigQueryOAuthSuite) TestOAuthQueryMultipleColumns() {
	ctx := context.TODO()
	conf := &BigQueryConf{
		ProjectId:   "nifty-motif-341212",
		AccessToken: os.Getenv("BIGQUERY_OAUTH_TOKEN"),
	}

	execer, err := NewBigqueryExecutor(ctx, conf)
	s.Require().NoError(err)
	defer execer.Close()

	type multiResult struct {
		A int64  `bigquery:"a"`
		B string `bigquery:"b"`
		C bool   `bigquery:"c"`
	}
	q := NewQuerier[multiResult](execer)
	results, err := q.QueryMany(ctx, "SELECT 42 as a, 'world' as b, true as c",
		exec.WithArgs[multiResult](),
	)
	s.Require().NoError(err)
	s.Require().Len(results, 1)
	s.Equal(int64(42), results[0].A)
	s.Equal("world", results[0].B)
	s.Equal(true, results[0].C)
}
